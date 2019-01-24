//! Logic for incorporating changes to a Soup graph into an already running graph.
//!
//! Performing a migration involves a number of steps:
//!
//!  - New nodes that are children of nodes in a different domain must be preceeded by an ingress
//!  - Egress nodes must be added to nodes that now have children in a different domain
//!  - Timestamp ingress nodes for existing domains must be connected to new base nodes
//!  - Timestamp ingress nodes must be added to all new domains
//!  - New nodes for existing domains must be sent to those domains
//!  - New domains must be booted up
//!  - Input channels must be set up for new base nodes
//!  - The graph must be analyzed for new materializations. These materializations must be
//!    *initialized* before data starts to flow to the new nodes. This may require two domains to
//!    communicate directly, and may delay migration completion.
//!  - Index requirements must be resolved, and checked for conflicts.
//!
//! Furthermore, these must be performed in the correct *order* so as to prevent dead- or
//! livelocks. This module defines methods for performing each step in relative isolation, as well
//! as a function for performing them in the right order.
//!
//! Beware, Here be dragons™

use crate::controller::{ControllerInner, WorkerIdentifier};
use dataflow::prelude::*;
use dataflow::{node, payload};
use dataflow::node::ReplicaType;
use dataflow::ops::identity::Identity;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use petgraph;
use slog;

pub mod assignment;
pub mod augmentation;
pub mod materialization;
pub mod routing;
pub mod sharding;

#[derive(Clone)]
pub(super) enum ColumnChange {
    Add(String, DataType),
    Drop(usize),
}

/// A `Migration` encapsulates a number of changes to the Soup data flow graph.
///
/// Only one `Migration` can be in effect at any point in time. No changes are made to the running
/// graph until the `Migration` is committed (using `Migration::commit`).
pub struct Migration<'a> {
    pub(super) mainline: &'a mut ControllerInner,
    pub(super) added: Vec<NodeIndex>,
    pub(super) columns: Vec<(NodeIndex, ColumnChange)>,
    pub(super) readers: HashMap<NodeIndex, Vec<NodeIndex>>,

    pub(super) start: Instant,
    pub(super) log: slog::Logger,

    /// Additional migration information provided by the client
    pub(super) context: HashMap<String, DataType>,
    /// Worker iterator for domain-worker assignment
    pub(super) wis: Option<std::vec::IntoIter<WorkerIdentifier>>,
    /// Mapping from nodes to their bottom replicas
    pub(super) replicas: HashMap<NodeIndex, NodeIndex>,
}

impl<'a> Migration<'a> {
    // Correct the parents by replacing top replicas with their corresponding bottom replicas.
    // Ensures that new ingredients or readers are children of the bottom replica. Also, none
    // of the inputs should be bottom replicas since users should be unaware of their existence.
    // TODO(ygina): probably doesn't work if the parent was added in a separate migration.
    fn correct_parents(&self, nodes: &mut Vec<NodeIndex>) -> Vec<NodeIndex> {
        let bottoms = self.replicas.values().collect::<HashSet<&NodeIndex>>();
        let mut corrected = Vec::new();
        for i in 0..nodes.len() {
            let t = nodes.get(i).unwrap();
            assert!(!bottoms.contains(&t));
            if let Some(bottom) = self.replicas.get(&t) {
                nodes.push(*bottom);
                corrected.push(nodes.swap_remove(i));
            }
        }
        corrected
    }

    /// Add the given `Ingredient` to the Soup.
    ///
    /// The returned identifier can later be used to refer to the added ingredient.
    /// Edges in the data flow graph are automatically added based on the ingredient's reported
    /// `ancestors`.
    pub fn add_ingredient<S1, FS, S2, I>(&mut self, name: S1, fields: FS, mut i: I) -> NodeIndex
    where
        S1: ToString,
        S2: ToString,
        FS: IntoIterator<Item = S2> + Clone,
        I: Ingredient + Into<NodeOperator>,
    {
        i.on_connected(&self.mainline.ingredients);
        let (parents, corrected) = {
            let mut parents = i.ancestors();
            let corrected = self.correct_parents(&mut parents);
            (parents, corrected)
        };
        assert!(!parents.is_empty());

        // add to the graph
        let ni =
            self.mainline
                .ingredients
                .add_node(node::Node::new(name.to_string(), fields.clone(), i.into()));
        info!(self.log,
              "adding new node";
              "node" => ni.index(),
              "type" => format!("{:?}", self.mainline.ingredients[ni])
        );

        // keep track of the fact that it's new
        self.added.push(ni);
        // insert it into the graph
        for parent in &parents {
            self.mainline.ingredients.add_edge(*parent, ni, ());
        }

        // if any parents were corrected, update bottom_next_nodes for the corrected parent
        if corrected.len() > 0 {
            assert_eq!(corrected.len(), 1);
            self.mainline.ingredients[corrected[0]]
                .set_replica_type(ReplicaType::Top { bottom_next_nodes: vec![ni] });
        }

        // if the node is an aggregator, then it is a top replica.
        // we also need to create a bottom replica.
        if self.mainline.ingredients[ni].is_aggregator() {
            let bottom_ni = self.add_ingredient(name, fields, Identity::new(ni));
            assert!(self.replicas.insert(ni, bottom_ni).is_none());
            assert_eq!(parents.len(), 1, "only handle top replicas with one parent, for now");

            self.mainline.ingredients[ni]
                .set_replica_type(ReplicaType::Top { bottom_next_nodes: Vec::new() });
            self.mainline.ingredients[bottom_ni]
                .set_replica_type(ReplicaType::Bottom { top_prev_nodes: parents });
        }

        // and tell the caller its id
        ni.into()
    }

    /// Add the given `Base` to the Soup.
    ///
    /// The returned identifier can later be used to refer to the added ingredient.
    pub fn add_base<S1, FS, S2>(
        &mut self,
        name: S1,
        fields: FS,
        b: node::special::Base,
    ) -> NodeIndex
    where
        S1: ToString,
        S2: ToString,
        FS: IntoIterator<Item = S2>,
    {
        // add to the graph
        let ni = self
            .mainline
            .ingredients
            .add_node(node::Node::new(name.to_string(), fields, b));
        info!(self.log,
              "adding new base";
              "node" => ni.index(),
        );

        // keep track of the fact that it's new
        self.added.push(ni);
        // insert it into the graph
        self.mainline
            .ingredients
            .add_edge(self.mainline.source, ni, ());
        // and tell the caller its id
        ni.into()
    }

    /// Returns the context of this migration
    pub fn context(&self) -> &HashMap<String, DataType> {
        &self.context
    }

    /// Returns the universe in which this migration is operating in.
    /// If not specified, assumes `global` universe.
    pub fn universe(&self) -> (DataType, Option<DataType>) {
        let id = match self.context.get("id") {
            Some(id) => id.clone(),
            None => "global".into(),
        };

        let group = match self.context.get("group") {
            Some(g) => Some(g.clone()),
            None => None,
        };

        (id, group)
    }

    /// Add a new column to a base node.
    ///
    /// Note that a default value must be provided such that old writes can be converted into this
    /// new type.
    pub fn add_column<S: ToString>(
        &mut self,
        node: NodeIndex,
        field: S,
        default: DataType,
    ) -> usize {
        // not allowed to add columns to new nodes
        assert!(!self.added.iter().any(|&ni| ni == node));

        let field = field.to_string();
        let base = &mut self.mainline.ingredients[node];
        assert!(base.is_base());

        // we need to tell the base about its new column and its default, so that old writes that
        // do not have it get the additional value added to them.
        let col_i1 = base.add_column(&field);
        // we can't rely on DerefMut, since it disallows mutating Taken nodes
        {
            let col_i2 = base.get_base_mut().unwrap().add_column(default.clone());
            assert_eq!(col_i1, col_i2);
        }

        // also eventually propagate to domain clone
        self.columns.push((node, ColumnChange::Add(field, default)));

        col_i1
    }

    /// Drop a column from a base node.
    pub fn drop_column(&mut self, node: NodeIndex, column: usize) {
        // not allowed to drop columns from new nodes
        assert!(!self.added.iter().any(|&ni| ni == node));

        let base = &mut self.mainline.ingredients[node];
        assert!(base.is_base());

        // we need to tell the base about the dropped column, so that old writes that contain that
        // column will have it filled in with default values (this is done in Mutator).
        // we can't rely on DerefMut, since it disallows mutating Taken nodes
        base.get_base_mut().unwrap().drop_column(column);

        // also eventually propagate to domain clone
        self.columns.push((node, ColumnChange::Drop(column)));
    }

    #[cfg(test)]
    pub fn graph(&self) -> &Graph {
        self.mainline.graph()
    }

    /// Ensures the given node has `replication_factor` number of readers. Stores only the newly
    /// added readers to the migration state, then returns all readers for the given node.
    pub fn ensure_reader_for(
        &mut self,
        n: NodeIndex,
        name: Option<String>,
        replication_factor: usize
    ) -> Vec<NodeIndex> {
        if self.readers.contains_key(&n) {
            return self.readers.get(&n).unwrap().clone();
        }

        let mut readers = self.mainline.ingredients[n].get_readers().to_vec();
        let mut new_readers = Vec::new();
        while readers.len() < replication_factor {
            // Make a reader node
            let i = self.mainline.ingredients[n].next_reader_to_add();
            let r = node::special::Reader::new(n, i);
            let r = if let Some(name) = &name {
                self.mainline.ingredients[n].named_mirror(r, format!("{}_{}", name, i))
            } else {
                self.mainline.ingredients[n].mirror(r)
            };

            // Add it to the graph along with an edge to the node it reads for
            let r = self.mainline.ingredients.add_node(r);
            self.mainline.ingredients.add_edge(n, r, ());
            self.mainline.ingredients[n].add_reader(r);
            debug!(self.log,
                  "adding reader node";
                  "node" => r.index(),
                  "for_node" => n.index(),
                  "reader_index" => i
            );
            readers.push(r);
            new_readers.push(r);
        }

        // TODO(ygina): implement removing readers when num readers > replication_factor
        info!(
            self.log,
            "added {} readers for node {}, total {} readers",
            new_readers.len(),
            n.index(),
            readers.len(),
        );
        self.readers.insert(n, new_readers);
        readers
    }

    /// Set up the given node such that its output can be efficiently queried.
    ///
    /// To query into the maintained state, use `ControllerInner::get_getter`.
    #[cfg(test)]
    pub fn maintain_anonymous(&mut self, n: NodeIndex, key: &[usize]) -> Vec<NodeIndex> {
        let (n, corrected) = {
            let mut nodes = vec![n];
            let corrected = self.correct_parents(&mut nodes);
            (nodes[0], corrected)
        };
        let ris = self.ensure_reader_for(n, None, self.mainline.replication_factor);

        for ri in &ris {
            self.mainline.ingredients[*ri]
                .with_reader_mut(|r| r.set_key(key))
                .unwrap();
        }

        // if any parents were corrected, update bottom_next_nodes for the corrected parent
        if corrected.len() > 0 {
            assert_eq!(corrected.len(), 1);
            self.mainline.ingredients[corrected[0]]
                .set_replica_type(ReplicaType::Top { bottom_next_nodes: ris.clone() });
        }

        ris
    }

    /// Set up the given node such that its output can be efficiently queried.
    ///
    /// To query into the maintained state, use `ControllerInner::get_getter`.
    pub fn maintain(&mut self, name: String, n: NodeIndex, key: &[usize]) {
        let (n, corrected) = {
            let mut nodes = vec![n];
            let corrected = self.correct_parents(&mut nodes);
            (nodes[0], corrected)
        };
        let ris = self.ensure_reader_for(n, Some(name), self.mainline.replication_factor);

        for ri in &ris {
            self.mainline.ingredients[*ri]
                .with_reader_mut(|r| r.set_key(key))
                .unwrap();
        }

        // if any parents were corrected, update bottom_next_nodes for the corrected parent
        if corrected.len() > 0 {
            assert_eq!(corrected.len(), 1);
            self.mainline.ingredients[corrected[0]]
                .set_replica_type(ReplicaType::Top { bottom_next_nodes: ris });
        }
    }

    fn assign_local_addresses(
            mainline: &mut ControllerInner,
            log: &slog::Logger,
            sorted_new: &Vec<NodeIndex>,
            swapped: &HashMap<(NodeIndex, NodeIndex), NodeIndex>) {
        let domain_new_nodes = sorted_new
            .iter()
            .map(|&ni| (mainline.ingredients[ni].domain(), ni))
            .fold(HashMap::new(), |mut dns, (d, ni)| {
                dns.entry(d).or_insert_with(Vec::new).push(ni);
                dns
            });

        for (domain, nodes) in domain_new_nodes.iter() {
            // Number of pre-existing nodes
            let mut nnodes = mainline.remap.get(domain).map(HashMap::len).unwrap_or(0);

            if nodes.is_empty() {
                // Nothing to do here
                continue;
            }

            let log = log.new(o!("domain" => domain.index()));

            // Give local addresses to every (new) node
            for &ni in nodes.iter() {
                debug!(log,
                       "assigning local index";
                       "type" => format!("{:?}", mainline.ingredients[ni]),
                       "node" => ni.index(),
                       "local" => nnodes
                );

                let mut ip: IndexPair = ni.into();
                ip.set_local(unsafe { LocalNodeIndex::make(nnodes as u32) });
                mainline.ingredients[ni].set_finalized_addr(ip);
                mainline
                    .remap
                    .entry(*domain)
                    .or_insert_with(HashMap::new)
                    .insert(ni, ip);
                nnodes += 1;
            }

            // Initialize each new node
            for &ni in nodes.iter() {
                if mainline.ingredients[ni].is_internal() {
                    // Figure out all the remappings that have happened
                    // NOTE: this has to be *per node*, since a shared parent may be remapped
                    // differently to different children (due to sharding for example). we just
                    // allocate it once though.
                    let mut remap = mainline.remap[domain].clone();

                    // Parents in other domains have been swapped for ingress nodes.
                    // Those ingress nodes' indices are now local.
                    for (&(dst, src), &instead) in swapped.iter() {
                        if dst != ni {
                            // ignore mappings for other nodes
                            continue;
                        }

                        let old = remap.insert(src, mainline.remap[domain][&instead]);
                        assert_eq!(old, None);
                    }

                    trace!(log, "initializing new node"; "node" => ni.index());
                    mainline
                        .ingredients
                        .node_weight_mut(ni)
                        .unwrap()
                        .on_commit(&remap);
                }
            }
        }
    }

    fn workers_with_readers_for(
        mainline: &ControllerInner,
        node: NodeIndex,
    ) -> HashSet<WorkerIdentifier> {
        mainline.ingredients[node]
            .get_readers()
            .iter()
            .filter(|&ni| !mainline.ingredients[*ni].is_dropped())
            .filter_map(|&ni| mainline.domains.get(&mainline.ingredients[ni].domain()))
            .flat_map(|dh| &dh.shards)
            .map(|dsh| dsh.worker)
            .collect()
    }

    /// Places the domains and their nodes on workers according to the round robin iterator. This
    /// is used to ensure, for example, that readers for the same view and shards of the same
    /// node end up on different workers. It is also currently used to approximately distribute
    /// domains across the remaining workers equally. In the future, we'd like to place the domain
    /// on the machine with the fewest domains already assigned. This does not take into account
    /// readers and shards that were NOT created for their very first time.
    ///
    /// Domains are placed round robin in the order that they are provided.
    fn place_round_robin(
        mainline: &mut ControllerInner,
        log: &slog::Logger,
        uninformed_domain_nodes: &mut HashMap<DomainIndex, Vec<(NodeIndex, bool)>>,
        changed_domains: &Vec<DomainIndex>,
        wis: Option<std::vec::IntoIter<WorkerIdentifier>>,
    ) -> Option<std::vec::IntoIter<WorkerIdentifier>> {
        // TODO(malte): simple round-robin placement for the moment
        let mut wis = {
            if let Some(wis) = wis {
                wis
            } else {
                let mut wis_sorted = mainline.workers
                    .keys()
                    .map(|wi| wi.clone())
                    .collect::<Vec<WorkerIdentifier>>();
                // Sort the worker identifiers so domain-worker assignment is deterministic
                wis_sorted.sort_by(|wia, wib| wia.to_string().cmp(&wib.to_string()));
                wis_sorted.into_iter()
            }
        };

        for domain in changed_domains {
            if mainline.domains.contains_key(&domain) {
                // this is not a new domain
                continue;
            }

            // find a worker identifier for each shard
            let nodes = uninformed_domain_nodes.remove(&domain).unwrap();
            let num_shards = mainline.ingredients[nodes[0].0].sharded_by().shards();
            let mut identifiers = Vec::new();

            // TODO(ygina): check this works for sharded
            let mut curr_workers = HashSet::new();
            for (ni, _) in &nodes {
                if mainline.ingredients[*ni].is_reader() {
                    curr_workers = Self::workers_with_readers_for(
                        mainline,
                        mainline
                            .ingredients[*ni]
                            .with_reader(|r| r.is_for())
                            .unwrap()
                    );
                    break;
                }
            }

            for _ in 0..num_shards.unwrap_or(1) {
                let mut num_tries = 0;
                let wi = loop {
                    if let Some(wi) = wis.next() {
                        if curr_workers.contains(&wi) && num_tries < curr_workers.len() {
                            num_tries += 1;
                            continue;
                        }
                        let w = mainline.workers.get(&wi).unwrap();
                        if w.healthy {
                            break wi;
                        }
                    } else {
                        let mut wis_sorted = mainline.workers
                            .keys()
                            .map(|wi| wi.clone())
                            .collect::<Vec<WorkerIdentifier>>();
                        wis_sorted.sort_by(|wia, wib| wia.to_string().cmp(&wib.to_string()));
                        wis = wis_sorted.into_iter();
                    }
                };
                identifiers.push(wi);
            }

            // place the domain on the worker
            let d = mainline.place_domain(
                *domain,
                identifiers,
                num_shards,
                &log,
                nodes,
            );
            mainline.domains.insert(*domain, d);
        }
        Some(wis)
    }

    /// Commit the changes introduced by this `Migration` to the master `Soup`.
    ///
    /// This will spin up an execution thread for each new thread domain, and hook those new
    /// domains into the larger Soup graph. The returned map contains entry points through which
    /// new updates should be sent to introduce them into the Soup.
    pub fn commit(mut self) {
        info!(self.log, "finalizing migration"; "#nodes" => self.added.len());

        let log = self.log;
        let start = self.start;
        let mut mainline = self.mainline;
        let mut new: HashSet<_> = self.added.into_iter().collect();

        // Readers are nodes too.
        for (_parent, readers) in &self.readers {
            for reader in readers {
                new.insert(*reader);
            }
        }

        // Shard the graph as desired
        let mut swapped0 = if let Some(shards) = mainline.sharding {
            sharding::shard(
                &log,
                &mut mainline.ingredients,
                mainline.source,
                &mut new,
                shards,
            )
        } else {
            HashMap::default()
        };

        // Assign domains
        assignment::assign(
            &log,
            &mut mainline.ingredients,
            mainline.source,
            &new,
            &mut mainline.ndomains,
        );

        // Set up ingress and egress nodes
        let swapped1 = routing::add(&log, &mut mainline.ingredients, mainline.source, &mut new);

        // Merge the swap lists
        for ((dst, src), instead) in swapped1 {
            use std::collections::hash_map::Entry;
            match swapped0.entry((dst, src)) {
                Entry::Occupied(mut instead0) => {
                    if &instead != instead0.get() {
                        // This can happen if sharding decides to add a Sharder *under* a node,
                        // and routing decides to add an ingress/egress pair between that node
                        // and the Sharder. It's perfectly okay, but we should prefer the
                        // "bottommost" swap to take place (i.e., the node that is *now*
                        // closest to the dst node). This *should* be the sharding node, unless
                        // routing added an ingress *under* the Sharder. We resolve the
                        // collision by looking at which translation currently has an adge from
                        // `src`, and then picking the *other*, since that must then be node
                        // below.
                        if mainline.ingredients.find_edge(src, instead).is_some() {
                            // src -> instead -> instead0 -> [children]
                            // from [children]'s perspective, we should use instead0 for from, so
                            // we can just ignore the `instead` swap.
                        } else {
                            // src -> instead0 -> instead -> [children]
                            // from [children]'s perspective, we should use instead for src, so we
                            // need to prefer the `instead` swap.
                            *instead0.get_mut() = instead;
                        }
                    }
                }
                Entry::Vacant(hole) => {
                    hole.insert(instead);
                }
            }

            // we may also already have swapped the parents of some node *to* `src`. in
            // swapped0. we want to change that mapping as well, since lookups in swapped
            // aren't recursive.
            for (_, instead0) in swapped0.iter_mut() {
                if *instead0 == src {
                    *instead0 = instead;
                }
            }
        }
        let swapped = swapped0;

        // Assign local addresses to all new nodes, and initialize them.
        let mut sorted_new: Vec<NodeIndex> = new
            .iter()
            .map(|&ni| ni)
            .filter(|&ni| ni != mainline.source)
            .filter(|&ni| !mainline.ingredients[ni].is_dropped())
            .collect::<Vec<_>>();
        sorted_new.sort();

        Self::assign_local_addresses(&mut mainline, &log, &sorted_new, &swapped);
        if let Some(shards) = mainline.sharding {
            sharding::validate(&log, &mainline.ingredients, mainline.source, &new, shards)
        };

        // at this point, we've hooked up the graph such that, for any given domain, the graph
        // looks like this:
        //
        //      o (egress)
        //     +.\......................
        //     :  o (ingress)
        //     :  |
        //     :  o-------------+
        //     :  |             |
        //     :  o             o
        //     :  |             |
        //     :  o (egress)    o (egress)
        //     +..|...........+.|..........
        //     :  o (ingress) : o (ingress)
        //     :  |\          :  \
        //     :  | \         :   o
        //
        // etc.
        // println!("{}", mainline);

        let mut uninformed_domain_nodes = mainline
            .ingredients
            .node_indices()
            .filter(|&ni| ni != mainline.source)
            .filter(|&ni| !mainline.ingredients[ni].is_dropped())
            .map(|ni| (mainline.ingredients[ni].domain(), ni, new.contains(&ni)))
            .fold(HashMap::new(), |mut dns, (d, ni, new)| {
                dns.entry(d).or_insert_with(Vec::new).push((ni, new));
                dns
            });

        // Since we're using a round robin iterator, obtain a vec of DomainIndexes in the order
        // that we want to assign them to workers. For readers, we have to specifically list
        // readers for the same node in consecutive order to ensure their respective domains end
        // up on different workers. For non-readers, the order doesn't actually matter.
        let mut changed_domains_readers = Vec::new();
        for (_, readers) in &self.readers {
            changed_domains_readers.extend(
                readers
                .iter()
                .map(|&ni| mainline.ingredients[ni].domain())
            );
        }
        let changed_domains_other = sorted_new
            .iter()
            .map(|&ni| mainline.ingredients[ni].domain())
            .collect::<HashSet<_>>()
            .into_iter()
            .filter(|&domain| !changed_domains_readers.contains(&domain))
            .collect::<Vec<_>>();
        debug!(
            log,
            "found changed domains";
            "reader domains" => format!("{:?}", changed_domains_readers),
            "other domains" => format!("{:?}", changed_domains_other),
        );

        // Check invariants on the changed reader domains. Each changed reader domain should be
        // just created. The domain should contain the reader node and its ingress node,
        // and no other nodes. Also, each new reader node should be in a unique domain.
        assert_eq!(
            changed_domains_readers.len(),
            changed_domains_readers.iter().collect::<HashSet<_>>().len()
        );
        for domain in &changed_domains_readers {
            assert!(!mainline.domains.contains_key(&domain));
            let nodes: &Vec<_> = uninformed_domain_nodes.get(&domain).unwrap();
            assert_eq!(nodes.len(), 2);
            let (ni_a, _) = nodes.get(0).unwrap();
            let (ni_b, _) = nodes.get(1).unwrap();
            assert!(
                (mainline.ingredients[*ni_a].is_reader()
                    && mainline.ingredients[*ni_b].is_ingress())
                || (mainline.ingredients[*ni_a].is_ingress()
                    && mainline.ingredients[*ni_b].is_reader())
            );
        }

        // Boot up new domains (they'll ignore all updates for now).
        // We call `place_round_robin` twice to distinguish the changed domains for which the order
        // matters (readers) from the changed domains for which it doesn't, as stated above.
        debug!(log, "booting new domains");
        self.wis = Self::place_round_robin(
            mainline,
            &log,
            &mut uninformed_domain_nodes,
            &changed_domains_readers,
            self.wis,
        );
        self.wis = Self::place_round_robin(
            mainline,
            &log,
            &mut uninformed_domain_nodes,
            &changed_domains_other,
            self.wis,
        );

        // Add any new nodes to existing domains (they'll also ignore all updates for now)
        debug!(log, "mutating existing domains");
        augmentation::inform(&log, &mut mainline, uninformed_domain_nodes);

        // Tell all base nodes and base ingress children about newly added columns
        for (ni, change) in self.columns {
            let mut inform = if let ColumnChange::Add(..) = change {
                // we need to inform all of the base's children too,
                // so that they know to add columns to existing records when replaying
                mainline
                    .ingredients
                    .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                    .filter(|&eni| mainline.ingredients[eni].is_egress())
                    .flat_map(|eni| {
                        // find ingresses under this egress
                        mainline
                            .ingredients
                            .neighbors_directed(eni, petgraph::EdgeDirection::Outgoing)
                    })
                    .collect()
            } else {
                // ingress nodes don't need to know about deleted columns, because those are only
                // relevant when new writes enter the graph.
                Vec::new()
            };
            inform.push(ni);

            for ni in inform {
                let n = &mainline.ingredients[ni];
                let m = match change.clone() {
                    ColumnChange::Add(field, default) => box payload::Packet::AddBaseColumn {
                        node: n.local_addr(),
                        field: field,
                        default: default,
                    },
                    ColumnChange::Drop(column) => box payload::Packet::DropBaseColumn {
                        node: n.local_addr(),
                        column: column,
                    },
                };

                let domain = mainline.domains.get_mut(&n.domain()).unwrap();

                domain.send_to_healthy(m, &mainline.workers).unwrap();
                mainline.replies.wait_for_acks(&domain);
            }
        }

        // Set up inter-domain connections
        // NOTE: once we do this, we are making existing domains block on new domains!
        info!(log, "bringing up inter-domain connections");
        routing::connect(
            &log,
            &mut mainline.ingredients,
            &mut mainline.domains,
            &mainline.workers,
            &new,
        );

        // And now, the last piece of the puzzle -- set up materializations
        info!(log, "initializing new materializations");
        mainline.materializations.commit(
            &mainline.ingredients,
            &new,
            &mut mainline.domains,
            &mainline.workers,
            &mut mainline.replies,
        );

        warn!(log, "migration completed"; "ms" => start.elapsed().as_millis());
    }
}
