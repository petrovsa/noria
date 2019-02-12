const WRITE_BATCH_SIZE: usize = 1000;

#[macro_use]
extern crate clap;
extern crate futures;
extern crate noria;
extern crate rand;
extern crate zipf;

#[path = "../vote/clients/localsoup/graph.rs"]
mod graph;

use noria::{DataType, TableOperation};
use rand::{distributions::Distribution, Rng};
use std::io::prelude::*;
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::{fs, thread, time};

const NANOS_PER_SEC: u64 = 1_000_000_000;

use zipf::ZipfDistribution;

struct Reporter {
    last: time::Instant,
    every: time::Duration,
    count: usize,
}

impl Reporter {
    pub fn report(&mut self, n: usize) -> Option<(time::Duration, usize)> {
        self.count += n;

        let now = time::Instant::now();
        let elapsed = now - self.last;
        if elapsed > self.every {
            let count = Some((elapsed, self.count));
            self.last = now;
            self.count = 0;
            count
        } else {
            None
        }
    }

    pub fn new(every: time::Duration) -> Self {
        Reporter {
            last: time::Instant::now(),
            every,
            count: 0,
        }
    }
}

fn one(s: &graph::Builder, skewed: bool, args: &clap::ArgMatches, w: Option<fs::File>) {
    let narticles = value_t_or_exit!(args, "narticles", usize);
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let migrate_after = time::Duration::from_secs(value_t_or_exit!(args, "migrate", u64));
    assert!(migrate_after < runtime);

    // reporting config
    let every = time::Duration::from_millis(200);

    // default persistence (memory only)
    let mut persistence_params = noria::PersistenceParameters::default();
    persistence_params.mode = noria::DurabilityMode::MemoryOnly;

    // make the graph!
    eprintln!("Setting up soup");
    let mut g = s.start_sync(persistence_params).unwrap();
    eprintln!("Getting accessors");
    let mut articles = g.graph.table("Article").unwrap().into_sync();
    let mut votes = g.graph.table("Vote").unwrap().into_sync();
    let mut read_old = g.graph.view("ArticleWithVoteCount").unwrap().into_sync();

    // prepopulate
    eprintln!("Prepopulating with {} articles", narticles);
    articles
        .perform_all(
            (0..narticles).map(|i| vec![(i as i32).into(), format!("Article #{}", i + 1).into()]),
        )
        .unwrap();

    let (stat, stat_rx) = mpsc::channel();
    let barrier = Arc::new(Barrier::new(3));

    // start writer that just does a bunch of old writes
    eprintln!("Starting old writer");
    let w1 = {
        let stat = stat.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let zipf = ZipfDistribution::new(narticles, 1.08).unwrap();
            barrier.wait();

            let mut reporter = Reporter::new(every);
            let start = time::Instant::now();
            while start.elapsed() < runtime {
                votes
                    .perform_all((0..WRITE_BATCH_SIZE).map(|i| {
                        // always generate both so that we aren't artifically faster with one
                        let id_uniform = rng.gen_range(0, narticles);
                        let id_zipf = zipf.sample(&mut rng);
                        let id = if skewed { id_zipf } else { id_uniform };
                        TableOperation::from(vec![DataType::from(id), i.into()])
                    }))
                    .unwrap();

                if let Some((took, count)) = reporter.report(WRITE_BATCH_SIZE) {
                    let count_per_ns = count as f64 / took.as_nanos() as f64;
                    let count_per_s = count_per_ns * NANOS_PER_SEC as f64;
                    stat.send(("OLD", count_per_s)).unwrap();
                }
            }
        })
    };

    // start a read that just reads old forever
    eprintln!("Starting old reader");
    let r1 = {
        let barrier = barrier.clone();
        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let zipf = ZipfDistribution::new(narticles, 1.08).unwrap();
            barrier.wait();
            let start = time::Instant::now();
            while start.elapsed() < runtime {
                let id_uniform = rng.gen_range(0, narticles);
                let id_zipf = zipf.sample(&mut rng);
                let id = if skewed { id_zipf } else { id_uniform };
                read_old.lookup(&[DataType::from(id)], false).unwrap();
                thread::sleep(time::Duration::new(0, 10_000));
            }
        })
    };

    // wait for other threads to be ready
    barrier.wait();
    let start = time::Instant::now();

    let stats = thread::spawn(move || {
        let mut w = w;
        for (stat, val) in stat_rx {
            let line = format!("{} {} {:.2}", start.elapsed().as_nanos(), stat, val);
            println!("{}", line);
            if let Some(ref mut w) = w {
                writeln!(w, "{}", line).unwrap();
            }
        }
    });

    // we now need to wait for migrate_after
    eprintln!("Waiting for migration time...");
    thread::sleep(migrate_after);

    // all right, migration time
    eprintln!("Starting migration");
    stat.send(("MIG START", 0.0)).unwrap();
    g.transition();
    stat.send(("MIG FINISHED", 0.0)).unwrap();
    let mut ratings = g.graph.table("Rating").unwrap().into_sync();
    let mut read_new = g.graph.view("ArticleWithScore").unwrap().into_sync();

    // start writer that just does a bunch of new writes
    eprintln!("Starting new writer");
    let w2 = {
        let stat = stat.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let zipf = ZipfDistribution::new(narticles, 1.08).unwrap();
            barrier.wait();

            let mut reporter = Reporter::new(every);
            while start.elapsed() < runtime {
                ratings
                    .perform_all((0..WRITE_BATCH_SIZE).map(|i| {
                        let id_uniform = rng.gen_range(0, narticles);
                        let id_zipf = zipf.sample(&mut rng);
                        let id = if skewed { id_zipf } else { id_uniform };
                        TableOperation::from(vec![DataType::from(id), i.into(), 5.into()])
                    }))
                    .unwrap();

                if let Some((took, count)) = reporter.report(WRITE_BATCH_SIZE) {
                    let count_per_ns = count as f64 / took.as_nanos() as f64;
                    let count_per_s = count_per_ns * NANOS_PER_SEC as f64;
                    stat.send(("NEW", count_per_s)).unwrap();
                }
            }
        })
    };

    // start reader that keeps probing new read view
    eprintln!("Starting new read probe");
    let r2 = {
        let stat = stat.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            let n = 100;
            let mut hits = 0;
            let mut rng = rand::thread_rng();
            let zipf = ZipfDistribution::new(narticles, 1.08).unwrap();
            let mut reporter = Reporter::new(every);
            barrier.wait();
            while start.elapsed() < runtime {
                let ids = (0..n)
                    .map(|_| {
                        let id_uniform = rng.gen_range(0, narticles);
                        let id_zipf = zipf.sample(&mut rng);
                        vec![DataType::from(if skewed { id_zipf } else { id_uniform })]
                    })
                    .collect();
                if let Ok(rss) = read_new.multi_lookup(ids, false) {
                    hits += rss.into_iter().filter(|rs| !rs.is_empty()).count();
                } else {
                    // miss, or view not yet ready
                }

                if let Some((_, count)) = reporter.report(n) {
                    stat.send(("HITF", hits as f64 / count as f64)).unwrap();
                    hits = 0;
                }
                thread::sleep(time::Duration::new(0, 10_000_000));
            }
        })
    };

    // fire them both off!
    barrier.wait();

    // everything finishes!
    eprintln!("Waiting for experiment to end...");
    w1.join().unwrap();
    w2.join().unwrap();
    r1.join().unwrap();
    r2.join().unwrap();

    stat.send(("FIN", 0.0)).unwrap();
    drop(stat);
    stats.join().unwrap();
}

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks user-curated news aggregator throughput for in-memory Soup")
        .arg(
            Arg::with_name("narticles")
                .short("a")
                .long("articles")
                .takes_value(true)
                .default_value("100000")
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .required(true)
                .takes_value(true)
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("migrate")
                .short("m")
                .long("migrate")
                .required(true)
                .takes_value(true)
                .help("Perform a migration after this many seconds")
                .conflicts_with("stage"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .help("Enable verbose logging output"),
        )
        .arg(
            Arg::with_name("relevant")
                .long("just-do-it")
                .help(
                    "Run all interesting benchmarks and store \
                     results to appropriately named files.",
                )
                .conflicts_with("all"),
        )
        .arg(
            Arg::with_name("all")
                .long("do-it-all")
                .help(
                    "Run all benchmarks and store \
                     results to appropriately named files.",
                )
                .conflicts_with("relevant"),
        )
        .arg(
            Arg::with_name("skewed")
                .long("skewed")
                .conflicts_with("all")
                .help("Run with a skewed id distribution"),
        )
        .arg(
            Arg::with_name("full")
                .long("full")
                .conflicts_with("all")
                .help("Disable partial materialization"),
        )
        .arg(
            Arg::with_name("stupid")
                .long("stupid")
                .conflicts_with("all")
                .help("Make the migration stupid"),
        )
        .arg(
            Arg::with_name("shards")
                .long("shards")
                .takes_value(true)
                .help("Use N-way sharding."),
        )
        .get_matches();

    // set config options
    let mut s = graph::Builder::default();
    s.sharding = args
        .value_of("shards")
        .map(|_| value_t_or_exit!(args, "shards", usize));
    s.logging = args.is_present("verbose");

    if args.is_present("all") || args.is_present("relevant") {
        let narticles = value_t_or_exit!(args, "narticles", usize);
        let mills = format!("{}", narticles as f64 / 1_000_000.0);

        // do the ones we need for the paper first
        eprintln!("==> full no reuse (zipf)");
        s.partial = false;
        s.stupid = true;
        one(
            &s,
            true,
            &args,
            Some(
                fs::File::create(format!("vote-no-partial-stupid-{}M.zipf1.08.log", mills))
                    .unwrap(),
            ),
        );
        eprintln!("==> partial with reuse (uniform)");
        s.partial = true;
        s.stupid = false;
        one(
            &s,
            false,
            &args,
            Some(fs::File::create(format!("vote-partial-reuse-{}M.uniform.log", mills)).unwrap()),
        );
        eprintln!("==> partial with reuse (zipf)");
        s.partial = true;
        s.stupid = false;
        one(
            &s,
            true,
            &args,
            Some(fs::File::create(format!("vote-partial-reuse-{}M.zipf1.08.log", mills)).unwrap()),
        );

        if !args.is_present("all") {
            return;
        }

        // then the rest
        eprintln!("==> full no reuse (uniform)");
        s.partial = false;
        s.stupid = true;
        one(
            &s,
            false,
            &args,
            Some(
                fs::File::create(format!("vote-no-partial-stupid-{}M.uniform.log", mills)).unwrap(),
            ),
        );
        eprintln!("==> full with reuse (uniform)");
        s.partial = false;
        s.stupid = false;
        one(
            &s,
            false,
            &args,
            Some(
                fs::File::create(format!("vote-no-partial-reuse-{}M.uniform.log", mills)).unwrap(),
            ),
        );
        eprintln!("==> full with reuse (zipf)");
        s.partial = false;
        s.stupid = false;
        one(
            &s,
            true,
            &args,
            Some(
                fs::File::create(format!("vote-no-partial-reuse-{}M.zipf1.08.log", mills)).unwrap(),
            ),
        );
        eprintln!("==> partial no reuse (uniform)");
        s.partial = true;
        s.stupid = true;
        one(
            &s,
            false,
            &args,
            Some(fs::File::create(format!("vote-partial-stupid-{}M.uniform.log", mills)).unwrap()),
        );
        eprintln!("==> partial no reuse (zipf)");
        s.partial = true;
        s.stupid = true;
        one(
            &s,
            true,
            &args,
            Some(fs::File::create(format!("vote-partial-stupid-{}M.zipf1.08.log", mills)).unwrap()),
        );
    } else {
        let skewed = args.is_present("skewed");
        s.partial = !args.is_present("full");
        s.stupid = args.is_present("stupid");
        one(&s, skewed, &args, None);
    }
}
