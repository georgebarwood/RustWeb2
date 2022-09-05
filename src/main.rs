type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    println!( "args={:?}", args );

    let listen = format!("{}:{}", args.ip, args.port);
    // let listen = listen.parse().expect("Error parsing listen address:port");
    let is_master = args.rep.is_empty();
    let replicate_source = args.rep;
    let replicate_credentials = args.login;

    // Construct an AtomicFile. This ensures that updates to the database are "all or nothing".
    let file = Box::new(SimpleFileStorage::new("rustweb.rustdb"));
    let upd = Box::new(SimpleFileStorage::new("rustweb.upd"));
    let stg = Box::new(AtomicFile::new(file, upd));

    // SharedPagedData allows for one writer and multiple readers.
    // Note that readers never have to wait, they get a "virtual" read-only copy of the database.
    let spd = Arc::new(SharedPagedData::new(stg));
    {
        let mut s = spd.stash.lock().unwrap();
        s.mem_limit = args.mem * 1000000;
        s.trace = args.tracemem;
    }

    let bmap = Arc::new(builtins::get_bmap());

    // Construct task communication channels.
    let (tx, mut rx) = mpsc::channel::<share::ServerMessage>(1);
    let (email_tx, email_rx) = mpsc::unbounded_channel::<()>();
    let (sleep_tx, sleep_rx) = mpsc::unbounded_channel::<u64>();
    let (sync_tx, sync_rx) = oneshot::channel::<bool>();
    let (wait_tx, _wait_rx) = broadcast::channel::<()>(16);

    // Construct shared state.
    let ss = Arc::new(share::SharedState {
        spd: spd.clone(),
        bmap: bmap.clone(),
        tx,
        email_tx,
        sleep_tx,
        wait_tx,
        is_master,
        replicate_source,
        replicate_credentials,
        tracetime: args.tracetime,
        dos_limit: args.dos * 1000_000_000,
        dos: Arc::new(Mutex::new(HashMap::default())),
    });

    if is_master {
        // Start the email task.
        let ssc = ss.clone();
        tokio::spawn(async move { tasks::email_loop(email_rx, ssc).await });

        // Start the sleep task.
        let ssc = ss.clone();
        tokio::spawn(async move { tasks::sleep_loop(sleep_rx, ssc).await });
    } else {
        // Start the sync task.
        let ssc = ss.clone();
        tokio::spawn(async move { tasks::sync_loop(sync_rx, ssc).await });
    }

    // Start the ip_decay task.
    let ssc = ss.clone();
    tokio::spawn(async move { tasks::ip_decay_loop(ssc).await });

    // Start the task that updates the database.
    let ssc = ss.clone();
    thread::spawn(move || {
        let ss = ssc;

        // Get write-access to database ( there will only be one of these ).
        let wapd = AccessPagedData::new_writer(spd);

        let db = Database::new(wapd, if is_master { init::INITSQL } else { "" }, bmap);
        if !is_master {
            let _ = sync_tx.send(db.is_new);
        }
        loop {
            let mut sm = rx.blocking_recv().unwrap();
            let sql = sm.st.x.qy.sql.clone();
            if ss.tracetime {
                db.run_timed(&sql, &mut sm.st.x);
            } else {
                db.run(&sql, &mut sm.st.x);
            }

            if sm.st.log && db.changed() {
                if let Some(t) = db.get_table(&ObjRef::new("log", "Transaction")) {
                    // Append serialised transaction to log.Transaction table
                    let ser = rmp_serde::to_vec(&sm.st.x.qy).unwrap();
                    let ser = Value::RcBinary(Rc::new(ser));
                    let mut row = t.row();
                    row.id = t.alloc_id() as i64;
                    row.values[0] = ser;
                    t.insert(&db, &mut row);
                }
            }
            let updates = db.save();
            if updates > 0 {
                let _ = ss.wait_tx.send(());
                if ss.tracetime {
                    println!("Pages updated={updates}");
                }
            }
            let _x = sm.reply.send(sm.st);
        }
    });

    let listener = tokio::net::TcpListener::bind(listen).await?;
    loop {
        let (stream, src) = listener.accept().await?;
        let ssc = ss.clone();
        tokio::spawn(async move {
            // println!("Start process_requests");
            let _ = request::process(stream, src.ip().to_string(), ssc).await;
            // println!("End process_requests");
        });
    }
}

/// Extra SQL builtin functions.
pub mod builtins;
/// SQL initialisation string.
pub mod init;
/// Async request processing.
pub mod request;
/// Shared data structures.
pub mod share;
/// Tasks for email, sync etc.
pub mod tasks;

use mimalloc::MiMalloc;
use rustc_hash::FxHashMap as HashMap;
use rustdb::{
    AccessPagedData, AtomicFile, Database, ObjRef, SharedPagedData, SimpleFileStorage, Value,
};
use std::{
    rc::Rc,
    sync::{Arc, Mutex},
    thread,
};
use tokio::sync::{broadcast, mpsc, oneshot};

/// Memory allocator ( MiMalloc ).
#[global_allocator]
static MEMALLOC: MiMalloc = MiMalloc;

use clap::Parser;

/// Command line arguments.
#[derive(Parser,Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on
    #[clap(value_parser = clap::value_parser!(u16).range(1..))]
    port: u16,

    /// Ip Address to listen on
    #[clap(long, value_parser, default_value = "0.0.0.0")]
    ip: String,

    /// Server to replicate
    #[clap(long, value_parser, default_value = "")]
    rep: String,

    /// Login cookies for replication
    #[clap(long, value_parser, default_value = "")]
    login: String,

    /// Memory limit for page cache (in MB)
    #[clap(long, value_parser, default_value_t = 100)]
    mem: usize,

    /// Denial of Service Limit
    #[clap(long, value_parser, default_value_t = 100)]
    dos: u64,

    /// Trace query time
    #[clap(long, value_parser, default_value_t = false)]
    tracetime: bool,

    /// Trace memory trimming
    #[clap(long, value_parser, default_value_t = false)]
    tracemem: bool,
}
