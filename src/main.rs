#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let args = Args::parse();
    // println!("args={:?}", args);

    let listen = format!("{}:{}", args.ip, args.port);
    let is_master = args.rep.is_empty();

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

    // Construct tokio task communication channels.
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
        replicate_source: args.rep,
        replicate_credentials: args.login,
        dos_limit: [args.dos_count, args.dos_read, args.dos_cpu, args.dos_write],
        dos: Arc::new(Mutex::new(HashMap::default())),
        tracetime: args.tracetime,
        tracedos: args.tracedos,
    });

    if is_master {
        // Start the task that sends emails
        let ssc = ss.clone();
        tokio::spawn(async move { tasks::email_loop(email_rx, ssc).await });

        // Start the task that calls timed.Run
        let ssc = ss.clone();
        tokio::spawn(async move { tasks::sleep_loop(sleep_rx, ssc).await });
    } else {
        // Start the database reeplication task.
        let ssc = ss.clone();
        tokio::spawn(async move { tasks::sync_loop(sync_rx, ssc).await });
    }

    // Start the task that regularly decreases usage values.
    let ssc = ss.clone();
    tokio::spawn(async move { tasks::u_decay_loop(ssc).await });

    // Start the task that updates the database.
    thread::spawn(move || {
        // Get write-access to database ( there will only be one of these ).
        let wapd = AccessPagedData::new_writer(spd);
        let db = Database::new(wapd, "", bmap);
        if db.is_new && is_master {
            let f = std::fs::read_to_string("admin-ScriptAll.txt");
            let init = if let Ok(f) = &f { f } else { init::INITSQL };
            exec(&db, init);
        }
        if !is_master {
            let _ = sync_tx.send(db.is_new);
        }
        loop {
            let mut sm = rx.blocking_recv().unwrap();
            let sql = sm.st.x.qy.sql.clone();
            db.run(&sql, &mut sm.st.x);
            if sm.st.log && db.changed() {
                if let Some(t) = db.get_table(&ObjRef::new("log", "Transaction")) {
                    // Append serialised transaction to log.Transaction table
                    let ser = bincode::serialize(&sm.st.x.qy).unwrap();
                    let ser = Value::RcBinary(Rc::new(ser));
                    let mut row = t.row();
                    row.id = t.alloc_id() as i64;
                    row.values[0] = ser;
                    t.insert(&db, &mut row);
                }
            }
            sm.st.updates = db.save();
            let _x = sm.reply.send(sm.st);
        }
    });

    // Process http requests.
    let listener = tokio::net::TcpListener::bind(listen).await?;
    loop {
        let (stream, src) = listener.accept().await?;
        let ssc = ss.clone();
        tokio::spawn(async move {
            if let Err(x) = request::process(stream, src.ip().to_string(), ssc).await {
                println!("End request process result={:?}", x);
            }
        });
    }
}

fn exec(db: &rustdb::DB, sql: &str) {
    let mut tr = rustdb::GenTransaction::default();
    db.run(sql, &mut tr);
    db.save();
}

/// Extra SQL builtin functions.
mod builtins;
/// SQL initialisation string.
mod init;
/// http request processing.
mod request;
/// Shared data structures.
mod share;
/// Tasks for email, sync etc.
mod tasks;

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
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(value_parser = clap::value_parser!(u16).range(1..))]
    port: u16,

    /// Ip Address to listen on
    #[arg(long, value_parser, default_value = "0.0.0.0")]
    ip: String,

    /// Denial of Service Count Limit
    #[arg(long, value_parser, default_value_t = 1000)]
    dos_count: u64,

    /// Denial of Service Read Request Limit
    #[arg(long, value_parser, default_value_t = 1_000_000_000_000)]
    dos_read: u64,

    /// Denial of Service CPU Limit
    #[arg(long, value_parser, default_value_t = 100_000)]
    dos_cpu: u64,

    /// Denial of Service Write Response Limit
    #[arg(long, value_parser, default_value_t = 1_000_000_000_000)]
    dos_write: u64,

    /// Memory limit for page cache (in MB)
    #[arg(long, value_parser, default_value_t = 100)]
    mem: usize,

    /// Server to replicate
    #[arg(long, value_parser, default_value = "")]
    rep: String,

    /// Login cookies for replication
    #[arg(long, value_parser, default_value = "")]
    login: String,

    /// Trace query time
    #[arg(long, value_parser, default_value_t = false)]
    tracetime: bool,

    /// Trace memory trimming
    #[arg(long, value_parser, default_value_t = false)]
    tracemem: bool,

    /// Trace memory DoS
    #[arg(long, value_parser, default_value_t = false)]
    tracedos: bool,
}
