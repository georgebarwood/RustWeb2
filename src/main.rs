use rustc_hash::FxHashMap as HashMap;
use rustdb::{
    AccessPagedData, AtomicFile, BlockPageStg, Database, Limits, MultiFileStorage, ObjRef,
    SharedPagedData, SimpleFileStorage, Value, DB,
};

use std::{
    rc::Rc,
    sync::{Arc, Mutex},
};
use tokio::sync::{broadcast, mpsc, oneshot};

/// Program entry point - construct shared state, start async tasks, process requests.
fn main() {
    main_inner();
    std::thread::sleep(std::time::Duration::from_millis(10));
    println!("Server stopped");
}

fn main_inner() {
    // Read program arguments.
    let args = Args::parse();
    let listen = format!("{}:{}", args.ip, args.port);
    let is_master = args.rep.is_empty();

    let mut limits = Limits::default();
    limits.blk_cap = args.blk_cap;
    limits.page_sizes = args.page_sizes;
    limits.max_div = args.max_div;
    limits.map_lim = args.map_lim;
    limits.rbuf_mem = args.rbuf_mem;
    limits.swbuf = args.swbuf;
    limits.uwbuf = args.uwbuf;

    // Construct an AtomicFile. This ensures that updates to the database are "all or nothing".
    let file = MultiFileStorage::new("rustweb.rustdb");
    let upd = SimpleFileStorage::new("rustweb.upd");
    let stg = AtomicFile::new_with_limits(file, upd, &limits);
    let ps = BlockPageStg::new(stg, &limits);

    // SharedPagedData allows for one writer and multiple readers.
    // Note that readers never have to wait, they get a "virtual" read-only copy of the database.
    let spd = SharedPagedData::new_from_ps(ps);
    let spdc = spd.clone();

    {
        let mut s = spd.stash.lock().unwrap();
        s.mem_limit = args.mem << 20;
    }

    let bmap = Arc::new(builtins::get_bmap());

    // Construct tokio task communication channels.
    let (update_tx, mut update_rx) = mpsc::channel::<share::UpdateMessage>(1);
    let (email_tx, email_rx) = mpsc::unbounded_channel::<()>();
    let (sleep_tx, sleep_rx) = mpsc::unbounded_channel::<u64>();
    let (sync_tx, sync_rx) = oneshot::channel::<bool>();
    let (wait_tx, _wait_rx) = broadcast::channel::<()>(16);

    // Construct shared state.
    let ss = Arc::new(share::SharedState {
        spd: spd.clone(),
        bmap: bmap.clone(),
        update_tx,
        email_tx,
        sleep_tx,
        wait_tx,
        is_master,
        replicate_source: args.rep,
        replicate_credentials: args.login,
        dos_limit: [args.dos_count, args.dos_read, args.dos_cpu, args.dos_write],
        dos: Mutex::new(HashMap::default()),
        tracetime: args.tracetime,
        tracedos: args.tracedos,
        tracemem: args.tracemem,
    });

    // let rt = tokio::runtime::Runtime::new().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    rt.block_on(async {
        if is_master {
            // Start the task that sends emails
            let ssc = ss.clone();
            tokio::spawn(async move { tasks::email_loop(email_rx, ssc).await });

            // Start the task that calls timed.Run
            let ssc = ss.clone();
            tokio::spawn(async move { tasks::sleep_loop(sleep_rx, ssc).await });
        } else {
            // Start the database replication task.
            let ssc = ss.clone();
            tokio::spawn(async move { tasks::sync_loop(sync_rx, ssc).await });
        }

        // Start the task that regularly decreases usage values.
        let ssc = ss.clone();
        tokio::spawn(async move { tasks::u_decay_loop(ssc).await });

        // Start the task that updates the database.
        std::thread::spawn(move || {
            // Get write-access to database ( there will only be one of these ).
            let wapd = AccessPagedData::new_writer(spd);
            let db = Database::new(wapd, "", bmap);
            if db.is_new && is_master {
                let f = std::fs::read_to_string("admin-ScriptAll.txt");
                let init = if let Ok(f) = &f { f } else { init::INITSQL };
                let mut tr = rustdb::GenTransaction::default();
                db.run(init, &mut tr);
                db.save();
            }
            if !is_master {
                let _ = sync_tx.send(db.is_new);
            }
            while let Some(mut sm) = update_rx.blocking_recv() {
                let sql = sm.trans.x.qy.sql.clone();
                db.run(&sql, &mut sm.trans.x);
                if is_master && !sm.trans.no_log() && db.changed() {
                    let ser = bincode::serialize(&sm.trans.x.qy).unwrap();
                    let ser = flate3::deflate(&ser);
                    save_transaction(&db, ser);
                }
                sm.trans.updates = db.save();
                let _x = sm.reply.send(sm.trans);
            }
        });

        // Process http requests.
        let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
        loop {
            tokio::select! {
                a = listener.accept() =>
                {
                    let (stream, src) = a.unwrap();
                    let ssc = ss.clone();
                    tokio::spawn(async move {
                        if let Err(x) = request::process(stream, src.ip().to_string(), ssc).await {
                            println!("End request process error={:?}", x);
                        }
                    });
                }
                _ = tokio::signal::ctrl_c() =>
                {
                    println!("Processing of new http requests stopped by ctrl-C signal - stopping");
                    break;
                }
                _ = term() =>
                {
                    println!("Processing of new http requests stopped by signal - stopping");
                    break;
                }

            }
        }
    });
    // Make sure outstanding writes are flushed to secondary storage.
    spdc.wait_complete();
}

fn save_transaction(db: &DB, bytes: Vec<u8>) {
    if let Some(t) = db.get_table(&ObjRef::new("log", "Transaction")) {
        // Append compressed, serialised transaction to log.Transaction table.
        let ser = Value::RcBinary(Rc::new(bytes));
        let mut row = t.row();
        row.id = t.alloc_id(db);
        row.values[0] = ser;
        t.insert(db, &mut row);
    }
}

#[cfg(unix)]
async fn term() {
    let _ = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .unwrap()
        .recv()
        .await;
}

#[cfg(windows)]
async fn term() {
    let _ = tokio::signal::windows::ctrl_c().unwrap().recv().await;
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

/// Memory allocator ( MiMalloc ).
#[global_allocator]
static MEMALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

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
    #[arg(long, value_parser, default_value_t = 1_000_000)]
    dos_read: u64,

    /// Denial of Service CPU Limit
    #[arg(long, value_parser, default_value_t = 10_000_000)]
    dos_cpu: u64,

    /// Denial of Service Write Response Limit
    #[arg(long, value_parser, default_value_t = 1_000_000)]
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

    /// Trace query time.
    #[arg(long, value_parser, default_value_t = false)]
    tracetime: bool,

    /// Trace memory trimming.
    #[arg(long, value_parser, default_value_t = false)]
    tracemem: bool,

    /// Trace Denial of Service information
    #[arg(long, value_parser, default_value_t = false)]
    tracedos: bool,

    /// Block Capacity
    #[arg(long, value_parser, default_value_t = 27720*5)]
    blk_cap: u64,

    /// Number of different page sizes - max page size must be < 64kb
    #[arg(long, value_parser, default_value_t = 7)]
    page_sizes: usize,

    /// Maximum page size division - min page size must be > 1kb
    #[arg(long, value_parser, default_value_t = 12)]
    max_div: usize,

    /// Limit on size of commit write map.
    #[arg(long, value_parser, default_value_t = 5000)]
    map_lim: usize,

    /// Memory for buffering small reads.
    #[arg(long, value_parser, default_value_t = 0x200000)]
    rbuf_mem: usize,

    /// Memory for buffering writes to main storage.
    #[arg(long, value_parser, default_value_t = 0x100000)]
    swbuf: usize,

    /// Memory for buffering writes to temporary storage
    #[arg(long, value_parser, default_value_t = 0x100000)]
    uwbuf: usize,
}
