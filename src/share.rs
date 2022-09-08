use rustc_hash::FxHashMap as HashMap;
use rustdb::{GenTransaction, Transaction};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};

/// Global shared state.
pub struct SharedState {
    /// Shared storage used for read-only queries.
    pub spd: Arc<rustdb::SharedPagedData>,

    /// Map of builtin SQL functions for Database.
    pub bmap: Arc<rustdb::BuiltinMap>,

    /// Sender channel for sending queries to server task.
    pub tx: mpsc::Sender<ServerMessage>,

    /// For notifying email loop that emails are in Queue ready to be sent.
    pub email_tx: mpsc::UnboundedSender<()>,

    /// For setting sleep time.
    pub sleep_tx: mpsc::UnboundedSender<u64>,

    /// For notifying tasks waiting for update transaction.
    pub wait_tx: broadcast::Sender<()>,

    /// Server is master ( not replicating another database ).
    pub is_master: bool,

    /// URL of master server.
    pub replicate_source: String,

    /// Cookies for replication.
    pub replicate_credentials: String,

    /// Denial of service limits.
    pub dos_limit: UA,

    /// Information for mitigating DoS attacks
    pub dos: Arc<Mutex<HashMap<String, UseInfo>>>,

    /// Trace time to process each request.
    pub tracetime: bool,

    /// Trace Dos
    pub tracedos: bool,
}

/// Usage array ( total or limit ).
pub type UA = [u64; 4];

/// Index into usage array for request read usage.
pub const U_READ: usize = 1;

/// Index into usage array for CPU usage ( time to process a request ).
pub const U_CPU: usize = 2;

/// Index into usage array for response write usage .
pub const U_WRITE: usize = 3;

/// Information kept on usage by each user.
#[derive(Debug)]
pub struct UseInfo {
    used: UA,
    limit: UA,
}

impl UseInfo {
    fn new() -> Self {
        Self {
            used: [0; 4],
            limit: [0; 4],
        }
    }
}

impl SharedState {
    pub fn u_budget(&self, uid: String) -> UA {
        let mut m = self.dos.lock().unwrap();
        let info = m.entry(uid).or_insert_with(UseInfo::new);
        if info.limit[0] == 0 {
            info.limit = self.dos_limit;
        }
        let mut result = [0; 4];
        for (i, item) in result.iter_mut().enumerate() {
            if info.used[i] >= info.limit[i] {
                return [0; 4];
            }
            *item = info.limit[i] - info.used[i];
        }
        result
    }

    pub fn u_inc(&self, uid: &str, amount: UA) {
        let mut m = self.dos.lock().unwrap();
        if let Some(info) = m.get_mut(uid) {
            for (i, amt) in amount.iter().enumerate() {
                info.used[i] += *amt;
            }
            if self.tracedos {
                println!(
                    "uid={} Count={}% Read={}% Cpu={}% Write={}%",
                    uid,
                    100f64 * info.used[0] as f64 / info.limit[0] as f64,
                    100f64 * info.used[1] as f64 / info.limit[1] as f64,
                    100f64 * info.used[2] as f64 / info.limit[2] as f64,
                    100f64 * info.used[3] as f64 / info.limit[3] as f64,
                );
            }
        }
    }

    pub fn u_set_limits(&self, u: String, limit: UA) -> bool {
        let mut m = self.dos.lock().unwrap();
        let info = m.entry(u).or_insert_with(UseInfo::new);
        info.limit = limit;
        for i in 0..4 {
            if info.used[i] >= info.limit[i] {
                return false;
            }
        }
        true
    }

    /// Deflate old usage by 10% periodically.
    pub fn u_decay(&self) {
        let mut m = self.dos.lock().unwrap();
        m.retain(|_uid, info| {
            let mut nz = false;
            for i in 0..4 {
                if info.used[i] > 0 {
                    info.used[i] -= 1 + info.used[i] / 10;
                }
                if info.used[i] > 0 {
                    nz = true;
                }
            }
            nz
        });
    }

    /// Process a server transaction.
    pub async fn process(&self, mut st: ServerTrans) -> ServerTrans {
        let start = std::time::SystemTime::now();
        let mut wait_rx = self.wait_tx.subscribe();
        let mut st = if st.readonly {
            // Readonly request, use read-only copy of database.
            let spd = self.spd.clone();
            let bmap = self.bmap.clone();
            let tracetime = self.tracetime;
            let task = tokio::task::spawn_blocking(move || {
                let apd = rustdb::AccessPagedData::new_reader(spd);
                let db = rustdb::Database::new(apd, "", bmap);
                let sql = st.x.qy.sql.clone();
                if tracetime {
                    db.run_timed(&sql, &mut st.x);
                } else {
                    db.run(&sql, &mut st.x);
                }
                st
            });
            task.await.unwrap()
        } else {
            let (reply, rx) = oneshot::channel::<ServerTrans>();
            let _ = self.tx.send(ServerMessage { st, reply }).await;
            rx.await.unwrap()
        };
        st.run_time = start.elapsed().unwrap();

        let ext = st.x.get_extension();
        if let Some(ext) = ext.downcast_ref::<TransExt>() {
            st.uid = ext.uid.clone();
            if self.is_master {
                if ext.sleep > 0 {
                    let _ = self.sleep_tx.send(ext.sleep);
                }
                if ext.tx_email {
                    let _ = self.email_tx.send(());
                }
            }
            if ext.trans_wait {
                tokio::select! {
                   _ = wait_rx.recv() => {}
                   _ = tokio::time::sleep(Duration::from_secs(600)) => {}
                }
            }
        }
        st.x.set_extension(ext);
        st
    }
}

/// Transaction to be processed.
pub struct ServerTrans {
    pub x: GenTransaction,
    pub log: bool,
    pub readonly: bool,
    pub run_time: core::time::Duration,
    pub uid: String,
}

impl ServerTrans {
    pub fn new() -> Self {
        let mut result = Self {
            x: GenTransaction::new(),
            log: true,
            readonly: false,
            run_time: Duration::from_micros(0),
            uid: String::new(),
        };
        result.x.ext = TransExt::new();
        result
    }

    pub fn new_with_state(ss: Arc<SharedState>, ip: String) -> Self {
        let mut result = Self {
            x: GenTransaction::new(),
            log: true,
            readonly: false,
            run_time: Duration::from_micros(0),
            uid: String::new(),
        };
        let mut ext = TransExt::new();
        ext.ss = Some(ss);
        ext.uid = ip;
        result.x.ext = ext;
        result
    }
}

impl Default for ServerTrans {
    fn default() -> Self {
        Self::new()
    }
}

/// Message to server task, includes oneshot Sender for reply.
pub struct ServerMessage {
    pub st: ServerTrans,
    pub reply: oneshot::Sender<ServerTrans>,
}

/// Extra transaction data.
pub struct TransExt {
    /// Shared State.
    pub ss: Option<Arc<SharedState>>,
    /// Id of requestor ( IP address or logged in user id ).
    pub uid: String,
    /// Signals there is new email to be sent.
    pub tx_email: bool,
    /// Signals time to sleep.
    pub sleep: u64,
    /// Signals wait for new transaction to be logged
    pub trans_wait: bool,
}

impl TransExt {
    fn new() -> Box<Self> {
        Box::new(Self {
            ss: None,
            uid: String::new(),
            tx_email: false,
            sleep: 0,
            trans_wait: false,
        })
    }

    /// Set limits, returns false if limit exceeded.
    pub fn set_dos(&self, uid: String, to: UA) -> bool {
        if let Some(ss) = &self.ss {
            ss.u_set_limits(uid, to)
        } else {
            true
        }
    }
}

/// http error
#[derive(Debug)]
pub struct Error {
    pub code: u16,
}

impl From<std::io::Error> for Error {
    fn from(_e: std::io::Error) -> Self {
        Self { code: 500 }
    }
}

impl std::error::Error for Error {}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.code)
    }
}
