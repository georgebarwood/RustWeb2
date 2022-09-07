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
    /// For notifying tasks waiting for transaction.
    pub wait_tx: broadcast::Sender<()>,
    /// Server is master ( not replicating another database ).
    pub is_master: bool,
    /// URL of master server.
    pub replicate_source: String,
    /// Cookies for replication.
    pub replicate_credentials: String,
    /// Denial of service limit.
    pub dos_limit: u64,
    /// Information for mitigating DoS attacks
    pub dos: Arc<Mutex<HashMap<String, IpInfo>>>,

    /// Trace time to process each request.
    pub tracetime: bool,

    /// Trace Dos
    pub tracedos: bool,
}

#[derive(Debug)]
pub struct IpInfo {
    used: u64,
    limit: u64,
}

impl IpInfo {
    fn new() -> Self {
        Self { used: 0, limit: 0 }
    }
}

impl SharedState {
    pub fn ip_budget(&self, ip: String) -> u64 {
        let mut m = self.dos.lock().unwrap();
        let info = m.entry(ip).or_insert_with(IpInfo::new);
        if info.limit == 0 {
            info.limit = self.dos_limit;
        }
        if info.used > info.limit {
            0
        } else {
            info.limit - info.used
        }
    }

    pub fn ip_used(&self, ip: &str, amount: u64) -> bool {
        let mut m = self.dos.lock().unwrap();
        if let Some(info) = m.get_mut(ip) {
            if self.tracedos {
                println!(
                    "ip_used ip={} delta={}% used={}%",
                    ip,
                    (amount) as f64 * 100f64 / info.limit as f64,
                    (info.used + amount) as f64 * 100f64 / info.limit as f64
                );
            }
            info.used += amount;
            info.used > info.limit
        } else {
            false
        }
    }

    pub fn set_ip_limit(&self, ip: String, limit: u64) {
        let mut m = self.dos.lock().unwrap();
        let info = m.entry(ip).or_insert_with(IpInfo::new);
        info.limit = limit;
    }

    /// Deflate old usage by 10% periodically.
    pub fn ip_decay(&self) {
        let mut m = self.dos.lock().unwrap();
        m.retain(|_ip, info| {
            if info.used > 0 {
                info.used -= 1 + info.used / 10;
            }
            info.used > 0
        });
    }

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
        st
    }
}

/// Transaction to be processed.
pub struct ServerTrans {
    pub x: GenTransaction,
    pub log: bool,
    pub readonly: bool,
    pub run_time: core::time::Duration,
}

impl ServerTrans {
    pub fn new() -> Self {
        let mut result = Self {
            x: GenTransaction::new(),
            log: true,
            readonly: false,
            run_time: Duration::from_micros(0),
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
        };
        let mut ext = TransExt::new();
        ext.ss = Some(ss);
        ext.ip = ip;
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
    /// IP Address of requestor.
    pub ip: String,
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
            ip: String::new(),
            tx_email: false,
            sleep: 0,
            trans_wait: false,
        })
    }

    pub fn set_dos(&self, to: u64) {
        if let Some(ss) = &self.ss {
            ss.set_ip_limit(self.ip.clone(), to * 1_000_000_000);
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
