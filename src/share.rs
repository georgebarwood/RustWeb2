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

    /// Sender channel for sending queries to update task.
    pub update_tx: mpsc::Sender<UpdateMessage>,

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
    pub dos: Mutex<HashMap<String, UseInfo>>,

    /// Trace time to process each request.
    pub tracetime: bool,

    /// Trace Dos
    pub tracedos: bool,

    /// Trace memory
    pub tracemem: bool,
}

/// Usage array ( total or limit ).
pub type UA = [u64; 4];

/// Index into usage array for number of requests.
pub const U_COUNT: usize = 0;

/// Index into usage array for request read usage.
pub const U_READ: usize = 1;

/// Index into usage array for CPU usage ( time to process a request ).
pub const U_CPU: usize = 2;

/// Index into usage array for response write usage .
pub const U_WRITE: usize = 3;

/// Information kept on usage for each user.
#[derive(Debug, Default)]
pub struct UseInfo {
    /// Running totals of amount of CPU/IO etc. used.
    pub used: UA,
    /// Limits on usage.
    pub limit: UA,
}

impl SharedState {
    /// Get the usage budget for specified user ( difference between limit and usage ).
    pub fn u_budget(&self, uid: String) -> UA {
        let mut m = self.dos.lock().unwrap();
        let info = m.entry(uid).or_default();
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

    /// Increment usage linits for specified user.
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
                    100. * info.used[0] as f32 / info.limit[0] as f32,
                    100. * info.used[1] as f32 / info.limit[1] as f32,
                    100. * info.used[2] as f32 / info.limit[2] as f32,
                    100. * info.used[3] as f32 / info.limit[3] as f32,
                );
            }
        }
    }

    /// Set the limits for specified user. Result is no limit exceeded.
    pub fn u_set_limits(&self, uid: String, limit: UA) -> bool {
        let mut m = self.dos.lock().unwrap();
        let info = m.entry(uid).or_default();
        info.limit = limit;
        for i in 0..4 {
            if info.used[i] >= info.limit[i] {
                return false;
            }
        }
        true
    }

    /// Deflate old usage by 10% periodically. Items with zero usage are removed.
    pub fn u_decay(&self) {
        let mut m = self.dos.lock().unwrap();
        m.retain(|_uid, info| {
            let mut nz = false;
            for i in 0..4 {
                if info.used[i] > 0 {
                    info.used[i] -= 1 + info.used[i] / 10;
                }
                nz = nz || info.used[i] > 0;
            }
            nz
        });
    }

    /// Called to notify tasks waiting for new transaction.
    pub fn new_trans(&self) {
        let _ = self.wait_tx.send(());
    }

    /// Process a server transaction.
    pub async fn process(&self, mut trans: Trans) -> Trans {
        let start = std::time::SystemTime::now();
        let mut trans = if trans.readonly {
            // Readonly request, use read-only copy of database.
            let spd = self.spd.clone();
            let bmap = self.bmap.clone();
            let task = tokio::task::spawn_blocking(move || {
                let apd = rustdb::AccessPagedData::new_reader(spd);
                let db = rustdb::Database::new(apd, "", bmap);
                let sql = trans.x.qy.sql.clone();
                db.run(&sql, &mut trans.x);
                trans
            });
            task.await.unwrap()
        } else {
            let (reply, rx) = oneshot::channel::<Trans>();
            let _ = self.update_tx.send(UpdateMessage { trans, reply }).await;
            rx.await.unwrap()
        };
        if trans.updates > 0 {
            self.new_trans();
        }
        trans.run_time = start.elapsed().unwrap();

        let ext = trans.x.get_extension();
        if let Some(ext) = ext.downcast_ref::<TransExt>() {
            trans.uid = ext.uid.clone();
            if self.is_master {
                if ext.sleep > 0 {
                    let _ = self.sleep_tx.send(ext.sleep);
                }
                if ext.tx_email {
                    let _ = self.email_tx.send(());
                }
            }
            if ext.trans_wait {
                let mut wait_rx = self.wait_tx.subscribe();
                tokio::select! {
                   _ = wait_rx.recv() => {}
                   _ = tokio::time::sleep(Duration::from_secs(600)) => {}
                }
            }
            if ext.trans_flush {
                let spd = self.spd.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    spd.wait_complete();
                })
                .await;
            }
            if ext.to_pdf {
                trans.convert_to_pdf();
            }
        }
        trans.x.set_extension(ext);
        trans
    }
}

/// Transaction to be processed.
pub struct Trans {
    pub x: GenTransaction,
    pub log: bool,
    pub readonly: bool,
    pub run_time: core::time::Duration,
    pub updates: usize,
    pub uid: String,
}

impl Trans {
    fn make() -> Self {
        Self {
            x: GenTransaction::new(),
            log: true,
            readonly: false,
            run_time: Duration::from_micros(0),
            updates: 0,
            uid: String::new(),
        }
    }

    pub fn new() -> Self {
        let mut result = Self::make();
        result.x.ext = TransExt::new();
        result
    }

    pub fn new_with_state(ss: Arc<SharedState>, ip: String) -> Self {
        let mut result = Self::make();
        let mut ext = TransExt::new();
        ext.ss = Some(ss);
        ext.uid = ip;
        result.x.ext = ext;
        result
    }

    fn convert_to_pdf(&mut self) {
        let source = &self.x.rp.output;
        let mut w = pdf_min::Writer::default();
        pdf_min::html(&mut w, source);
        w.finish();
        self.x.rp.output = w.b.b;
    }

    pub fn no_log(&mut self) -> bool {
        let mut result = false;
        let ext = self.x.get_extension();
        if let Some(ext) = ext.downcast_ref::<TransExt>() {
            result = ext.no_log;
        }
        self.x.set_extension(ext);
        result
    }
}

impl Default for Trans {
    fn default() -> Self {
        Self::new()
    }
}

/// Message to update task, includes oneshot Sender for reply.
pub struct UpdateMessage {
    pub trans: Trans,
    pub reply: oneshot::Sender<Trans>,
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
    /// Signals wait for transactions to be flushed
    pub trans_flush: bool,
    /// Transform html output to pdf.
    pub to_pdf: bool,
    /// Do not log transaction.
    pub no_log: bool,
}

impl TransExt {
    fn new() -> Box<Self> {
        Box::new(Self {
            ss: None,
            uid: String::new(),
            tx_email: false,
            sleep: 0,
            trans_wait: false,
            trans_flush: false,
            to_pdf: false,
            no_log: false,
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

impl From<std::str::Utf8Error> for Error {
    fn from(_e: std::str::Utf8Error) -> Self {
        Self { code: 400 } // 400 = HTTTP Bad Request
    }
}

impl From<serde_urlencoded::de::Error> for Error {
    fn from(_e: serde_urlencoded::de::Error) -> Self {
        Self { code: 400 } // 400 = HTTTP Bad Request
    }
}

impl std::error::Error for Error {}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.code)
    }
}
