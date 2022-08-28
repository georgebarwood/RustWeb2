use rustdb::{GenTransaction, Transaction};
use std::sync::Arc;
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
    /// Trace time to process each request.
    pub tracetime: bool,
}

impl SharedState {
    pub async fn process(&self, mut st: ServerTrans) -> ServerTrans {
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
                   _ = tokio::time::sleep(core::time::Duration::from_secs(600)) => {}
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
}

impl ServerTrans {
    pub fn new() -> Self {
        let mut result = Self {
            x: GenTransaction::new(),
            log: true,
            readonly: false,
        };
        result.x.ext = TransExt::new();
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
#[derive(Default)]
pub struct TransExt {
    /// Signals there is new email to be sent.
    pub tx_email: bool,
    /// Signals time to sleep.
    pub sleep: u64,
    /// Signals wait for new transaction to be logged
    pub trans_wait: bool,
}

impl TransExt {
    fn new() -> Box<Self> {
        Box::new(Self::default())
    }
}
