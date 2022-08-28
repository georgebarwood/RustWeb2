use crate::share::*;
use rustdb::{AccessPagedData, Database};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Task for syncing with master database
pub async fn sync_loop(rx: oneshot::Receiver<bool>, state: Arc<SharedState>) {
    let db_is_new = rx.await.unwrap();
    if db_is_new {
        let sql = rget(state.clone(), "/ScriptExact?readonly").await;
        let sql = std::str::from_utf8(&sql).unwrap().to_string();
        let mut st = ServerTrans::new();
        st.log = false;
        st.x.qy.sql = Arc::new(sql);
        state.process(st).await;
        println!("New slave database initialised");
    }
    loop {
        let tid = {
            let apd = AccessPagedData::new_reader(state.spd.clone());
            let db = Database::new(apd, "", state.bmap.clone());
            let lt = db.table("log", "Transaction");
            lt.id_gen.get()
        };
        let url = format!("/GetTransaction?k={tid}");
        let ser = rget(state.clone(), &url).await;
        if !ser.is_empty() {
            let mut st = ServerTrans::new();
            st.x.qy = rmp_serde::from_slice(&ser).unwrap();
            state.process(st).await;
            println!("Slave database updated Transaction Id={tid}");
        }
    }
}

/// Sleep function that checks real time elapsed.
async fn sleep_real(secs: u64) {
    let start = std::time::SystemTime::now();
    for _ in (0..secs).step_by(10) {
        tokio::time::sleep(core::time::Duration::from_secs(10)).await;
        match start.elapsed() {
            Ok(e) => {
                if e >= core::time::Duration::from_secs(secs) {
                    return;
                }
            }
            Err(_) => {
                return;
            }
        }
    }
}

/// Get data from master server, retries in case of error.
async fn rget(state: Arc<SharedState>, query: &str) -> Vec<u8> {
    // get a client builder
    let client = reqwest::Client::builder()
        .default_headers(reqwest::header::HeaderMap::new())
        .build()
        .unwrap();
    loop {
        let mut retry_delay = true;
        let req = client
            .get(state.replicate_source.clone() + query)
            .header("Cookie", state.replicate_credentials.clone());

        tokio::select! {
            response = req.send() =>
            {
                match response
                {
                  Ok(r) => {
                     let status = r.status();
                     if status.is_success()
                     {
                         match r.bytes().await {
                            Ok(b) => { return b.to_vec(); }
                            Err(e) => { println!("rget failed to get bytes err={e}" ); }
                         }
                     } else {
                         println!("rget bad response status = {status}");
                     }
                  }
                  Err(e) => {
                    println!("rget send error {e}");
                  }
               }
            }
            _ = sleep_real(800) =>
            {
              println!( "rget timed out after 800 seconds" );
              retry_delay = false;
            }
        }
        if retry_delay {
            // Wait before retrying after error/timeout.
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
    }
}

/// Task for sleeping - calls timed.Run once sleep time has elapsed.
pub async fn sleep_loop(mut rx: mpsc::UnboundedReceiver<u64>, state: Arc<SharedState>) {
    let mut sleep_micro = 5000000;
    loop {
        tokio::select! {
            ns = rx.recv() => { sleep_micro = ns.unwrap(); }
            _ = tokio::time::sleep(core::time::Duration::from_micros(sleep_micro)) =>
            {
              if state.is_master
              {
                let mut st = ServerTrans::new();
                st.x.qy.sql = Arc::new("EXEC timed.Run()".to_string());
                state.process(st).await;
              }
            }
        }
    }
}

/// task that sends emails
pub async fn email_loop(mut rx: mpsc::UnboundedReceiver<()>, state: Arc<SharedState>) {
    loop {
        let mut send_list = Vec::new();
        {
            let _ = rx.recv().await;
            let apd = AccessPagedData::new_reader(state.spd.clone());
            let db = Database::new(apd, "", state.bmap.clone());
            let qt = db.table("email", "Queue");
            let mt = db.table("email", "Msg");
            let at = db.table("email", "SmtpAccount");

            for (pp, off) in qt.scan(&db) {
                let p = &pp.borrow();
                let a = qt.access(p, off);
                let msg = a.int(0) as u64;

                if let Some((pp, off)) = mt.id_get(&db, msg) {
                    let p = &pp.borrow();
                    let a = mt.access(p, off);
                    let from = a.str(&db, 0);
                    let to = a.str(&db, 1);
                    let title = a.str(&db, 2);
                    let body = a.str(&db, 3);
                    let format = a.int(4);
                    let account = a.int(5) as u64;

                    if let Some((pp, off)) = at.id_get(&db, account) {
                        let p = &pp.borrow();
                        let a = at.access(p, off);
                        let server = a.str(&db, 0);
                        let username = a.str(&db, 1);
                        let password = a.str(&db, 2);

                        send_list.push((
                            msg,
                            (from, to, title, body, format),
                            (server, username, password),
                        ));
                    }
                }
            }
        }
        for (msg, email, account) in send_list {
            let blocking_task = tokio::task::spawn_blocking(move || send_email(email, account));
            let result = blocking_task.await.unwrap();
            match result {
                Ok(_) => email_sent(&state, msg).await,
                Err(e) => match e {
                    EmailError::Address(ae) => {
                        email_error(&state, msg, 0, ae.to_string()).await;
                    }
                    EmailError::Lettre(le) => {
                        email_error(&state, msg, 0, le.to_string()).await;
                    }
                    EmailError::Send(se) => {
                        let retry = if se.is_transient() { 1 } else { 0 };
                        email_error(&state, msg, retry, se.to_string()).await;
                    }
                },
            }
        }
    }
}

/// Error enum for send_email
#[derive(Debug)]
enum EmailError {
    Address(lettre::address::AddressError),
    Lettre(lettre::error::Error),
    Send(lettre::transport::smtp::Error),
}

impl From<lettre::address::AddressError> for EmailError {
    fn from(e: lettre::address::AddressError) -> Self {
        EmailError::Address(e)
    }
}

impl From<lettre::error::Error> for EmailError {
    fn from(e: lettre::error::Error) -> Self {
        EmailError::Lettre(e)
    }
}

impl From<lettre::transport::smtp::Error> for EmailError {
    fn from(e: lettre::transport::smtp::Error) -> Self {
        EmailError::Send(e)
    }
}

/// Send an email using lettre.
fn send_email(
    (from, to, title, body, format): (String, String, String, String, i64),
    (server, username, password): (String, String, String),
) -> Result<(), EmailError> {
    use lettre::{
        message::SinglePart,
        transport::smtp::{
            authentication::{Credentials, Mechanism},
            PoolConfig,
        },
        Message, SmtpTransport, Transport,
    };

    let body = match format {
        1 => SinglePart::html(body),
        _ => SinglePart::plain(body),
    };

    let email = Message::builder()
        .to(to.parse()?)
        .from(from.parse()?)
        .subject(title)
        .singlepart(body)?;

    // Create TLS transport on port 587 with STARTTLS
    let sender = SmtpTransport::starttls_relay(&server)?
        // Add credentials for authentication
        .credentials(Credentials::new(username, password))
        // Configure expected authentication mechanism
        .authentication(vec![Mechanism::Plain])
        // Connection pool settings
        .pool_config(PoolConfig::new().max_size(20))
        .build();

    let _result = sender.send(&email)?;
    Ok(())
}

/// Update the database to reflect an email was sent.
async fn email_sent(state: &SharedState, msg: u64) {
    let mut st = ServerTrans::new();
    st.x.qy.sql = Arc::new(format!("EXEC email.Sent({})", msg));
    state.process(st).await;
}

/// Update the database to reflect an error occurred sending an email.
async fn email_error(state: &SharedState, msg: u64, retry: i8, err: String) {
    let mut st = ServerTrans::new();
    let src = format!("EXEC email.LogSendError({},{},'{}')", msg, retry, err);
    st.x.qy.sql = Arc::new(src);
    state.process(st).await;
}
