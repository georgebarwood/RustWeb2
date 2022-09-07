/* New thoughts on Dos checking.

(1) Have several measures.

#Requests
#CPU usage
#Request IO
#Response IO

(2) Have builtin call to set limits and user ID ( if someone is logged on ).

(3) If request has a body, need to get user ID before reading body, to get correct budget for reading body.

SETDOS( userid (string), req_count, cpu_limit, req_limit, res_limit )

*/

use crate::share::{Error, ServerTrans, SharedState};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Process http requests.
pub async fn process(
    mut stream: tokio::net::TcpStream,
    ip: String,
    ss: Arc<SharedState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (r, mut w) = stream.split();
    let mut r = Buffer::new(r, ss.clone(), ip.clone());

    let mut used = [1, 0, 0, 0];

    let h = Headers::get(&mut r).await;
    let h = match h {
        Ok(h) => h,
        Err(e) => {
            if e.code == 0 {
                return Ok(());
            }
            return Err(e)?;
        }
    };
    let (hdrs, outp, uid) = {
        let mut st = ServerTrans::new_with_state(ss.clone(), r.ip.clone());
        let readonly = h.method == b"GET" || h.args.get("readonly").is_some();
        st.x.qy.path = h.path;
        st.x.qy.params = h.args;
        st.x.qy.cookies = h.cookies;
        let (ct, clen) = (&h.content_type, h.content_length);
        if ct.is_empty() {
            // No body.
        } else {

            st.readonly = true;
            let save = st.x.qy.sql.clone();
            st.x.qy.sql = Arc::new("EXEC web.SetUser()".to_string());
            st = ss.process(st).await;
            st.x.qy.sql = save;
            r.budget = ss.u_budget(st.uid.clone());
            st.readonly = false;

            if ct == b"application/x-www-form-urlencoded" {
                let clen: usize = clen.parse()?;
                let bytes = r.read(clen).await?;
                st.x.qy.form = serde_urlencoded::from_bytes(&bytes)?;
            } else if is_multipart(ct) {
                get_multipart(&mut r, &mut st.x.qy.parts).await?;
            } else {
                st.x.rp.status_code = 501;
            }
        }

        if st.x.rp.status_code == 200 { 
            st.readonly = readonly;
            // println!("qy={:?} readonly={}", st.x.qy, readonly);
            st = ss.process(st).await;
            used[2] = st.run_time.as_micros() as u64;
        }
        (header(&st), st.x.rp.output, st.uid)
    };

    // let _ = w.write_all(&hdrs).await;
    // let _ = w.write_all(&outp).await;

    let budget = r.budget[3];
    let mut load = write(&mut w, &hdrs, budget).await?;
    load += write(&mut w, &outp, budget - load).await?;

    used[1] = r.used();
    used[3] = load;
    ss.u_inc(&uid, used);

    ss.spd.trim_cache(); // Not sure if this is best place to do this or not.

    Ok(())
}

/// Get response header.
fn header(st: &ServerTrans) -> Vec<u8> {
    let mut h = Vec::with_capacity(4096);
    let status_line = format!("HTTP/1.1 {}\r\n", st.x.rp.status_code);
    h.extend_from_slice(status_line.as_bytes());
    for (name, value) in &st.x.rp.headers {
        h.extend_from_slice(name.as_bytes());
        h.push(b':');
        h.extend_from_slice(value.as_bytes());
        h.push(13);
        h.push(10);
    }
    let clen = st.x.rp.output.len();
    let x = format!("Content-Length: {clen}\r\n\r\n");
    h.extend_from_slice(x.as_bytes());
    h
}

// Header parsing.

#[derive(Default, Debug)]
struct Headers {
    method: Vec<u8>,
    path: String,
    args: BTreeMap<String, String>,
    host: String,
    cookies: BTreeMap<String, String>,

    content_type: Vec<u8>,
    content_length: String,
}

impl Headers {
    async fn get<'a>(br: &mut Buffer<'a>) -> Result<Headers, Error> {
        let mut r = Self::default();
        br.read_until(b' ', &mut r.method).await?;
        r.method.pop(); // Remove trailing space.

        let mut pq = Vec::new();
        br.read_until(b' ', &mut pq).await?;
        pq.pop(); // Remove trailing space.
        r.split_pq(&pq);

        let mut protocol = Vec::new();
        br.read_until(b'\n', &mut protocol).await?;

        let mut line0 = Vec::new();
        loop {
            let n = br.read_until(b'\n', &mut line0).await?;
            if n <= 2 {
                break;
            }
            let line = &line0[0..n - 2];
            if line.len() >= 2 {
                let b0 = lower(line[0]);
                let b2 = lower(line[2]);
                match (b0, b2) {
                    (b'c', b'o') => {
                        if let Some(n) = line_is(line, b"cookie") {
                            r.cookies = cookie_map(line, n);
                        }
                    }
                    (b'c', b'n') => {
                        if let Some(n) = line_is(line, b"content-type") {
                            r.content_type = ltob(line, n).to_vec();
                        } else if let Some(n) = line_is(line, b"content-length") {
                            r.content_length = ltos(line, n);
                        }
                    }
                    (b'h', b's') => {
                        if let Some(n) = line_is(line, b"host") {
                            r.host = ltos(line, n);
                        }
                    }
                    _ => {
                        if let Some(n) = line_is(line, b"x-real-ip") {
                            let ip = ltos(line, n);
                            br.budget = br.ss.u_budget(ip.clone());
                            br.ip = ip;
                        }
                    }
                }
            }
            line0.clear();
        }
        Ok(r)
    }

    /// Split the path and args by finding '?'.
    fn split_pq(&mut self, pq: &[u8]) {
        let n = pq.len();
        let mut i = 0;
        let mut q = n;
        while i < n {
            if pq[i] == b'?' {
                q = i;
                break;
            }
            i += 1;
        }
        self.path = tos(&pq[0..q]);
        if q != n {
            q += 1;
        }
        let qs = &pq[q..n];
        self.args = serde_urlencoded::from_bytes(qs).unwrap();
    }
}

/// Check whether current line is named header.
fn line_is(line: &[u8], name: &[u8]) -> Option<usize> {
    let n = name.len();
    if line.len() < n + 1 {
        return None;
    }
    if line[n] != b':' {
        return None;
    }
    for i in 0..n {
        if lower(line[i]) != name[i] {
            return None;
        }
    }
    Some(n + 1)
}

/// Trim header name.
fn ltob(line: &[u8], mut skip: usize) -> &[u8] {
    let n = line.len();
    while skip < n && line[skip] == b' ' {
        skip += 1;
    }
    &line[skip..n]
}

/// Header value as string.
fn ltos(line: &[u8], skip: usize) -> String {
    tos(ltob(line, skip))
}

/// Map upper case char to lower case.
fn lower(mut b: u8) -> u8 {
    if (b'A'..=b'Z').contains(&b) {
        b += 32;
    }
    b
}

/// Convert byte slice into string.
fn tos(s: &[u8]) -> String {
    std::str::from_utf8(s).unwrap().to_string()
}

/// Not enough input.
fn eof() -> Error {
    Error { code: 0 }
}

/// Too many requests.
fn tmr() -> Error {
    Error { code: 429 }
}

/// Parse cookie header to a map of cookies.
fn cookie_map(s: &[u8], skip: usize) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    let n = s.len();
    let mut i = skip;

    while i < n {
        while i < n && s[i] == b' ' {
            i += 1;
        }
        let start = i;
        while i < n && s[i] != b'=' {
            i += 1;
        }
        let name = tos(&s[start..i]);
        i += 1;
        let start = i;
        while i < n && s[i] != b';' {
            i += 1;
        }
        let value = tos(&s[start..i]);
        i += 1;
        map.insert(name, value);
    }
    map
}

/// Check content-type is multipart.
fn is_multipart(s: &[u8]) -> bool {
    let temp = b"multipart/form-data";
    let n = temp.len();
    temp == &s[0..n]
}

/// Extract name and file_name from content-disposition header.
fn split_cd(s: &[u8]) -> Option<(String, String)> {
    /* Expected input:
       form-data; name="file"; filename="logo.png"
    */
    let s = "multipart/".to_string() + std::str::from_utf8(s).unwrap();
    let (mut name, mut filename) = ("", "");
    let m: mime::Mime = s.parse().ok()?;
    assert!(m.subtype() == mime::FORM_DATA);
    if let Some(n) = m.get_param("name") {
        name = n.as_str()
    }
    if let Some(n) = m.get_param("filename") {
        filename = n.as_str()
    }
    Some((name.to_string(), filename.to_string()))
}

/*
Parts are delimited by boundary lines.
Each boundary line starts with --
The final boundary line has an extra --
Each part has headers, typically Content-Disposition and Content-Type.
Example:
------WebKitFormBoundaryAhgB6VordnzCD84Z
Content-Disposition: form-data; name="file"; filename=""
Content-Type: application/octet-stream


------WebKitFormBoundaryAhgB6VordnzCD84Z
Content-Disposition: form-data; name="submit"

Upload
------WebKitFormBoundaryAhgB6VordnzCD84Z--
*/

use rustdb::Part;

/// Parse multipart body.
async fn get_multipart<'a>(br: &mut Buffer<'a>, parts: &mut Vec<Part>) -> Result<(), Error> {
    let mut boundary = Vec::new();
    let n = br.read_until(10, &mut boundary).await?;
    if n < 4 {
        return Err(eof())?;
    }

    let bn = boundary.len() - 2;
    boundary.truncate(bn);

    let mut got_last = false;
    while !got_last {
        let mut part = Part::default();
        // Read headers
        let mut line0 = Vec::new();
        loop {
            let n = br.read_until(10, &mut line0).await?;
            if n <= 2 {
                break;
            }
            let line = &line0[0..n - 2];
            if let Some(n) = line_is(line, b"content-type") {
                part.content_type = tos(ltob(line, n));
                // Note: if part content-type is multipart, maybe it should be parsed.
            } else if let Some(n) = line_is(line, b"content-disposition") {
                let cd = ltob(line, n);
                if let Some((name, file_name)) = split_cd(cd) {
                    part.name = name;
                    part.file_name = file_name;
                }
            }
            line0.clear();
        }
        // Read lines into data looking for boundary.
        let mut data = Vec::new();
        loop {
            let n = br.read_until(10, &mut data).await?;
            if n == bn + 2 || n == bn + 4 {
                let start = data.len() - n;
                if data[start..start + bn] == boundary {
                    got_last = n == bn + 4;
                    data.truncate(start - 2);
                    break;
                }
            }
        }
        part.data = Arc::new(data);
        parts.push(part);
    }
    Ok(())
}

/// Buffer for reading tcp input stream, with budget check.
struct Buffer<'a> {
    stream: tokio::net::tcp::ReadHalf<'a>,
    buf: [u8; 2048],
    i: usize,
    n: usize,
    total: u64,
    budget: [u64; 4],
    timer: std::time::SystemTime,
    ss: Arc<SharedState>,
    ip: String,
}

impl<'a> Drop for Buffer<'a> {
    fn drop(&mut self) {
        // ToDo : call ss.u_inc
    }
}

impl<'a> Buffer<'a> {
    fn new(stream: tokio::net::tcp::ReadHalf<'a>, ss: Arc<SharedState>, ip: String) -> Self {
        let budget = ss.u_budget(ip.clone());
        Self {
            stream,
            buf: [0; 2048],
            i: 0,
            n: 0,
            total: 0,
            budget,
            timer: std::time::SystemTime::now(),
            ss,
            ip,
        }
    }

    fn used(&mut self) -> u64 {
        if self.total == 0 {
            return 0;
        }
        let elapsed = 1 + self.timer.elapsed().unwrap().as_micros() as u64;
        let result = elapsed as u64 * self.total as u64;
        self.total = 0;
        result
    }

    async fn fill(&mut self) -> Result<(), Error> {
        self.i = 0;
        let micros = self.budget[1] / (self.total + 1000);
        let bm = core::time::Duration::from_micros(micros as u64);
        let used = self.timer.elapsed().unwrap();
        if used >= bm {
            return Err(tmr());
        }
        let timeout = bm - used;

        tokio::select! {
            _ = tokio::time::sleep(timeout) =>
            {
               Err(tmr())?
            }
            rd = self.stream.read(&mut self.buf) =>
            {
                match rd
                {
                   Ok(n) =>
                   {
                     if n == 0 {
                        Err(eof())?
                     }
                     self.n = n;
                     self.total += n as u64;
                   }
                   Err(e) => { Err(e)? }
                }
            }

        }
        Ok(())
    }

    /// Read until delim is found. Returns eof error if input is closed.
    async fn read_until(&mut self, delim: u8, to: &mut Vec<u8>) -> Result<usize, Error> {
        let start = to.len();
        loop {
            if self.i == self.n {
                self.fill().await?;
            }
            let b = self.buf[self.i];
            self.i += 1;
            to.push(b);
            if b == delim {
                return Ok(to.len() - start);
            }
        }
    }

    /// Read specified number of bytes.
    async fn read(&mut self, n: usize) -> Result<Vec<u8>, Error> {
        let mut to = Vec::new();
        loop {
            if self.i == self.n {
                self.fill().await?;
            }
            let b = self.buf[self.i];
            self.i += 1;
            to.push(b);
            if to.len() == n {
                return Ok(to);
            }
        }
    }
}

/// Function to write response, with budgete-based timeout.
async fn write<'a>(
    w: &mut tokio::net::tcp::WriteHalf<'a>,
    data: &[u8],
    budget: u64,
) -> Result<u64, Error> {
    if data.is_empty() {
        return Ok(0);
    }
    let timer = std::time::SystemTime::now();
    let micros = budget / (data.len() as u64 + 1000);
    let timeout = core::time::Duration::from_micros(micros as u64);
    tokio::select! {
        _ = tokio::time::sleep(timeout) =>
            {
                Err(tmr())
            }
        x = w.write_all(data) =>
            {
               x?;
               let elapsed = timer.elapsed().unwrap();
               let load = elapsed.as_micros() as u64 * data.len() as u64;
               Ok(load)
            }
    }
}
