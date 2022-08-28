use crate::share::{ServerTrans, SharedState};
use crate::Result;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

/// Process http requests.
pub async fn process(mut socket: tokio::net::TcpStream, ss: Arc<SharedState>) -> Result<()> {
    loop {
        let (r, _w) = socket.split();
        let mut r = tokio::io::BufReader::with_capacity(2048, r);
        let h = Headers::get(&mut r).await?;

        let mut st = ServerTrans::new();
        st.readonly = h.method == b"GET" || h.args.get("readonly").is_some();
        st.x.qy.path = h.path;
        st.x.qy.params = h.args;
        st.x.qy.cookies = h.cookies;
        let (ct, clen) = (&h.content_type, h.content_length);

        if ct.is_empty() {
            // No body.
        } else if ct == b"application/x-www-form-urlencoded" {
            let mut bytes = Vec::new();
            let clen: usize = clen.parse()?;
            while bytes.len() < clen {
                let buf = r.fill_buf().await?;
                let buflen = buf.len();
                if buflen == 0 {
                    return Ok(());
                }
                let mut m = clen - bytes.len();
                if m > buflen {
                    m = buflen;
                }
                bytes.extend_from_slice(&buf[0..m]);
                r.consume(m);
            }
            st.x.qy.form = serde_urlencoded::from_bytes(&bytes)?;
        } else if is_multipart(ct) {
            get_multipart(&mut r, &mut st.x.qy.parts).await?;
        } else {
            return Err(nos())?;
        }

        //println!("qy={:?}", st.x.qy);

        st = ss.process(st).await;

        let hdrs = header(&st);
        let _ = socket.write_all(&hdrs).await;
        let body = &st.x.rp.output;
        let _ = socket.write_all(body).await;

        ss.spd.trim_cache(); // Not sure if this is best place to do this or not.
    }
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
    async fn get<R>(br: &mut tokio::io::BufReader<R>) -> Result<Headers>
    where
        R: AsyncReadExt + Unpin + Send,
    {
        let mut r = Self::default();
        let n = br.read_until(b' ', &mut r.method).await?;
        if n == 0 {
            return Err(eof())?;
        }
        r.method.pop(); // Remove trailing space.

        let mut pq = Vec::new();
        let n = br.read_until(b' ', &mut pq).await?;
        if n == 0 {
            return Err(eof())?;
        }
        pq.pop(); // Remove trailing space.
        r.split_pq(&pq);

        let mut protocol = Vec::new();
        let n = br.read_until(b'\n', &mut protocol).await?;
        if n == 0 {
            return Err(eof())?;
        }

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
                        if line_is(line, b"cookie") {
                            r.cookies = cookie_map(line);
                        }
                    }
                    (b'c', b'n') => {
                        if line_is(line, b"content-type") {
                            r.content_type = ltob(line, 13).to_vec();
                        } else if line_is(line, b"content-length") {
                            r.content_length = ltos(line, 15);
                        }
                    }
                    (b'h', b's') => {
                        if line_is(line, b"host") {
                            r.host = ltos(line, 5);
                        }
                    }
                    _ => {}
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
fn line_is(line: &[u8], name: &[u8]) -> bool {
    if line.len() < name.len() + 1 {
        return false;
    }
    if line[name.len()] != b':' {
        return false;
    }
    for i in 1..name.len() {
        if lower(line[i]) != name[i] {
            return false;
        }
    }
    true
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
    Error::from(ErrorKind::UnexpectedEof)
}

/// Unknown content type etc.
fn nos() -> Error {
    Error::from(ErrorKind::Unsupported)
}

/// Parse cookie header to a map of cookies.
fn cookie_map(s: &[u8]) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    let n = s.len() - 1;
    let mut i = 7;

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
async fn get_multipart<R>(br: &mut tokio::io::BufReader<R>, parts: &mut Vec<Part>) -> Result<()>
where
    R: AsyncReadExt + Unpin + Send,
{
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
            if line_is(line, b"content-type") {
                part.content_type = tos(ltob(line, 13));
                // Note: if part content-type is multipart, maybe it should be parsed.
            } else if line_is(line, b"content-disposition") {
                let cd = ltob(line, 20);
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
