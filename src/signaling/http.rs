//! WHIP / WHEP 共用的极简 HTTP 服务原语与 SDP 工具。
//!
//! 只实现两者需要的部分: 读取一次请求 (含 body)、查询串解析、SDP answer
//! 候选注入、trickle-ice fragment 解析、简单响应写出。避免引入完整 HTTP 栈。

use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// 解析后的 HTTP 请求。
pub struct HttpRequest {
    pub method: String,
    pub target: String,
    /// 头部 (键统一转小写)
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

pub fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

pub async fn read_request(stream: &mut TcpStream) -> io::Result<Option<HttpRequest>> {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "no header end",
            ));
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 1 << 20 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header too large",
            ));
        }
    };

    let head = String::from_utf8_lossy(&buf[..header_end]);
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();

    let mut content_length = 0usize;
    let mut headers: Vec<(String, String)> = Vec::new();
    for l in lines {
        let Some((k, v)) = l.split_once(':') else {
            continue;
        };
        let key = k.trim().to_ascii_lowercase();
        let val = v.trim().to_string();
        if key == "content-length" {
            content_length = val.parse().unwrap_or(0);
        }
        headers.push((key, val));
    }

    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);

    Ok(Some(HttpRequest {
        method,
        target,
        headers,
        body,
    }))
}

/// 返回 (path, query)。
pub fn parse_head(req: &HttpRequest) -> (String, String) {
    match req.target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (req.target.clone(), String::new()),
    }
}

pub fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return Some(percent_decode(v));
            }
        }
    }
    None
}

pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// 把 SFU 的 ICE 候选注入 SDP answer (非 trickle 场景)。
pub fn inject_candidates(sdp: &str, candidates: &[String]) -> String {
    let mut out = String::with_capacity(sdp.len() + 128);
    for line in sdp.lines() {
        out.push_str(line);
        out.push_str("\r\n");
        if line.starts_with("a=ice-pwd:") {
            for c in candidates {
                out.push_str("a=");
                out.push_str(c);
                out.push_str("\r\n");
            }
        }
    }
    out
}

/// 从 trickle-ice SDP fragment 中提取 (mid, candidate)。
pub fn parse_candidates(frag: &str) -> Vec<(Option<String>, String)> {
    let mut out = Vec::new();
    let mut cur_mid: Option<String> = None;
    for line in frag.lines() {
        if let Some(m) = line.strip_prefix("a=mid:") {
            cur_mid = Some(m.trim().to_string());
        } else if let Some(c) = line.strip_prefix("a=candidate:") {
            out.push((cur_mid.clone(), format!("candidate:{}", c.trim())));
        }
    }
    out
}

pub async fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    extra: &[(&str, &str)],
    body: &[u8],
) -> io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        status,
        content_type,
        body.len()
    );
    // CORS (浏览器客户端)
    head.push_str("Access-Control-Allow-Origin: *\r\n");
    head.push_str("Access-Control-Allow-Methods: POST, PATCH, DELETE, OPTIONS\r\n");
    head.push_str("Access-Control-Allow-Headers: Content-Type, Authorization\r\n");
    for (k, v) in extra {
        head.push_str(k);
        head.push_str(": ");
        head.push_str(v);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

/// 从 SDP offer 中按出现顺序提取各 m-section 的 (kind, mid)。
///
/// kind 取 `audio` / `video`; mid 来自该 section 的 `a=mid:`。
pub fn offer_mids(sdp: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut cur_kind: Option<String> = None;
    for line in sdp.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(rest) = line.strip_prefix("m=") {
            let kind = rest.split_whitespace().next().unwrap_or_default();
            cur_kind = match kind {
                "audio" | "video" => Some(kind.to_string()),
                _ => None,
            };
        } else if let Some(m) = line.strip_prefix("a=mid:") {
            if let Some(kind) = &cur_kind {
                out.push((kind.clone(), m.trim().to_string()));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inject_candidates_after_ice_pwd() {
        let sdp = "v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=ice-ufrag:xx\r\na=ice-pwd:yy\r\n";
        let out = inject_candidates(sdp, &["candidate:1 1 udp 2 127.0.0.1 5175 typ host".into()]);
        assert!(out.contains("a=ice-pwd:yy\r\na=candidate:1 1 udp 2 127.0.0.1 5175 typ host\r\n"));
    }

    #[test]
    fn parse_trickle_fragment() {
        let frag = "a=ice-ufrag:xx\r\na=mid:0\r\na=candidate:1 1 udp 2 1.2.3.4 9 typ host\r\n";
        let c = parse_candidates(frag);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].0.as_deref(), Some("0"));
        assert!(c[0].1.starts_with("candidate:"));
    }

    #[test]
    fn query_parsing() {
        assert_eq!(
            query_param("room=demo&x=1", "room").as_deref(),
            Some("demo")
        );
        assert_eq!(query_param("room=demo", "missing"), None);
    }

    #[test]
    fn offer_mids_in_order() {
        let sdp = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\na=recvonly\r\n\
                   m=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:1\r\na=recvonly\r\n";
        assert_eq!(
            offer_mids(sdp),
            vec![
                ("audio".to_string(), "0".to_string()),
                ("video".to_string(), "1".to_string())
            ]
        );
    }
}
