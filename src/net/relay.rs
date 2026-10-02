//! 跨主机 worker 中继传输 (跨主机订阅的第二阶段)
//!
//! 跨线程订阅复用进程内 channel; 跨主机则把同样的 `RelayedRtp` / 控制命令
//! 序列化后经 UDP 发送到对端进程的 **relay 端口**, 对端解码后按
//! `target_worker` 重新注入本地中继/命令 channel, 从而**完全复用** worker
//! 事件循环里的 `forward_relayed_rtp` 与命令处理逻辑。
//!
//! ```text
//!   Host A (worker 0)                         Host B (worker 4)
//!   publisher ── decrypt ──┐                  ┌── worker4 事件循环
//!                          │  encode          │   forward_relayed_rtp
//!                          └──► UDP relay ───►│ 按 target_worker 注入
//!                               (relay.rs)    └── RelayedRtp channel + wake
//! ```
//!
//! 帧格式为手写的紧凑二进制 (避免逐包 JSON/依赖)。`PayloadParams` 用
//! serde_json 编码并随每个数据包携带 (接收端按 `(publisher_id, mid)` 缓存)。

use crate::worker::RelayedRtp;
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Instant;

use str0m::format::PayloadParams;
use str0m::media::{Mid, Rid};
use str0m::rtp::{ExtensionValues, SeqNo, Ssrc};

/// 数据包: 一个中继 RTP 包
pub const MSG_RTP: u8 = 1;
/// 控制: 在发布者 worker 登记远端订阅者
pub const MSG_REGISTER: u8 = 2;
/// 控制: 注销远端订阅者
pub const MSG_UNREGISTER: u8 = 3;
/// 控制: 请求关键帧
pub const MSG_REQ_KEYFRAME: u8 = 4;

/// 反序列化后的中继数据包 (params 已解析)。
pub struct DecodedRtp {
    pub target_worker: u32,
    pub pkt: RelayedRtp,
}

/// 解码后的控制消息。
pub enum DecodedControl {
    Register {
        target_worker: u32,
        publisher_id: String,
        publisher_mid: Mid,
        subscriber_worker: usize,
        subscriber_id: String,
        subscriber_mid: Mid,
    },
    Unregister {
        target_worker: u32,
        publisher_id: String,
        publisher_mid: Mid,
        subscriber_id: String,
    },
    RequestKeyframe {
        target_worker: u32,
        publisher_id: String,
        publisher_mid: Mid,
        rid: Option<Rid>,
    },
}

/// 解码结果。
#[allow(clippy::large_enum_variant)]
pub enum Decoded {
    Rtp(DecodedRtp),
    Control(DecodedControl),
}

// ──────────────────────────────────────────────────────────
// 写游标
// ──────────────────────────────────────────────────────────

struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(1500),
        }
    }
    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn i16(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn bytes(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.buf.extend_from_slice(b);
    }
    fn str(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }
    fn mid(&mut self, m: Mid) {
        self.str(&m.to_string());
    }
}

// ──────────────────────────────────────────────────────────
// 读游标
// ──────────────────────────────────────────────────────────

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn u8(&mut self) -> io::Result<u8> {
        let b = *self.buf.get(self.pos).ok_or_else(truncated)?;
        self.pos += 1;
        Ok(b)
    }
    fn u32(&mut self) -> io::Result<u32> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }
    fn u64(&mut self) -> io::Result<u64> {
        let s = self.take(8)?;
        let mut b = [0u8; 8];
        b.copy_from_slice(s);
        Ok(u64::from_le_bytes(b))
    }
    fn i16(&mut self) -> io::Result<i16> {
        let s = self.take(2)?;
        Ok(i16::from_le_bytes([s[0], s[1]]))
    }
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(truncated());
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn bytes(&mut self) -> io::Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn string(&mut self) -> io::Result<String> {
        let b = self.bytes()?;
        String::from_utf8(b.to_vec()).map_err(|_| bad_data())
    }
    fn mid(&mut self) -> io::Result<Mid> {
        Ok(Mid::from(self.string()?.as_str()))
    }
}

fn truncated() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "relay packet truncated")
}
fn bad_data() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "relay packet malformed")
}

// ──────────────────────────────────────────────────────────
// 编码
// ──────────────────────────────────────────────────────────

/// 编码一个中继 RTP 包 (target_worker = 接收端本地的目标 worker 全局编号)。
///
/// `include_params` 为 true 时随包携带完整 `PayloadParams` (JSON)，否则仅写
/// 0 长度占位，由接收端从 `ParamsCache` 取。发送方应周期性携带一次，
/// 以容忍中继 UDP 丢包。
pub fn encode_rtp(target_worker: u32, pkt: &RelayedRtp, include_params: bool) -> Vec<u8> {
    let mut w = Writer::new();
    w.u8(MSG_RTP);
    w.u32(target_worker);
    w.str(&pkt.publisher_id);
    w.mid(pkt.publisher_mid);
    w.u32(*pkt.ssrc);
    w.u8(*pkt.params.pt());
    w.u64(*pkt.seq_no);
    w.u32(pkt.timestamp);
    w.u8(pkt.marker as u8);
    w.u8(pkt.rid.is_some() as u8);
    if let Some(rid) = pkt.rid {
        w.str(&rid.to_string());
    }
    // 扩展值子集 (转发音频所需)
    w.i16(pkt.ext_vals.audio_level.unwrap_or(i8::MIN) as i16);
    w.u8(match pkt.ext_vals.voice_activity {
        Some(true) => 1,
        Some(false) => 0,
        None => 0xff,
    });
    // params: 仅按需携带完整 JSON, 否则 0 长度占位 (接收端缓存)
    if include_params {
        let params_json = serde_json::to_vec(&pkt.params).unwrap_or_default();
        w.bytes(&params_json);
    } else {
        w.bytes(&[]);
    }
    // payload
    w.bytes(&pkt.payload);
    w.buf
}

/// 编码控制: 登记远端订阅者。
#[allow(clippy::too_many_arguments)]
pub fn encode_register(
    target_worker: u32,
    publisher_id: &str,
    publisher_mid: Mid,
    subscriber_worker: usize,
    subscriber_id: &str,
    subscriber_mid: Mid,
) -> Vec<u8> {
    let mut w = Writer::new();
    w.u8(MSG_REGISTER);
    w.u32(target_worker);
    w.str(publisher_id);
    w.mid(publisher_mid);
    w.u32(subscriber_worker as u32);
    w.str(subscriber_id);
    w.mid(subscriber_mid);
    w.buf
}

/// 编码控制: 注销远端订阅者。
pub fn encode_unregister(
    target_worker: u32,
    publisher_id: &str,
    publisher_mid: Mid,
    subscriber_id: &str,
) -> Vec<u8> {
    let mut w = Writer::new();
    w.u8(MSG_UNREGISTER);
    w.u32(target_worker);
    w.str(publisher_id);
    w.mid(publisher_mid);
    w.str(subscriber_id);
    w.buf
}

/// 编码控制: 请求关键帧。
pub fn encode_request_keyframe(
    target_worker: u32,
    publisher_id: &str,
    publisher_mid: Mid,
    rid: Option<Rid>,
) -> Vec<u8> {
    let mut w = Writer::new();
    w.u8(MSG_REQ_KEYFRAME);
    w.u32(target_worker);
    w.str(publisher_id);
    w.mid(publisher_mid);
    w.u8(rid.is_some() as u8);
    if let Some(r) = rid {
        w.str(&r.to_string());
    }
    w.buf
}

// ──────────────────────────────────────────────────────────
// 解码
// ──────────────────────────────────────────────────────────

/// 跨主机中继接收端的参数缓存。
///
/// 数据包仅在发送端周期性携带参数时才会更新本缓存，其余包 (0 长度 params)
/// 从缓存取，避免每包 JSON 解析。按 `(publisher_id, mid)` 组织，`get` 不分配。
#[derive(Default)]
pub struct ParamsCache {
    map: HashMap<String, HashMap<Mid, PayloadParams>>,
}

impl ParamsCache {
    fn get(&self, publisher_id: &str, mid: Mid) -> Option<PayloadParams> {
        self.map
            .get(publisher_id)
            .and_then(|m| m.get(&mid))
            .copied()
    }

    fn put(&mut self, publisher_id: &str, mid: Mid, params: PayloadParams) {
        self.map
            .entry(publisher_id.to_string())
            .or_default()
            .insert(mid, params);
    }
}

/// 解码一个 relay UDP 包。
///
/// `cache` 保存各 `(publisher_id, mid)` 最近一次收到的 `PayloadParams`；
/// 数据包未携带参数时从缓存取。
pub fn decode(buf: &[u8], cache: &mut ParamsCache) -> io::Result<Decoded> {
    let mut r = Reader::new(buf);
    let msg = r.u8()?;
    let target_worker = r.u32()?;
    match msg {
        MSG_RTP => {
            let publisher_id = r.string()?;
            let publisher_mid = r.mid()?;
            let ssrc = Ssrc::from(r.u32()?);
            let pt = str0m::media::Pt::from(r.u8()?);
            let seq_no = SeqNo::from(r.u64()?);
            let timestamp = r.u32()?;
            let marker = r.u8()? != 0;
            let has_rid = r.u8()? != 0;
            let rid = if has_rid {
                Some(Rid::from(r.string()?.as_str()))
            } else {
                None
            };
            let audio_level = r.i16()?;
            let va = r.u8()?;
            let params_json = r.bytes()?;
            let params = if params_json.is_empty() {
                match cache.get(&publisher_id, publisher_mid) {
                    Some(p) => p,
                    None => {
                        // 参数尚未到达 (或缓存被丢包跳过): 丢弃该数据包。
                        return Err(io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "relay 数据包缺少参数且无缓存",
                        ));
                    }
                }
            } else {
                let p: PayloadParams =
                    serde_json::from_slice(params_json).map_err(|_| bad_data())?;
                cache.put(&publisher_id, publisher_mid, p);
                p
            };
            let payload = r.bytes()?.to_vec();

            // audio_level 用 i8::MIN 作为 None 哨兵
            let audio_level = if audio_level == i8::MIN as i16 {
                None
            } else {
                Some(audio_level as i8)
            };
            let voice_activity = match va {
                0 => Some(false),
                1 => Some(true),
                _ => None,
            };
            let ext_vals = ExtensionValues {
                audio_level,
                voice_activity,
                ..Default::default()
            };
            // 保证 pt 与 params 一致
            debug_assert_eq!(*params.pt(), *pt);

            Ok(Decoded::Rtp(DecodedRtp {
                target_worker,
                pkt: RelayedRtp {
                    publisher_id: Arc::from(publisher_id.as_str()),
                    publisher_mid,
                    params,
                    seq_no,
                    timestamp,
                    marker,
                    ext_vals,
                    rid,
                    ssrc,
                    // 跨主机无法传递本地 Instant, 用接收时刻
                    arrival: Instant::now(),
                    payload: Arc::from(payload),
                },
            }))
        }
        MSG_REGISTER => {
            let publisher_id = r.string()?;
            let publisher_mid = r.mid()?;
            let subscriber_worker = r.u32()? as usize;
            let subscriber_id = r.string()?;
            let subscriber_mid = r.mid()?;
            Ok(Decoded::Control(DecodedControl::Register {
                target_worker,
                publisher_id,
                publisher_mid,
                subscriber_worker,
                subscriber_id,
                subscriber_mid,
            }))
        }
        MSG_UNREGISTER => {
            let publisher_id = r.string()?;
            let publisher_mid = r.mid()?;
            let subscriber_id = r.string()?;
            Ok(Decoded::Control(DecodedControl::Unregister {
                target_worker,
                publisher_id,
                publisher_mid,
                subscriber_id,
            }))
        }
        MSG_REQ_KEYFRAME => {
            let publisher_id = r.string()?;
            let publisher_mid = r.mid()?;
            let has_rid = r.u8()? != 0;
            let rid = if has_rid {
                Some(Rid::from(r.string()?.as_str()))
            } else {
                None
            };
            Ok(Decoded::Control(DecodedControl::RequestKeyframe {
                target_worker,
                publisher_id,
                publisher_mid,
                rid,
            }))
        }
        _ => Err(bad_data()),
    }
}

// ──────────────────────────────────────────────────────────
// 进程级 relay 端点
// ──────────────────────────────────────────────────────────

/// 本进程的 relay UDP 端点: 绑定监听端口, 同时用于向对端发送。
pub struct RelayEndpoint {
    sock: UdpSocket,
}

impl RelayEndpoint {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let sock = UdpSocket::bind(addr)?;
        sock.set_nonblocking(false)?;
        Ok(Self { sock })
    }

    /// 监听地址 (仅测试使用)。
    #[cfg(test)]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// 发送已编码的 relay 包。
    pub fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        self.sock.send_to(buf, addr)
    }

    /// 阻塞接收一个 relay 包 (供独立的 relay 线程使用)。
    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.sock.recv_from(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use str0m::format::CodecConfig;

    fn sample_params() -> PayloadParams {
        CodecConfig::new_with_defaults().params()[0]
    }

    #[test]
    fn rtp_roundtrip() {
        let params = sample_params();
        let mut ext = ExtensionValues::default();
        ext.audio_level = Some(-42);
        ext.voice_activity = Some(true);
        let pkt = RelayedRtp {
            publisher_id: Arc::from("pub-1"),
            publisher_mid: Mid::from("1"),
            params,
            seq_no: SeqNo::from(12345),
            timestamp: 90000,
            marker: true,
            ext_vals: ext,
            rid: Some(Rid::from("h")),
            ssrc: Ssrc::from(777),
            arrival: Instant::now(),
            payload: Arc::from(&[1u8, 2, 3, 4, 5][..]),
        };
        let buf = encode_rtp(4, &pkt, true);
        let mut cache = ParamsCache::default();
        match decode(&buf, &mut cache).unwrap() {
            Decoded::Rtp(d) => {
                assert_eq!(d.target_worker, 4);
                assert_eq!(&*d.pkt.publisher_id, "pub-1");
                assert_eq!(d.pkt.publisher_mid, Mid::from("1"));
                assert_eq!(d.pkt.seq_no, SeqNo::from(12345));
                assert_eq!(d.pkt.timestamp, 90000);
                assert!(d.pkt.marker);
                assert_eq!(d.pkt.rid, Some(Rid::from("h")));
                assert_eq!(d.pkt.ssrc, Ssrc::from(777));
                assert_eq!(d.pkt.ext_vals.audio_level, Some(-42));
                assert_eq!(d.pkt.ext_vals.voice_activity, Some(true));
                assert_eq!(&*d.pkt.payload, &[1u8, 2, 3, 4, 5]);
            }
            _ => panic!("expected RTP"),
        }

        // 后续包不携带 params, 应从缓存解出同一份参数。
        let lean = encode_rtp(4, &pkt, false);
        assert!(lean.len() < buf.len(), "省略 params 的包应更小");
        match decode(&lean, &mut cache).unwrap() {
            Decoded::Rtp(d) => assert_eq!(d.pkt.params, pkt.params),
            _ => panic!("expected RTP"),
        }
        // 空缓存时缺少 params 应报错 (而不是解出错误的参数)。
        assert!(decode(&lean, &mut ParamsCache::default()).is_err());
    }

    #[test]
    fn control_roundtrip() {
        let b = encode_register(7, "p", Mid::from("0"), 2, "sub-1", Mid::from("s"));
        match decode(&b, &mut ParamsCache::default()).unwrap() {
            Decoded::Control(DecodedControl::Register {
                target_worker,
                publisher_id,
                subscriber_worker,
                subscriber_id,
                subscriber_mid,
                ..
            }) => {
                assert_eq!(target_worker, 7);
                assert_eq!(publisher_id, "p");
                assert_eq!(subscriber_worker, 2);
                assert_eq!(subscriber_id, "sub-1");
                assert_eq!(subscriber_mid, Mid::from("s"));
            }
            _ => panic!("expected Register"),
        }

        let b = encode_request_keyframe(3, "p", Mid::from("1"), Some(Rid::from("l")));
        match decode(&b, &mut ParamsCache::default()).unwrap() {
            Decoded::Control(DecodedControl::RequestKeyframe { rid, .. }) => {
                assert_eq!(rid, Some(Rid::from("l")));
            }
            _ => panic!("expected RequestKeyframe"),
        }
    }

    #[test]
    fn decode_rejects_truncated() {
        assert!(decode(&[], &mut ParamsCache::default()).is_err());
        assert!(decode(&[MSG_RTP, 0, 0], &mut ParamsCache::default()).is_err());
    }

    /// 两个进程级端点通过 UDP 真实收发一个 relay 包。
    #[test]
    fn udp_endpoint_roundtrip() {
        let a = RelayEndpoint::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = RelayEndpoint::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let b_addr = b.local_addr().unwrap();

        let pkt = RelayedRtp {
            publisher_id: Arc::from("pub"),
            publisher_mid: Mid::from("0"),
            params: sample_params(),
            seq_no: SeqNo::from(1),
            timestamp: 2,
            marker: false,
            ext_vals: ExtensionValues::default(),
            rid: None,
            ssrc: Ssrc::from(9),
            arrival: Instant::now(),
            payload: Arc::from(&[9u8, 8, 7][..]),
        };
        let buf = encode_rtp(0, &pkt, true);
        a.send_to(&buf, b_addr).unwrap();

        let mut rbuf = [0u8; 2048];
        let (n, _src) = b.recv_from(&mut rbuf).unwrap();
        match decode(&rbuf[..n], &mut ParamsCache::default()).unwrap() {
            Decoded::Rtp(d) => assert_eq!(&*d.pkt.payload, &[9u8, 8, 7]),
            _ => panic!("expected RTP"),
        }
    }
}
