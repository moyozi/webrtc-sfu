//! Worker 层单元测试

use super::forward::RemotePublisherStats;
use super::relay::{dispatch_relay_control, RelayHub, RelayedRtp};
use super::WorkerCommand;
use crossbeam_channel::unbounded;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, Instant};
use str0m::media::{Mid, Rid};
use str0m::rtp::{ExtensionValues, SeqNo, Ssrc};

/// RemotePublisherStats 应能按 RID 选出层、按 TID 计算累计时间层。
#[test]
fn remote_stats_layers_and_temporal() {
    let now = Instant::now();
    let mid = Mid::from("1");
    let mut s = RemotePublisherStats::default();

    // 跨过 500ms 窗口 (首包 +100ms, 到 +600ms 触发)
    for i in 1..=6u64 {
        let t = now + Duration::from_millis(100 * i);
        s.observe(Ssrc::from(10), mid, Some(Rid::from("l")), None, 1000, t);
        s.observe(Ssrc::from(20), mid, Some(Rid::from("h")), None, 2000, t);
        // SVC 时间层 (单 SSRC)
        s.observe(Ssrc::from(30), mid, None, Some(0), 1000, t);
        s.observe(Ssrc::from(30), mid, None, Some(1), 1000, t);
    }

    let layers = s.layer_rates(mid);
    assert_eq!(layers.len(), 2);
    assert_eq!(layers[0].0, Rid::from("l"));
    assert_eq!(layers[1].0, Rid::from("h"));
    assert!(layers[0].1 <= layers[1].1);

    let tr = s.temporal_rates(mid);
    assert_eq!(tr.len(), 2);
    assert_eq!(tr[0].0, 0);
    assert_eq!(tr[1].0, 1);
    assert!(tr[1].1 > tr[0].1);

    // 其它 mid 无数据
    assert!(s.layer_rates(Mid::from("9")).is_empty());
    assert!(s.temporal_rates(Mid::from("9")).is_empty());
}

/// RelayHub: 向目标 worker 投递中继包并唤醒。
#[test]
fn relay_hub_delivers_and_wakes() {
    let (rt, rr) = unbounded::<RelayedRtp>();
    let (wrx, wtx) = UnixStream::pair().unwrap();
    wrx.set_nonblocking(true).unwrap();
    wtx.set_nonblocking(true).unwrap();

    let hub = RelayHub {
        relay_txs: vec![rt],
        wake_txs: vec![Arc::new(wtx)],
        cmd_txs: vec![],
        worker_offset: 0,
        local_workers: 1,
        peer_addrs: Default::default(),
        endpoint: None,
        dropped_relay: std::sync::atomic::AtomicU64::new(0),
        dropped_cmd: std::sync::atomic::AtomicU64::new(0),
    };

    let pkt = RelayedRtp {
        publisher_id: Arc::from("pub"),
        publisher_mid: Mid::from("0"),
        params: str0m::format::CodecConfig::new_with_defaults().params()[0],
        seq_no: SeqNo::default(),
        timestamp: 0,
        marker: false,
        ext_vals: ExtensionValues::default(),
        rid: None,
        ssrc: Ssrc::from(1),
        arrival: Instant::now(),
        payload: Arc::from(&[1u8, 2, 3][..]),
    };
    hub.relay(0, pkt, true);

    assert!(rr.try_recv().is_ok(), "中继包应送达目标 worker");
    // 唤醒管道应可读到字节
    use std::io::Read;
    let mut buf = [0u8; 8];
    let mut r = &wrx;
    assert!(r.read(&mut buf).unwrap_or(0) > 0, "应写入唤醒字节");
}

fn sample_pkt() -> RelayedRtp {
    RelayedRtp {
        publisher_id: Arc::from("pub"),
        publisher_mid: Mid::from("0"),
        params: str0m::format::CodecConfig::new_with_defaults().params()[0],
        seq_no: SeqNo::from(1),
        timestamp: 0,
        marker: false,
        ext_vals: ExtensionValues::default(),
        rid: None,
        ssrc: Ssrc::from(1),
        arrival: Instant::now(),
        payload: Arc::from(&[1u8, 2, 3][..]),
    }
}

/// 目标 worker 在远端进程时, `RelayHub::relay` 应经 UDP 发送, 对端可解码。
#[test]
fn relay_routes_remote_over_udp() {
    use crate::net::relay::{decode, Decoded, RelayEndpoint};

    let sender = Arc::new(RelayEndpoint::bind("127.0.0.1:0".parse().unwrap()).unwrap());
    let receiver = RelayEndpoint::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let receiver_addr = receiver.local_addr().unwrap();

    let mut peers = std::collections::HashMap::new();
    peers.insert(1u32, receiver_addr);
    let hub = RelayHub {
        relay_txs: vec![],
        wake_txs: vec![],
        cmd_txs: vec![],
        worker_offset: 0,
        local_workers: 1,
        peer_addrs: peers,
        endpoint: Some(sender),
        dropped_relay: std::sync::atomic::AtomicU64::new(0),
        dropped_cmd: std::sync::atomic::AtomicU64::new(0),
    };

    // 目标全局编号 1 属于另一台主机
    hub.relay(1, sample_pkt(), true);

    let mut buf = [0u8; 2048];
    let (n, _src) = receiver.recv_from(&mut buf).unwrap();
    match decode(&buf[..n], &mut crate::net::relay::ParamsCache::default()).unwrap() {
        Decoded::Rtp(d) => {
            assert_eq!(d.target_worker, 1);
            assert_eq!(&*d.pkt.payload, &[1u8, 2, 3]);
        }
        _ => panic!("expected RTP"),
    }
}

/// 解码后的跨主机控制消息应注入目标本地 worker 的命令通道。
#[test]
fn dispatch_control_injects_command() {
    use crate::net::relay::DecodedControl;
    let (ctx, crx) = unbounded::<WorkerCommand>();
    let hub = RelayHub {
        relay_txs: vec![],
        wake_txs: vec![],
        cmd_txs: vec![ctx],
        worker_offset: 0,
        local_workers: 1,
        peer_addrs: Default::default(),
        endpoint: None,
        dropped_relay: std::sync::atomic::AtomicU64::new(0),
        dropped_cmd: std::sync::atomic::AtomicU64::new(0),
    };
    dispatch_relay_control(
        &hub,
        DecodedControl::RequestKeyframe {
            target_worker: 0,
            publisher_id: "p".into(),
            publisher_mid: Mid::from("0"),
            rid: None,
        },
    );
    match crx.try_recv().unwrap() {
        WorkerCommand::RequestKeyframe { publisher_id, .. } => {
            assert_eq!(publisher_id, "p")
        }
        _ => panic!("expected RequestKeyframe"),
    }
}
