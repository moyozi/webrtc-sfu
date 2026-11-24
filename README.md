<div align="center">

# 🎥 webrtc-sfu

**A high-performance WebRTC SFU written in Rust.**

Thread-per-core · zero-copy RTP forwarding · batch syscalls · WHIP/WHEP · QUIC signaling

[![Rust](https://img.shields.io/badge/Rust-1.75%2B-orange)](#)
[![License](https://img.shields.io/badge/License-MIT-blue)](#license)
[![Platform](https://img.shields.io/badge/Platform-Linux%20x86__64-lightgrey)](#requirements)
[![WebRTC](https://img.shields.io/badge/WebRTC-str0m%20%7C%20WHIP%20%7C%20WHEP-green)](#)

[简体中文](./README.zh-CN.md) · [Architecture](./docs/design.md) · [Manual](./docs/manual.md)

</div>

---

**webrtc-sfu** is a Selective Forwarding Unit (SFU) for multi-party real-time media. It is built on
[`str0m`](https://github.com/algesten/str0m) (a Sans-I/O WebRTC stack) and a hand-rolled, fully
synchronous media path that avoids async overhead on the hot path. Every media worker is pinned to a
dedicated CPU core, batches socket I/O with `recvmmsg`/`sendmmsg` (with an optional `io_uring`
backend), and forwards RTP packets with shared `Arc<[u8]>` payloads — no re-assembly, no re-packetization,
no per-subscriber copy.

---

## ✨ Highlights

- **Thread-per-core, share-nothing workers** — each worker owns its UDP socket, `str0m` `Rtc`
  instances, routing table and timer wheel, and is pinned to its own CPU core.
- **Batch I/O everywhere** — `recvmmsg`/`sendmmsg` batch 64+ packets per syscall; `io_uring`
  receive backend is auto-detected and falls back to `epoll + recvmmsg` on older kernels.
- **Zero-copy packet-level forwarding** — `set_rtp_mode(true)`; RTP payloads are shared as
  `Arc<[u8]>` across subscribers and only PT/SSRC are rewritten.
- **Per-subscriber bandwidth adaptation** — TWCC/REMB-driven BWE with simulcast (RID) and SVC
  (VP8/VP9 temporal & spatial) layer selection plus per-subscriber RTP sequence rewriting.
- **One signaling protocol, two transports** — the exact same JSON protocol over **WebSocket** and
  **QUIC**; QUIC gives mobile clients connection migration across Wi-Fi/cellular handovers.
- **Standard ingest & egress** — **WHIP** (RFC 9725 subset) for pushing with FFmpeg/GStreamer/OBS,
  and **WHEP** for pulling, with trickle-ICE support.
- **Optional JWT auth** — HMAC (HS256/384/512) and asymmetric (RS/ES/EdDSA) verification with
  `sub`/`exp`/`room`/`role` claim constraints.
- **Scales out** — room-level sharding across workers and hosts, a gRPC control plane
  (`MediaControl`), a UDP cross-host relay plane, and a signaling dispatcher for multi-node signaling.
- **Batteries-included reference clients** — Web, Android, iOS, HarmonyOS NEXT and multiple desktop
  variants (see the **Reference clients** section below).

---

## 📊 Single-core capacity

Measured with the bundled load tools (`examples/load_publisher` / `load_subscriber`), one
**1.5 Mbps@30fps video + 64 kbps Opus audio** publisher, multi-process subscribers. Full
methodology and data: [`docs/capacity.md`](./docs/capacity.md).

| Subscribed media | Full-quality subscribers / core | Worker CPU |
| --- | ---: | ---: |
| Audio + video | **≈ 200** | ≈ 79% |
| Video only | **≈ 250** | ≈ 75% |
| Audio only | **≈ 500** | ≈ 70% |

- The bottleneck is per-subscriber per-packet forwarding (SRTP encryption + `write_rtp`): roughly
  **25k–40k subscriber-forwards/sec** per core.
- `io_uring` and `epoll` are within ~2% CPU under non-overloaded load; `io_uring` delivers ~28–40%
  more under overload.
- A single room spread across 2 workers reaches **≈ 300–320** full-quality subscribers (~1.3–1.5×),
  at the cost of ~60% extra total CPU because the share-nothing design re-processes each packet on
  the target worker.

---

## 🏗️ Architecture

```
                    ┌─────────────────────────────────────────────┐
  Browser / App ───►│  Signaling (Tokio, async)                   │
   ws / quic / http │  WS :5174 · QUIC :5173 · WHIP :5180 · WHEP  │
                    │  rooms · SDP · JWT · signaling dispatcher   │
                    └──────────────────────┬──────────────────────┘
                                           │ WorkerCommand
                                           │ (channel / gRPC)
        ┌──────────────────────────────────┼──────────────────────────────────┐
        ▼                                  ▼                                  ▼
  ┌───────────┐                      ┌───────────┐                      ┌───────────┐
  │ Worker 0  │                      │ Worker 1  │        ...           │ Worker N  │
  │ CPU 0     │                      │ CPU 1     │                      │ CPU N     │
  │ UDP 5175  │◄── RelayHub ────────►│ UDP 5176  │◄────────────────────►│ UDP 5175+N│
  │ recvmmsg  │                      │ recvmmsg  │                      │ recvmmsg  │
  │ sendmmsg  │                      │ sendmmsg  │                      │ sendmmsg  │
  └───────────┘                      └───────────┘                      └───────────┘
```

Rooms are pinned to a worker by default (`room_id` hash). A single room can be explicitly spread
across workers/hosts with `SFU_ROOM_WORKERS`, with media relayed through the `RelayHub` plane.
See [`docs/design.md`](./docs/design.md) for the full design.

---

## 🚀 Quick start

### Requirements

- **Linux** (uses `recvmmsg`/`sendmmsg`/`epoll`/`sched_setaffinity`; `io_uring` optional, kernel 5.11+)
- **Rust 1.75+**
- **protoc** (to compile `proto/sfu.proto`) — e.g. `apt install protobuf-compiler` / `dnf install protobuf-compiler`

### Build & run

```bash
git clone <repo-url> && cd webrtc-sfu

# Optional but recommended before high fan-out deployments:
sudo bash scripts/sysctl-sfu.sh

cargo build --release

# SFU_ANNOUNCE_IP must be the IP reachable by your clients (written into SDP/ICE candidates)
SFU_ANNOUNCE_IP=192.168.1.10 RUST_LOG=info ./target/release/sfu-server
```

Default ports:

| Service | Protocol | Default port |
| --- | --- | ---: |
| WebSocket signaling | TCP | 5174 |
| QUIC signaling | UDP | 5173 |
| Media (RTP/RTCP/STUN/DTLS) | UDP | 5175 … 5175+N-1 |
| WHIP ingest (optional, `SFU_WHIP_ADDR`) | TCP | 5180 |
| WHEP egress (optional, `SFU_WHEP_ADDR`) | TCP | 5181 |

### Push a stream with FFmpeg (WHIP)

```bash
ffmpeg -re -f lavfi -i testsrc2=size=1280x720:rate=30 -f lavfi -i sine=frequency=440 \
  -c:v libopenh264 -b:v 2000k -g 60 -pix_fmt yuv420p \
  -c:a libopus -b:a 96k -ar 48000 -ac 2 \
  -f whip "http://<SFU_IP>:5180/whip?room=demo"
```

Then open the web client, join `room=demo` and subscribe to the FFmpeg publisher. GStreamer and OBS
examples, WHEP playback and the full signaling protocol are in [`docs/manual.md`](./docs/manual.md).

---

## 🧩 Reference clients

The repo ships several interoperable clients that all speak the same signaling protocol:

| Client | Directory | Stack |
| --- | --- | --- |
| Web | [`RoomChatWeb`](./RoomChatWeb) | Solid + TypeScript |
| Android | [`RoomChatAndroid`](./RoomChatAndroid) | Kotlin + Jetpack Compose |
| iOS | [`RoomChatApple`](./RoomChatApple) | Swift |
| HarmonyOS NEXT | [`RoomChatHarmony`](./RoomChatHarmony) | ArkTS |
| Desktop (GPUI) | [`RoomChatDesktopGpui`](./RoomChatDesktopGpui) | GPUI + reactor-webrtc |
| Desktop (Slint + libwebrtc) | [`RoomChatDesktopSlint`](./RoomChatDesktopSlint) | Slint + reactor-webrtc |
| Desktop (Slint + str0m) | [`RoomChatDesktopSlintStr0m`](./RoomChatDesktopSlintStr0m) | Slint + str0m + ffmpeg-next |
| Desktop (Slint + GStreamer) | [`RoomChatDesktopGStreamer`](./RoomChatDesktopGStreamer) | Slint + GStreamer webrtcbin |

---

## ⚙️ Configuration

All configuration is via environment variables. The essentials:

| Variable | Default | Description |
| --- | --- | --- |
| `SFU_ANNOUNCE_IP` | auto-detect | Public/reachable IP written into SDP & ICE candidates |
| `SFU_WORKERS` | `1` | Worker thread count (one media port each); `0` = signaling-only node |
| `SFU_CPU_BASE` | `0` | Pin worker `i` to CPU `cpu_base + i` |
| `SFU_ROOM_WORKERS` | — | Room → global worker list, e.g. `load=0,1,2,3` |
| `SFU_WS_ADDR` | `0.0.0.0:5174` | WebSocket signaling bind |
| `SFU_QUIC_ADDR` | `0.0.0.0:5173` | QUIC signaling bind |
| `SFU_MEDIA_ADDR` | `0.0.0.0:5175` | Media UDP base address |
| `SFU_WHIP_ADDR` / `SFU_WHEP_ADDR` | — | Enable WHIP / WHEP HTTP endpoints |
| `SFU_IO_URING` | `auto` | I/O backend: `auto` / `on` / `off` (epoll) |
| `SFU_JWT_SECRET` | — | JWT HMAC secret; enables auth when set |
| `SFU_JWT_PUBLIC_KEY_FILE` | — | JWT asymmetric public key (RS/ES/EdDSA) |
| `SFU_SIGNALING` | `on` | `off` for worker-only hosts |
| `SFU_CONTROL_LISTEN` / `SFU_CONTROL_PEERS` | — | gRPC control plane (cross-host) |
| `SFU_RELAY_LISTEN` / `SFU_RELAY_PEERS` | — | UDP cross-host media relay |
| `SFU_DISPATCHER_ADDR` | — | Run the signaling dispatcher (multi signaling nodes) |

The complete reference, deployment topologies (single process, multi-worker, multi-host,
signaling-only nodes, dispatcher) and troubleshooting are in
[`docs/manual.md`](./docs/manual.md) and [`docs/deployment.md`](./docs/deployment.md).

---

## 📚 Documentation

| Document | Content |
| --- | --- |
| [`docs/manual.md`](./docs/manual.md) | Usage: build, config, signaling protocol, WHIP/WHEP, load tools, ops |
| [`docs/deployment.md`](./docs/deployment.md) | Step-by-step deployment topologies & every env var |
| [`docs/design.md`](./docs/design.md) | Full architecture & implementation notes |
| [`docs/capacity.md`](./docs/capacity.md) | Single-core capacity measurements & io_uring/epoll comparison |
| [`docs/multi-worker-relay.md`](./docs/multi-worker-relay.md) | Cross-worker / cross-host relay design |
| [`docs/signaling-dispatcher.md`](./docs/signaling-dispatcher.md) | Multi-node signaling allocation (consistent hashing) |

---

## 🗂️ Project layout

```
webrtc-sfu/
├── proto/sfu.proto            # gRPC MediaControl control-plane definition
├── src/
│   ├── main.rs                # Entry: workers + signaling + gRPC control plane
│   ├── config.rs              # All env-var configuration
│   ├── worker/                # Per-core event loop & cross-worker relay
│   ├── sfu/                   # Router, participant, simulcast/SVC layer selection
│   ├── net/                   # recvmmsg/sendmmsg UDP, epoll, io_uring, relay codec
│   ├── signaling/             # WebSocket / QUIC / WHIP / WHEP / JWT
│   ├── grpc/                  # Signaling → media gRPC control plane
│   ├── cluster/               # Local channel / remote gRPC routing
│   └── dispatcher/            # Signaling dispatcher (room → signaling node)
├── examples/                  # load_publisher, load_subscriber, whep_client, quic client
├── scripts/                   # sysctl tuning, JWT helpers
└── deploy/                    # systemd unit & env examples
```

---

## 🔐 Security notes

- The SFU is **ICE-Lite + host candidates only** — no TURN/STUN server; deploy behind a firewall
  or security group.
- WebSocket is plaintext `ws://` and QUIC uses a self-signed certificate by default; terminate TLS
  in front with nginx/LB for production.
- Enable JWT auth (`SFU_JWT_SECRET` / `SFU_JWT_PUBLIC_KEY*`) for authenticated access control.

---

## 📄 License

MIT — see [`LICENSE`](./LICENSE).

---

## 🙌 Acknowledgements

- [`str0m`](https://github.com/algesten/str0m) — the Sans-I/O WebRTC stack that powers the media path.
- [`quinn`](https://github.com/quinn-rs/quinn) — QUIC transport for signaling.
- [`nix`](https://github.com/nix-rust/nix) — low-level Linux syscall bindings.
