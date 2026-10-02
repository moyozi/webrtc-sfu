# 单核容量参考（Simulcast/SVC 转发与 fan-out）

> 本文给出**单 Worker（单核）在「一路推流、多路拉流」场景下的实测容量**，
> 以及 io_uring / epoll 两种 I/O 后端、批量参数的对比。
> 数值来自本仓库的压测工具（`examples/load_publisher`、`load_subscriber`），
> 用于**相对比较与容量规划参考**，非厂商标称。

---

## 1. 关键结论（TL;DR）

在「1 路 1.5 Mbps@30fps 视频 + Opus 64 kbps 音频」推流、订阅端多进程并行的
条件下，**单个 Worker 线程（一个物理核）**：

| 订阅内容 | 满质量订阅者数 | 此时 Worker CPU | 每订阅者 | 折算单向媒体流 |
|---|---:|---:|---|---:|
| **音 + 视频**（默认） | **≈ 200** | ≈ 79% | ~1.56 Mbps，~200 pps | 400 条（200 音 + 200 视） |
| **只拉视频** | **≈ 250** | ≈ 75% | ~1.5 Mbps，~150 pps | 250 条 |
| **只拉音频** | **≈ 500** | ≈ 70% | 64 kbps，50 pps | 500 条 |

- 超过上述数量后开始丢包（满质量 → 部分丢包），不是硬断崖，而是随订阅者增加线性劣化。
- 瓶颈是**「每订阅者·每包的转发次数」**（SRTP 加密 + `write_rtp` + 发送），
  单核大约能做 **2.5–4 万次订阅者转发/秒**（70–79% CPU 时）。
- **两种 I/O 后端（io_uring / epoll）在满质量区间 CPU 基本一致**，
  io_uring 的优势只在**过载**时体现（同样 CPU 下投递更多、丢包更少）。
- `SFU_SEND_BATCH` / `SFU_SEND_BACKLOG` 只影响**突发吸收**，不改变稳态上限。
- **单房间跨 2 worker**：满质量 ≈ 300–320 路（约 **1.3–1.5×** 单 worker），
  因跨 worker 中继有 ~25–30% 额外开销，详见 §5。

---

## 2. 测试环境与方法

| 项 | 值 |
|---|---|
| 机器 | 12 核 x86_64，Linux 7.2（io_uring 可用），31 GB 内存 |
| 构建 | `cargo build --release`（LTO + 单 codegen-unit） |
| SFU | `SFU_WORKERS=1`，Worker 线程自 pin 到 **CPU0** |
| 发布者 | `load_publisher`，pin 到 CPU1；1.5 Mbps VP8@30fps + Opus 64 kbps |
| 订阅者 | `load_subscriber` **多进程**并行，pin 到 CPU2–11；每个订阅者拉同一发布者 |
| 网络 | 回环（127.0.0.1，无网卡） |
| 指标 | Worker 线程 `utime+stime`（`/proc/<pid>/task/*/stat`）；订阅端聚合「收流」Mbps |
| 采样窗口 | 预热后稳定运行 10s |

> 为什么订阅端要**多进程**：单个 `load_subscriber` 进程接收聚合速率约
> **35–40k pps** 就会成为瓶颈（客户端侧），无法把 SFU 压满。多进程可分散接收负载。
> 用 `--id-prefix` 避免多进程间 `participant_id` 冲突。

复现命令示例（单机自测）：

```bash
# 1) SFU（单 worker）
SFU_ANNOUNCE_IP=127.0.0.1 SFU_WORKERS=1 SFU_IO_URING=auto \
  ./target/release/sfu-server

# 2) 发布者（记下它打印的 [mids] load-0 <audio_mid> <video_mid>）
./target/release/examples/load_publisher --room load --count 1 --duration 60

# 3) 订阅者（可开多个进程, 各自 --id-prefix; --tracks 选 both/audio/video）
./target/release/examples/load_subscriber --room load --count 50 \
  --id-prefix a --tracks both --publisher load-0:<audio_mid>:<video_mid>
```

---

## 3. 实测数据点

### 3.1 音 + 视频（默认，每路 ~1.56 Mbps）

| 订阅数 N | Worker CPU | 实收 | 期望 | 满质量? |
|---:|---:|---:|---:|:--:|
| 100 | 55.3% | 177 Mbps | 156 | ✅ |
| **200** | **79.1%** | **331 Mbps** | 313 | ✅ |
| 300 | 78.5% | 242 Mbps | 469 | ❌ |
| 400 | 76.1% | 269 Mbps | 626 | ❌ |
| 600 | 74.2% | 302 Mbps | 938 | ❌ |

### 3.2 只拉视频（每路 ~1.5 Mbps）

| 订阅数 N | Worker CPU | 实收 | 期望 | 满质量? |
|---:|---:|---:|---:|:--:|
| 200 | 63.3% | 301 Mbps | 300 | ✅ |
| **250** | **75.3%** | **374 Mbps** | 375 | ✅ |
| 300 | 77.7% | 319 Mbps | 450 | ❌ |
| 400 | 76.1% | 382 Mbps | 600 | ❌ |

### 3.3 只拉音频（每路 64 kbps）

| 订阅数 N | Worker CPU | 实收 | 期望 | 满质量? |
|---:|---:|---:|---:|:--:|
| **500** | 70.3% | 32 Mbps | 32 | ✅ |
| 750 | 78.6% | 41 Mbps | 48 | ≈85% |
| 1000 | 67.7% | 9 Mbps | 64 | 测量不可信* |

\* 音频 N=1000 的骤降伴随连接建立风暴，且 SFU CPU 仅 68%、调大 backlog 无效，
判定为**压测客户端/连接建立侧**的问题，不代表 SFU 稳态上限；音频容量以 ~500 为保守值。

---

## 4. io_uring vs epoll

同一负载下的对比（音+视频）：

| 场景 | io_uring | epoll |
|---|---|---|
| N=100 | 62.3% CPU / 159 Mbps（满） | 61.4% / 159 Mbps（满） |
| N=200 | 78.7% / ~338 Mbps（满） | 76.6% / ~325 Mbps（满） |
| **N=400（过载）** | **76.1% / ~269 Mbps** | **78.3% / ~210 Mbps** |

- **满质量区间**：两者 CPU 差 ≤2 个百分点，收后端**不是瓶颈**。
- **过载区间**：io_uring 投递量高 **约 28–40%**，且 CPU 略低——即丢包更少、更稳。
- 结论：**用 io_uring `auto`**（支持则用，不支持自动回退），低负载零损失、高负载更稳。

---

## 5. 多 Worker（单房间跨 worker）

当**单个房间**的订阅者超过单核容量时，用 `SFU_ROOM_WORKERS` 让该房间跨 worker
（例如 `SFU_WORKERS=2 SFU_ROOM_WORKERS="load=0,1"`）。发布者与订阅者按参与者
顺序轮转到 worker 0/1；发布者所在 worker 把每个 RTP 包**中继一份**给另一 worker
（见 [`multi-worker-relay.md`](multi-worker-relay.md)），另一 worker 再转发给自己的
本地订阅者。

实测（1 路 1.5 Mbps@30fps 音+视频，单房间）：

| Worker 数 | 满质量订阅者 | 各 worker CPU | 合计 CPU |
|---:|---:|---|---:|
| 1 | ≈ 200–250 | 81%（N=250） | 81% |
| 2 | **≈ 300–320** | 77–80% each | ~155–160% |

关键数据点（音+视频）：

| 配置 | N | 各 worker CPU | 实收 / 期望 | 满质量? |
|---|---:|---|---:|:--:|
| 1 worker | 250 | 81.2% | 366 / 391 | ≈94% |
| 2 worker | 250 | 68.4% + 66.9% | 395 / 391 | ✅ |
| 2 worker | 300 | 79.7% + 80.2% | 493 / 469 | ✅ |
| 2 worker | 320 | 77.0% + 77.1% | 473 / 500 | ≈95% |
| 2 worker | 350 | 78.6% + 78.5% | 455 / 547 | 83% |
| 2 worker | 400 | 77.2% + 78.1% | 416 / 626 | 66% |

- 2 worker 仅提升约 **1.3–1.5×**，而非 2×。
- 原因：**跨 worker 中继开销**。同一个发布者的每个包在发布者 worker 上要
  「本地转发 + 编码中继」，在另一 worker 上要「解码 + 转发」；总转发量不变
  但多了中继处理。实测单房间 N=100 时，1 worker 总 CPU 55% → 2 worker 71%
  （**+28%**）。
- 两个 worker 的 CPU 基本均衡（发布者所在 worker 略高）。
- 每个 worker 的 CPU 仍平台化在 ~79%（与单 worker 一致）。

> **结论**：优先让每个房间落在**单 worker**（靠 `SFU_WORKERS` 把不同房间分散到不同核）；
> 只有单个房间确实需要更大容量时，才用 `SFU_ROOM_WORKERS` 将它跨 worker，
> 并接受 ~25–30% 的中继开销。

---

## 6. 批量参数的影响

| 参数 | 默认 | 作用 | 对稳态 fan-out 上限 |
|---|---|---|---|
| `SFU_BATCH_SIZE` | 64 | 批量收包大小（recvmmsg / io_uring 槽位） | 无明显影响 |
| `SFU_SEND_BATCH` | 128 | 每次 `sendmmsg` 最大包数 | 无明显影响 |
| `SFU_SEND_BACKLOG` | 8192 | 发送队列积压上限（超限丢最旧包） | 无明显影响 |

N=400（音+视频）对照：默认 **269 Mbps** vs `send_batch=256 + backlog=65536` **263 Mbps**；
音频 N=1000 把 backlog 调到 65536/262144 也仍 ~10 Mbps。

> 这三个参数**适合吸收突发**（大量订阅者同时加入、关键帧洪峰时的瞬时堆积），
> 而不是提升稳态容量。稳态瓶颈是单核的转发计算量。

---

## 7. 调优建议

1. **按房间加 Worker**：单核约 200 路（1.5 Mbps 音+视频）；要更多就用
   `SFU_WORKERS` + `SFU_ROOM_WORKERS` 把不同房间分到不同核，媒体在核内转发、零中继。
2. **单房间超 200 路**：`SFU_ROOM_WORKERS` 让同一房间跨 worker/主机分布
   （跨机媒体走 UDP relay，见 [`multi-worker-relay.md`](multi-worker-relay.md)）。
3. **I/O 后端**：`SFU_IO_URING=auto`。
4. **突发场景**再调 `SFU_SEND_BACKLOG` / `SFU_SEND_BATCH`。

---

## 8. 注意事项

- 数据来自**回环 + 压测进程与 SFU 同机**，绝对吞吐会低于真实网卡/多机部署；
  用于**相对比较**（不同订阅范围、两种 I/O 方式、参数调整）更可靠。
- 未做 `perf` / 火焰图分析；「瓶颈是转发计算量」是基于 CPU 与投递量的黑盒推断。
- `load_subscriber` 单进程聚合接收上限约 35–40k pps，做高 fan-out 压测时务必多进程。
- 音频 N≥1000 的测量受连接建立影响，暂不作为容量结论。
