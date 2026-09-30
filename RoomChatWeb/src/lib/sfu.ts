import { SignalingClient } from "./signaling";
import type { ServerMessage, LogEntry, RemoteParticipant } from "./types";

/**
 * SFU 客户端 — 单 WebSocket + 多 RTCPeerConnection (信令复用)
 *
 * 架构:
 *   ┌─────────────┐        一条 WebSocket (所有 PC 复用, 按 pc_id 区分)      ┌───────────┐
 *   │  浏览器 A    │ ────────────────────────────────────────────────────→  │  SFU 服务  │
 *   │             │   pc_id="pub"        → 发布 PC (sendonly)                │  (str0m)  │
 *   │             │   pc_id="sub-bob"    → 订阅 bob 的 PC (recvonly)        │           │
 *   │             │   pc_id="sub-carol"  → 订阅 carol 的 PC (recvonly)      │           │
 *   └─────────────┘                                                        └───────────┘
 *
 * 一个用户无论订阅多少人, 都只维持 1 条 WebSocket 连接, 节省大量 TCP/TLS 资源。
 * 服务端为每个 pc_id 创建一个独立的 str0m Rtc 实例 (参与者), 但共用同一条信令通道。
 */

const PUB_PC_ID = "pub";

interface PcEntry {
  pc: RTCPeerConnection;
  role: "publisher" | "subscriber";
  /** subscriber: 正在订阅的远端参与者 id */
  publisherId?: string;
}

// 回调类型
export interface SfuCallbacks {
  onLog: (msg: string, level?: LogEntry["level"]) => void;
  onParticipantJoined: (id: string) => void;
  onParticipantLeft: (id: string) => void;
  onSubscriptionUpdate: (publisherId: string, participant: Partial<RemoteParticipant>) => void;
  onConnectionStateChange: (state: string) => void;
}

export class SfuClient {
  // 身份
  private participantId = "";
  private roomId = "";

  // 单条信令连接 (所有 PC 复用)
  private sig: SignalingClient | null = null;

  // pc_id → PC 元数据
  private pcs = new Map<string, PcEntry>();
  // 远端 publisherId → 订阅它的 pc_id
  private subPcByPublisher = new Map<string, string>();

  private localStream: MediaStream | null = null;

  private cb: SfuCallbacks;

  constructor(callbacks: SfuCallbacks) {
    this.cb = callbacks;
  }

  // ── 发布: 加入房间 ─────────────────────────────────────────

  async join(serverUrl: string, roomId: string, participantId: string): Promise<void> {
    this.participantId = participantId;
    this.roomId = roomId;

    // 1. 获取本地媒体
    this.cb.onLog("正在获取摄像头和麦克风...");
    this.localStream = await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: { width: 640, height: 480 },
    });
    this.cb.onLog("本地媒体流已就绪", "success");

    // 2. 连接信令服务器 (整个会话只此一条 WebSocket)
    this.sig = new SignalingClient(serverUrl);
    await this.sig.connect();
    this.sig.onMessage = (msg) => this.onMessage(msg);
    this.sig.onClose = () => {
      this.cb.onLog("信令连接断开", "warn");
      this.cb.onConnectionStateChange("disconnected");
    };

    // 3. 创建发布 PC (sendonly)
    const pc = new RTCPeerConnection({ iceServers: [] });
    for (const track of this.localStream.getTracks()) {
      pc.addTransceiver(track, { direction: "sendonly", streams: [this.localStream] });
    }
    this.registerPc(PUB_PC_ID, pc, "publisher");

    pc.onicecandidate = (e) => {
      if (e.candidate) this.sendIce(PUB_PC_ID, e.candidate);
    };
    pc.onconnectionstatechange = () => {
      this.cb.onLog(`发布连接状态: ${pc.connectionState}`);
      this.cb.onConnectionStateChange(pc.connectionState);
    };

    // 4. 创建 offer, 等待 ICE 收集完成
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    await this.waitIceComplete(pc);

    // 5. 发送 join (发布连接, role=publisher)
    this.sig.send({
      type: "join",
      pc_id: PUB_PC_ID,
      room_id: roomId,
      participant_id: participantId,
      sdp: pc.localDescription!.sdp,
      role: "publisher",
    });
    this.cb.onLog(`正在加入房间 ${roomId}...`);
  }

  /** 获取本地媒体流 (用于 UI 显示) */
  getLocalStream(): MediaStream | null {
    return this.localStream;
  }

  // ── 订阅: 接收远端参与者媒体 (复用同一条 WebSocket) ──────────

  async subscribe(publisherId: string): Promise<void> {
    if (this.subPcByPublisher.has(publisherId)) {
      this.cb.onLog(`已订阅 ${publisherId}`, "warn");
      return;
    }
    if (!this.sig) return;

    // 不订阅自己, 也不订阅"订阅连接" (xxx-recv-yyy 只是 recvonly 连接, 不是发布者)。
    if (publisherId === this.participantId || publisherId.includes("-recv-")) {
      this.cb.onLog(`忽略非法订阅目标: ${publisherId}`, "warn");
      return;
    }

    this.cb.onLog(`正在订阅 ${publisherId}...`);
    this.cb.onSubscriptionUpdate(publisherId, { status: "connecting" });

    const pcId = `sub-${publisherId}`;

    // 1. 创建订阅 PC (recvonly)
    const pc = new RTCPeerConnection({ iceServers: [] });
    // recvonly 音频 (mid=0) + recvonly 视频 (mid=1)
    pc.addTransceiver("audio", { direction: "recvonly" });
    pc.addTransceiver("video", { direction: "recvonly" });

    // 接收远端轨道
    const stream = new MediaStream();
    pc.ontrack = (e) => {
      stream.addTrack(e.track);
      this.cb.onSubscriptionUpdate(publisherId, { stream });
      this.cb.onLog(`收到 ${publisherId} 的 ${e.track.kind} 轨道`, "success");
    };

    pc.onicecandidate = (e) => {
      if (e.candidate) this.sendIce(pcId, e.candidate);
    };
    pc.onconnectionstatechange = () => {
      this.cb.onLog(`订阅 ${publisherId} 连接状态: ${pc.connectionState}`);
    };

    this.registerPc(pcId, pc, "subscriber", publisherId);
    this.subPcByPublisher.set(publisherId, pcId);

    // 2. 创建 offer, 通过同一条 WebSocket 发送 join (role=subscriber)
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    await this.waitIceComplete(pc);

    this.sig.send({
      type: "join",
      pc_id: pcId,
      room_id: this.roomId,
      participant_id: `${this.participantId}-recv-${publisherId}`,
      sdp: pc.localDescription!.sdp,
      role: "subscriber",
    });
  }

  /** 取消订阅 */
  unsubscribe(publisherId: string): void {
    const pcId = this.subPcByPublisher.get(publisherId);
    if (!pcId) return;
    const entry = this.pcs.get(pcId);

    // 通知服务器取消订阅 (音频 mid=0 + 视频 mid=1)
    this.sig?.send({ type: "unsubscribe", pc_id: pcId, publisher_id: publisherId, track_mid: "0" });
    this.sig?.send({ type: "unsubscribe", pc_id: pcId, publisher_id: publisherId, track_mid: "1" });
    this.sig?.send({ type: "leave", pc_id: pcId });

    entry?.pc.close();
    this.pcs.delete(pcId);
    this.subPcByPublisher.delete(publisherId);
    this.cb.onLog(`已取消订阅 ${publisherId}`);
    this.cb.onSubscriptionUpdate(publisherId, { status: "idle", stream: null });
  }

  // ── 离开 ──────────────────────────────────────────────────

  leave(): void {
    // 通知服务器关闭所有 PC 会话
    if (this.sig?.isConnected) {
      for (const [pcId] of this.pcs) {
        this.sig.send({ type: "leave", pc_id: pcId });
      }
    }

    // 关闭所有 PC
    for (const [, entry] of this.pcs) {
      entry.pc.close();
    }
    this.pcs.clear();
    this.subPcByPublisher.clear();

    // 停止本地轨道
    if (this.localStream) {
      this.localStream.getTracks().forEach((t) => t.stop());
      this.localStream = null;
    }

    // 关闭信令
    this.sig?.close();
    this.sig = null;
  }

  // ── 媒体控制 ──────────────────────────────────────────────

  toggleAudio(enabled: boolean): void {
    this.localStream?.getAudioTracks().forEach((t) => (t.enabled = enabled));
  }

  toggleVideo(enabled: boolean): void {
    this.localStream?.getVideoTracks().forEach((t) => (t.enabled = enabled));
  }

  // ── 信令消息处理 (统一入口, 按 pc_id 分发) ─────────────────

  private onMessage(msg: ServerMessage): void {
    switch (msg.type) {
      case "sdp_answer": {
        const entry = this.pcs.get(msg.pc_id);
        if (!entry) {
          this.cb.onLog(`收到未知 pc_id 的 sdp_answer: ${msg.pc_id}`, "warn");
          return;
        }
        const { pc, role, publisherId } = entry;

        pc.setRemoteDescription({ type: "answer", sdp: msg.sdp })
          .then(() => {
            // 服务器的是 BUNDLE 单端口候选: 发布连接加到 mid 0,
            // 订阅连接两个 mid 都尝试加入 (对端 mid 数量不同时忽略失败)。
            const targets = role === "subscriber"
              ? [{ mid: "0", idx: 0 }, { mid: "1", idx: 1 }]
              : [{ mid: "0", idx: 0 }];
            for (const c of msg.ice_candidates) {
              for (const t of targets) {
                pc.addIceCandidate({ candidate: c, sdpMid: t.mid, sdpMLineIndex: t.idx })
                  .catch(() => {});
              }
            }

            if (role === "subscriber" && publisherId) {
              // SDP 完成后发送 subscribe (音频 mid=0 + 视频 mid=1), 复用同一 WebSocket
              this.sig?.send({ type: "subscribe", pc_id: msg.pc_id, publisher_id: publisherId, track_mid: "0" });
              this.sig?.send({ type: "subscribe", pc_id: msg.pc_id, publisher_id: publisherId, track_mid: "1" });
              this.cb.onLog(`订阅 ${publisherId} SDP 完成, 已发送 subscribe`, "success");
            } else {
              this.cb.onLog("SDP 协商完成, 已添加服务器候选", "success");
            }
          })
          .catch((e) => this.cb.onLog(`SDP 设置失败: ${e}`, "error"));
        break;
      }

      case "ice_candidate": {
        const entry = this.pcs.get(msg.pc_id);
        entry?.pc
          .addIceCandidate({ candidate: msg.candidate, sdpMid: msg.sdp_mid ?? "0", sdpMLineIndex: 0 })
          .catch(() => {});
        break;
      }

      case "subscribed": {
        const entry = this.pcs.get(msg.pc_id);
        const pid = entry?.publisherId ?? msg.publisher_id;
        this.cb.onSubscriptionUpdate(pid, { status: "subscribed" });
        this.cb.onLog(`订阅确认: ${pid}`, "success");
        break;
      }

      case "participant_joined":
        // 防御性过滤: 忽略自己, 以及自己的订阅连接 (participantId-recv-*).
        if (
          msg.participant_id === this.participantId ||
          msg.participant_id.startsWith(`${this.participantId}-recv-`)
        ) {
          return;
        }
        this.cb.onLog(`参与者加入: ${msg.participant_id}`, "info");
        this.cb.onParticipantJoined(msg.participant_id);
        break;

      case "participant_left":
        this.cb.onLog(`参与者离开: ${msg.participant_id}`, "warn");
        this.unsubscribe(msg.participant_id);
        this.cb.onParticipantLeft(msg.participant_id);
        break;

      case "error":
        this.cb.onLog(
          msg.pc_id ? `服务器错误 [${msg.pc_id}]: ${msg.error}` : `服务器错误: ${msg.error}`,
          "error"
        );
        break;
    }
  }

  // ── 工具 ──────────────────────────────────────────────────

  private registerPc(
    pcId: string,
    pc: RTCPeerConnection,
    role: "publisher" | "subscriber",
    publisherId?: string
  ): void {
    this.pcs.set(pcId, { pc, role, publisherId });
  }

  private sendIce(pcId: string, cand: RTCIceCandidate): void {
    this.sig?.send({
      type: "ice_candidate",
      pc_id: pcId,
      candidate: cand.candidate,
      sdp_mid: cand.sdpMid,
      sdp_mline_index: cand.sdpMLineIndex,
    });
  }

  /** 等待 ICE 收集完成 */
  private waitIceComplete(pc: RTCPeerConnection): Promise<void> {
    if (pc.iceGatheringState === "complete") return Promise.resolve();
    return new Promise((resolve) => {
      const check = () => {
        if (pc.iceGatheringState === "complete") {
          pc.removeEventListener("icegatheringstatechange", check);
          resolve();
        }
      };
      pc.addEventListener("icegatheringstatechange", check);
      // 超时保护 (2s)
      setTimeout(() => { pc.removeEventListener("icegatheringstatechange", check); resolve(); }, 2000);
    });
  }
}
