import { SignalingClient } from "./signaling";
import type { ServerMessage, LogEntry, RemoteParticipant } from "./types";

/**
 * SFU 客户端 — 管理发布与订阅
 *
 * 架构:
 *   ┌─────────────┐     主 WebSocket + PC (sendonly)     ┌───────────┐
 *   │  浏览器 A    │ ──────────────────────────────────→  │  SFU 服务  │
 *   │  (发布者)    │                                       │  (str0m)  │
 *   │             │     订阅 WebSocket + PC (recvonly)    │           │
 *   │             │ ←──────────────────────────────────  │           │
 *   └─────────────┘                                       └───────────┘
 *
 * 发布流: getUserMedia → sendonly PC → join → 服务器接收
 * 订阅流: 为每个远端参与者创建独立 WS + PC → join(recvonly) → subscribe → 服务器推送
 */

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

  // 发布 (主连接)
  private mainSig: SignalingClient | null = null;
  private publishPC: RTCPeerConnection | null = null;
  private localStream: MediaStream | null = null;

  // 订阅 (每个远端参与者一个独立连接)
  // key = publisherId
  private subs = new Map<string, {
    signaling: SignalingClient;
    pc: RTCPeerConnection;
  }>();

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

    // 2. 连接信令服务器
    this.mainSig = new SignalingClient(serverUrl);
    await this.mainSig.connect();
    this.mainSig.onMessage = (msg) => this.onMainMessage(msg);
    this.mainSig.onClose = () => {
      this.cb.onLog("信令连接断开", "warn");
      this.cb.onConnectionStateChange("disconnected");
    };

    // 3. 创建发布 PC (sendonly)
    this.publishPC = new RTCPeerConnection({ iceServers: [] });
    for (const track of this.localStream.getTracks()) {
      this.publishPC.addTransceiver(track, { direction: "sendonly", streams: [this.localStream] });
    }

    this.publishPC.onicecandidate = (e) => {
      if (e.candidate) {
        this.mainSig!.send({
          type: "ice_candidate",
          candidate: e.candidate.candidate,
          sdp_mid: e.candidate.sdpMid,
          sdp_mline_index: e.candidate.sdpMLineIndex,
        });
      }
    };

    this.publishPC.onconnectionstatechange = () => {
      const state = this.publishPC!.connectionState;
      this.cb.onLog(`发布连接状态: ${state}`);
      this.cb.onConnectionStateChange(state);
    };

    // 4. 创建 offer, 等待 ICE 收集完成
    const offer = await this.publishPC.createOffer();
    await this.publishPC.setLocalDescription(offer);
    await this.waitIceComplete(this.publishPC);

    // 5. 发送 join
    this.mainSig.send({
      type: "join",
      room_id: roomId,
      participant_id: participantId,
      sdp: this.publishPC.localDescription!.sdp,
    });
    this.cb.onLog(`正在加入房间 ${roomId}...`);
  }

  /** 获取本地媒体流 (用于 UI 显示) */
  getLocalStream(): MediaStream | null {
    return this.localStream;
  }

  // ── 订阅: 接收远端参与者媒体 ──────────────────────────────

  async subscribe(publisherId: string): Promise<void> {
    if (this.subs.has(publisherId)) {
      this.cb.onLog(`已订阅 ${publisherId}`, "warn");
      return;
    }
    if (!this.mainSig) return;

    this.cb.onLog(`正在订阅 ${publisherId}...`);
    this.cb.onSubscriptionUpdate(publisherId, { status: "connecting" });

    const subId = `${this.participantId}-recv-${publisherId}`;
    const serverUrl = this.mainSig.url;

    // 1. 创建独立信令连接
    const signaling = new SignalingClient(serverUrl);
    await signaling.connect();

    // 2. 创建订阅 PC (recvonly)
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
      if (e.candidate) {
        signaling.send({
          type: "ice_candidate",
          candidate: e.candidate.candidate,
          sdp_mid: e.candidate.sdpMid,
          sdp_mline_index: e.candidate.sdpMLineIndex,
        });
      }
    };

    pc.onconnectionstatechange = () => {
      this.cb.onLog(`订阅 ${publisherId} 连接状态: ${pc.connectionState}`);
    };

    this.subs.set(publisherId, { signaling, pc });

    // 3. 处理信令消息
    signaling.onMessage = (msg) => this.onSubMessage(msg, publisherId, pc);

    // 4. 创建 offer, 发送 join
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    await this.waitIceComplete(pc);

    signaling.send({
      type: "join",
      room_id: this.roomId,
      participant_id: subId,
      sdp: pc.localDescription!.sdp,
    });
  }

  /** 取消订阅 */
  unsubscribe(publisherId: string): void {
    const sub = this.subs.get(publisherId);
    if (!sub) return;

    // 通知服务器取消订阅
    sub.signaling.send({ type: "subscribe", publisher_id: publisherId, track_mid: "0" });
    sub.signaling.send({ type: "leave" });

    sub.pc.close();
    sub.signaling.close();
    this.subs.delete(publisherId);
    this.cb.onLog(`已取消订阅 ${publisherId}`);
    this.cb.onSubscriptionUpdate(publisherId, { status: "idle", stream: null });
  }

  // ── 离开 ──────────────────────────────────────────────────

  leave(): void {
    // 取消所有订阅
    for (const [pid] of this.subs) {
      this.unsubscribe(pid);
    }

    // 发送 leave
    if (this.mainSig?.isConnected) {
      this.mainSig.send({ type: "leave" });
    }

    // 关闭发布 PC
    if (this.publishPC) {
      this.publishPC.close();
      this.publishPC = null;
    }

    // 停止本地轨道
    if (this.localStream) {
      this.localStream.getTracks().forEach((t) => t.stop());
      this.localStream = null;
    }

    // 关闭信令
    this.mainSig?.close();
    this.mainSig = null;
  }

  // ── 媒体控制 ──────────────────────────────────────────────

  toggleAudio(enabled: boolean): void {
    this.localStream?.getAudioTracks().forEach((t) => (t.enabled = enabled));
  }

  toggleVideo(enabled: boolean): void {
    this.localStream?.getVideoTracks().forEach((t) => (t.enabled = enabled));
  }

  // ── 信令消息处理 ──────────────────────────────────────────

  /** 主连接消息 (发布) */
  private onMainMessage(msg: ServerMessage): void {
    switch (msg.type) {
      case "sdp_answer":
        this.publishPC?.setRemoteDescription({ type: "answer", sdp: msg.sdp })
          .then(() => {
            // 添加服务器 ICE 候选
            for (const c of msg.ice_candidates) {
              this.publishPC?.addIceCandidate({
                candidate: c,
                sdpMid: "0",
                sdpMLineIndex: 0,
              });
            }
            this.cb.onLog("SDP 协商完成, 已添加服务器候选", "success");
          })
          .catch((e) => this.cb.onLog(`SDP 设置失败: ${e}`, "error"));
        break;

      case "ice_candidate":
        this.publishPC?.addIceCandidate({
          candidate: msg.candidate,
          sdpMid: msg.sdp_mid ?? "0",
          sdpMLineIndex: 0,
        });
        break;

      case "participant_joined":
        this.cb.onLog(`参与者加入: ${msg.participant_id}`, "info");
        this.cb.onParticipantJoined(msg.participant_id);
        break;

      case "participant_left":
        this.cb.onLog(`参与者离开: ${msg.participant_id}`, "warn");
        this.unsubscribe(msg.participant_id);
        this.cb.onParticipantLeft(msg.participant_id);
        break;

      case "error":
        this.cb.onLog(`服务器错误: ${msg.error}`, "error");
        break;
    }
  }

  /** 订阅连接消息 */
  private onSubMessage(msg: ServerMessage, publisherId: string, pc: RTCPeerConnection): void {
    switch (msg.type) {
      case "sdp_answer":
        pc.setRemoteDescription({ type: "answer", sdp: msg.sdp })
          .then(() => {
            for (const c of msg.ice_candidates) {
              pc.addIceCandidate({ candidate: c, sdpMid: "0", sdpMLineIndex: 0 });
              pc.addIceCandidate({ candidate: c, sdpMid: "1", sdpMLineIndex: 1 });
            }
            // SDP 完成后发送 subscribe (音频 mid=0 + 视频 mid=1)
            const sig = this.subs.get(publisherId)?.signaling;
            sig?.send({ type: "subscribe", publisher_id: publisherId, track_mid: "0" });
            sig?.send({ type: "subscribe", publisher_id: publisherId, track_mid: "1" });
            this.cb.onLog(`订阅 ${publisherId} SDP 完成, 已发送 subscribe`, "success");
          })
          .catch((e) => this.cb.onLog(`订阅 SDP 失败: ${e}`, "error"));
        break;

      case "ice_candidate":
        pc.addIceCandidate({
          candidate: msg.candidate,
          sdpMid: msg.sdp_mid ?? "0",
          sdpMLineIndex: 0,
        });
        break;

      case "subscribed":
        this.cb.onSubscriptionUpdate(publisherId, { status: "subscribed" });
        this.cb.onLog(`订阅确认: ${publisherId}`, "success");
        break;

      case "error":
        this.cb.onLog(`订阅 ${publisherId} 错误: ${msg.error}`, "error");
        this.cb.onSubscriptionUpdate(publisherId, { status: "error" });
        break;
    }
  }

  // ── 工具 ──────────────────────────────────────────────────

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
