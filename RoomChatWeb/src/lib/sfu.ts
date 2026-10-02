import { SignalingClient } from "./signaling";
import { allocateSignaling, type AllocateResult } from "./dispatcher";
import type { ServerMessage, LogEntry, RemoteParticipant, ParticipantInfo } from "./types";

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

/** 与 Rust 端 `PROTOCOL_VERSION` 保持一致 */
const PROTOCOL_VERSION = 1;

/** 应用层心跳间隔 (毫秒) */
const HEARTBEAT_MS = 20_000;

/** 从 JWT 载荷中解析 `sub` (仅用于取用户 ID, 不做签名校验)。 */
function jwtSubject(token: string): string | null {
  try {
    const payload = token.split(".")[1];
    if (!payload) return null;
    const b64 = payload.replace(/-/g, "+").replace(/_/g, "/");
    const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
    const bytes = Uint8Array.from(atob(padded), (c) => c.charCodeAt(0));
    const json = JSON.parse(new TextDecoder().decode(bytes));
    return typeof json.sub === "string" ? json.sub : null;
  } catch {
    return null;
  }
}

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
  /** 远端成员媒体状态变化 (静音/关摄像头) */
  onParticipantUpdated?: (id: string, media: { audioEnabled: boolean; videoEnabled: boolean }) => void;
  /** 服务端握手完成 (welcome), 可用于展示协议版本/能力 */
  onWelcome?: (info: { protocol_version: number; server: string; features: string[] }) => void;
  /** 分配器已返回本房间的信令地址 */
  onAllocated?: (result: AllocateResult) => void;
}

export class SfuClient {
  // 身份
  private participantId = "";
  private roomId = "";
  /** 用户填写的分配器地址 (http(s)://) 或直连信令地址 (ws(s)://) */
  private endpoint = "";
  /** 分配器返回的实际信令 WebSocket 地址 */
  private wsUrl = "";
  /** 用户鉴权 JWT (可选; 服务端启用鉴权时必填) */
  private token = "";
  /** 是否已主动离开 (用于区分正常关闭与意外断开) */
  private leaving = false;

  // 单条信令连接 (所有 PC 复用)
  private sig: SignalingClient | null = null;

  // pc_id → PC 元数据
  private pcs = new Map<string, PcEntry>();
  // 远端 publisherId → 订阅它的 pc_id
  private subPcByPublisher = new Map<string, string>();
  // 已入房的远端成员 (用于 joined 快照与 participant_joined 去重)
  private knownParticipants = new Set<string>();
  // 已向服务器 join 成功的 pc_id (收到 joined 后置位)。
  // 用于避免在服务器登记会话前发送 ice_candidate (会被判为"未知 pc_id")。
  private joinedPcs = new Set<string>();
  // join 前收集到的 ICE 候选缓冲 (pc_id -> candidates), joined 后补发。
  private pendingIce = new Map<string, RTCIceCandidate[]>();
  // 应用层心跳定时器
  private heartbeat: ReturnType<typeof setInterval> | null = null;

  private localStream: MediaStream | null = null;

  private cb: SfuCallbacks;

  constructor(callbacks: SfuCallbacks) {
    this.cb = callbacks;
  }

  // ── 发布: 加入房间 ─────────────────────────────────────────

  async join(endpoint: string, roomId: string, participantId: string, token = ""): Promise<void> {
    this.endpoint = endpoint.trim();
    this.roomId = roomId;
    this.leaving = false;
    // 启用 JWT 鉴权时, participant_id 必须等于 token 的 sub。
    // 若提供了 token, 则以其 sub 为准, 避免与昵称不一致而被拒。
    this.token = token.trim();
    this.participantId = this.token
      ? jwtSubject(this.token) ?? participantId
      : participantId;

    // 1. 固定先向分配器请求本房间的信令地址 (同一 room_id → 同一信令机)
    this.cb.onLog(`正在向分配器请求信令地址 (room=${roomId})...`);
    const alloc = await allocateSignaling(this.endpoint, roomId);
    this.wsUrl = alloc.wsUrl;
    this.cb.onAllocated?.(alloc);
    this.cb.onLog(`分配信令节点 ${alloc.nodeId}: ${alloc.wsUrl}`, "success");

    // 2. 获取本地媒体
    this.cb.onLog("正在获取摄像头和麦克风...");
    this.localStream = await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: { width: 640, height: 480 },
    });
    this.cb.onLog("本地媒体流已就绪", "success");

    // 3. 连接信令服务器 (整个会话只此一条 WebSocket)
    this.sig = new SignalingClient(alloc.wsUrl, this.token || undefined);
    await this.sig.connect();
    this.sig.onMessage = (msg) => this.onMessage(msg);
    this.sig.onClose = () => {
      if (this.leaving) return;
      this.cb.onLog("信令连接断开, 请重新加入房间", "warn");
      this.cb.onConnectionStateChange("disconnected");
    };

    // 握手: 协商协议版本 (拿到 welcome 中的能力集)
    this.sig.send({ type: "hello", protocol_version: PROTOCOL_VERSION, client: "roomchat-web" });
    // 应用层心跳: 移动端切网 / NAT 空闲超时后保持信令通道活跃
    this.startHeartbeat();

    // 4. 创建发布 PC (sendonly)
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

    // 5. 创建 offer, 等待 ICE 收集完成
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    await this.waitIceComplete(pc);

    // 6. 发送 join (发布连接, role=publisher)
    this.sig.send({
      type: "join",
      pc_id: PUB_PC_ID,
      room_id: roomId,
      participant_id: this.participantId,
      sdp: pc.localDescription!.sdp,
      role: "publisher",
      ...(this.token ? { token: this.token } : {}),
    });
    this.cb.onLog(`正在加入房间 ${roomId}...`);
  }

  /** 获取本地媒体流 (用于 UI 显示) */
  getLocalStream(): MediaStream | null {
    return this.localStream;
  }

  /** 分配器为本房间返回的实际信令地址 (join 成功后可用)。 */
  get signalingUrl(): string {
    return this.wsUrl;
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
      ...(this.token ? { token: this.token } : {}),
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
    this.leaving = true;
    this.stopHeartbeat();
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
    this.knownParticipants.clear();
    this.joinedPcs.clear();
    this.pendingIce.clear();

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
    this.updateMedia({ audioEnabled: enabled });
  }

  toggleVideo(enabled: boolean): void {
    this.localStream?.getVideoTracks().forEach((t) => (t.enabled = enabled));
    this.updateMedia({ videoEnabled: enabled });
  }

  /** 通知服务端本发布者的媒体状态 (服务端会广播给房间内其他人)。 */
  updateMedia(media: { audioEnabled?: boolean; videoEnabled?: boolean }): void {
    if (media.audioEnabled === undefined && media.videoEnabled === undefined) return;
    this.sig?.send({
      type: "update_media",
      pc_id: PUB_PC_ID,
      ...(media.audioEnabled !== undefined ? { audio_enabled: media.audioEnabled } : {}),
      ...(media.videoEnabled !== undefined ? { video_enabled: media.videoEnabled } : {}),
    });
  }

  /** 手动请求房间成员快照 (可用于重连 / UI 刷新)。 */
  requestRoomState(): void {
    this.sig?.send({ type: "get_room_state", room_id: this.roomId });
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeat = setInterval(() => {
      this.sig?.send({ type: "ping", t: Date.now() });
    }, HEARTBEAT_MS);
  }

  private stopHeartbeat(): void {
    if (this.heartbeat !== null) {
      clearInterval(this.heartbeat);
      this.heartbeat = null;
    }
  }

  // ── 信令消息处理 (统一入口, 按 pc_id 分发) ─────────────────

  private onMessage(msg: ServerMessage): void {
    switch (msg.type) {
      case "welcome": {
        this.cb.onLog(`协议握手: ${msg.server} (v${msg.protocol_version})`, "info");
        this.cb.onWelcome?.(msg);
        break;
      }

      case "pong":
        // 心跳回包, 仅用于保活, 无需处理
        break;

      case "joined": {
        // 会话已在服务器登记, 现在补发此前缓冲的 ICE 候选。
        this.flushPendingIce(msg.pc_id);
        this.ingestRoster(msg.participants, true);
        this.cb.onLog(
          `已加入房间 ${msg.room_id} (${msg.participants.length} 位已有成员)`,
          "success"
        );
        break;
      }

      case "room_state": {
        this.ingestRoster(msg.participants, false);
        this.cb.onLog(`房间 ${msg.room_id} 共 ${msg.participants.length} 人`, "info");
        break;
      }

      case "left":
        this.cb.onLog(`已离开会话 ${msg.pc_id}`, "info");
        break;

      case "participant_updated":
        this.cb.onParticipantUpdated?.(msg.participant_id, {
          audioEnabled: msg.audio_enabled,
          videoEnabled: msg.video_enabled,
        });
        break;

      case "unsubscribed":
        // 已在 unsubscribe() 本地更新, 这里仅记录确认
        this.cb.onLog(`已取消订阅 ${msg.publisher_id}`, "info");
        break;

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

      case "participant_joined": {
        const id = msg.participant_id;
        // 防御性过滤: 忽略自己, 以及自己的订阅连接 (participantId-recv-*).
        if (id === this.participantId || id.startsWith(`${this.participantId}-recv-`)) return;
        // joined 快照可能已包含该成员, 去重避免重复订阅。
        if (this.knownParticipants.has(id)) return;
        this.knownParticipants.add(id);
        this.cb.onLog(`参与者加入: ${id}`, "info");
        this.cb.onParticipantJoined(id);
        break;
      }

      case "participant_left":
        this.knownParticipants.delete(msg.participant_id);
        this.cb.onLog(`参与者离开: ${msg.participant_id}`, "warn");
        this.unsubscribe(msg.participant_id);
        this.cb.onParticipantLeft(msg.participant_id);
        break;

      case "error":
        this.cb.onLog(
          (msg.pc_id ? `服务器错误 [${msg.pc_id}]` : "服务器错误") +
            (msg.code ? ` (${msg.code})` : "") +
            `: ${msg.error}`,
          "error"
        );
        break;
    }
  }

  /** 将服务端成员快照写入本地已知集合并触发回调 (joined / room_state 共用)。 */
  private ingestRoster(participants: ParticipantInfo[], reset: boolean): void {
    if (reset) this.knownParticipants.clear();
    for (const p of participants) {
      const id = p.participant_id;
      if (id === this.participantId || id.startsWith(`${this.participantId}-recv-`)) continue;
      const isNew = !this.knownParticipants.has(id);
      this.knownParticipants.add(id);
      if (isNew) this.cb.onParticipantJoined(id);
      this.cb.onParticipantUpdated?.(id, {
        audioEnabled: p.audio_enabled,
        videoEnabled: p.video_enabled,
      });
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

  /**
   * 发送 ICE 候选 (trickle)。
   *
   * `pc.onicecandidate` 会在发送 join **之前** 就触发 (offer/ICE 收集阶段),
   * 而服务器此时还没有该 pc_id 的会话, 直接发送会被判为 "未知 pc_id, 请先 join"。
   * 因此未 join 的 pc 先缓冲, 收到 `joined` 后再补发。
   */
  private sendIce(pcId: string, cand: RTCIceCandidate): void {
    if (!this.joinedPcs.has(pcId)) {
      const pending = this.pendingIce.get(pcId) ?? [];
      pending.push(cand);
      this.pendingIce.set(pcId, pending);
      return;
    }
    this.sendIceNow(pcId, cand);
  }

  private sendIceNow(pcId: string, cand: RTCIceCandidate): void {
    this.sig?.send({
      type: "ice_candidate",
      pc_id: pcId,
      candidate: cand.candidate,
      sdp_mid: cand.sdpMid,
      sdp_mline_index: cand.sdpMLineIndex,
    });
  }

  /** 服务器确认 pc 已 join 后, 标记并补发缓冲的 ICE 候选。 */
  private flushPendingIce(pcId: string): void {
    this.joinedPcs.add(pcId);
    const pending = this.pendingIce.get(pcId);
    if (pending && pending.length) {
      for (const cand of pending) this.sendIceNow(pcId, cand);
    }
    this.pendingIce.delete(pcId);
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
