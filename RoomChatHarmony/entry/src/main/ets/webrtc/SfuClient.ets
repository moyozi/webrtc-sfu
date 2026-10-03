import webrtc from '@ohos/webrtc';
import { SignalingClient, ServerMessage } from './SignalingClient';
import { LogLevel, SubStatus } from '../model/Models';

/**
 * SFU 客户端 — 单条 WebSocket + 多个 RTCPeerConnection
 *
 * 架构与 Web 端 demo/src/lib/sfu.ts、Android 端 SfuClient.kt 完全对应:
 *   - pc_id = "pub"          发布连接 (sendonly), 加入房间
 *   - pc_id = "sub-<远端id>" 订阅连接 (recvonly), 不加入房间
 *
 * 所有 PC 复用同一条 WebSocket, 服务端按 pc_id 路由。
 * 音频轨道必须最先添加 (mid=0), 视频第二 (mid=1), 以匹配服务端的订阅 mid。
 */
interface PcEntry {
  pc: webrtc.RTCPeerConnection;
  role: string;
  publisherId: string | null;
}

/** BUNDLE 单端口候选的添加目标 */
interface IceTarget {
  mid: string;
  idx: number;
}

/** getStats 中的 inbound-rtp 统计 (用于诊断订阅媒体是否真正到达) */
interface InboundRtpStats extends webrtc.RTCStats {
  kind?: string;
  bytesReceived?: number;
  framesDecoded?: number;
  packetsReceived?: number;
}

/** getStats 中的 outbound-rtp 统计 (用于诊断本地是否真的发出媒体) */
interface OutboundRtpStats extends webrtc.RTCStats {
  kind?: string;
  bytesSent?: number;
  framesSent?: number;
  packetsSent?: number;
}

const PUB_PC_ID: string = 'pub';
const VIDEO_WIDTH: number = 640;
const VIDEO_HEIGHT: number = 480;
const VIDEO_FPS: number = 30;

export class SfuClient {
  onLog: (msg: string, level: LogLevel) => void = () => {};
  onParticipantJoined: (id: string) => void = () => {};
  onParticipantLeft: (id: string) => void = () => {};
  onSubscriptionUpdate: (publisherId: string, status: SubStatus | null, hasVideo: boolean | null) => void = () => {};
  onConnectionStateChange: (state: string) => void = () => {};
  private factory: webrtc.PeerConnectionFactory | null = null;
  private adm: webrtc.AudioDeviceModule | null = null;
  private localStream: webrtc.MediaStream | null = null;
  private localAudioTrack: webrtc.MediaStreamTrack | null = null;
  private localVideoTrack: webrtc.MediaStreamTrack | null = null;
  private frontCamera: boolean = true;

  private sig: SignalingClient | null = null;
  private roomId: string = '';
  private participantId: string = '';

  private pcs: Map<string, PcEntry> = new Map();
  private subPcByPublisher: Map<string, string> = new Map();
  private remoteVideoTracks: Map<string, webrtc.MediaStreamTrack> = new Map();
  private readyPcs: Set<string> = new Set();
  private pendingIce: Map<string, webrtc.RTCIceCandidate[]> = new Map();
  private disposed: boolean = false;

  getLocalVideoTrack(): webrtc.MediaStreamTrack | null {
    return this.localVideoTrack;
  }

  getLocalAudioTrack(): webrtc.MediaStreamTrack | null {
    return this.localAudioTrack;
  }

  remoteVideoTrack(publisherId: string): webrtc.MediaStreamTrack | null {
    const track = this.remoteVideoTracks.get(publisherId);
    return track != null ? track : null;
  }

  // ── 加入 / 发布 ────────────────────────────────────────────

  async join(serverUrl: string, room: string, participant: string): Promise<void> {
    this.roomId = room;
    this.participantId = participant;

    this.initFactory();
    this.createLocalMedia();

    const s = new SignalingClient();
    s.onMessage = (msg: ServerMessage) => {
      this.handleMessage(msg);
    };
    s.onClosed = () => {
      this.onLog('信令连接断开', LogLevel.WARN);
      this.onConnectionStateChange('disconnected');
    };
    await s.connect(serverUrl);
    this.sig = s;

    await this.createPublisher();
  }

  private initFactory(): void {
    if (this.factory != null) {
      return;
    }
    this.adm = new webrtc.AudioDeviceModule();
    // 不指定 videoEncoder/DecoderFactory, 使用库默认的组合工厂:
    // DefaultVideoDecoderFactory = Hardware(H264/H265) + Software(VP8/VP9/AV1/H264),
    // 并带软解回退。之前硬指定 HardwareVideoDecoderFactory(仅 H264/H265),
    // 遇到 Android 端默认发布的 VP8 就会解码失败 -> 远端黑屏。
    this.factory = new webrtc.PeerConnectionFactory({
      adm: this.adm
    });
    // MediaDevices 依赖默认的 PeerConnectionFactory, 且需要与创建 PeerConnection 的是同一个。
    webrtc.PeerConnectionFactory.setDefault(this.factory);
  }

  /**
   * 在 ArkTS 主线程直接创建本地音视频轨道。
   *
   * 注意: 不使用 MediaDevices.getUserMedia —— 其实现基于 Napi::AsyncWorker,
   * Execute() 跑在 libuv/N-API 工作线程(非 WebRTC 线程), 会在非 WebRTC 线程
   * 里调用 PeerConnectionFactory, 触发 WebRTC 的线程断言/数据竞争。
   * 这里改为与官方 demo prepareLocalMedia 一致的主线程 createXxxSource 方式。
   */
  private createLocalMedia(): void {
    const factory = this.factory;
    if (factory == null) {
      return;
    }
    try {
      const audioSource: webrtc.AudioSource = factory.createAudioSource({
        echoCancellation: true,
        noiseSuppression: true
      });
      this.localAudioTrack = factory.createAudioTrack('audio0', audioSource);

      const videoSource: webrtc.VideoSource = factory.createVideoSource({
        width: VIDEO_WIDTH,
        height: VIDEO_HEIGHT,
        frameRate: VIDEO_FPS,
        facingMode: this.frontCamera ? 'user' : 'environment'
      }, false);
      this.localVideoTrack = factory.createVideoTrack('video0', videoSource);

      const stream = new webrtc.MediaStream();
      if (this.localAudioTrack != null) {
        stream.addTrack(this.localAudioTrack);
      }
      if (this.localVideoTrack != null) {
        stream.addTrack(this.localVideoTrack);
      }
      this.localStream = stream;
      this.onLog('本地媒体流已就绪', LogLevel.SUCCESS);
    } catch (e) {
      this.onLog(`获取本地媒体失败: ${JSON.stringify(e)}`, LogLevel.ERROR);
      throw e as Error;
    }
  }

  private async createPublisher(): Promise<void> {
    const factory = this.factory;
    if (factory == null) {
      return;
    }
    const pc = factory.createPeerConnection(this.rtcConfig());
    const entry: PcEntry = { pc: pc, role: 'publisher', publisherId: null };
    this.pcs.set(PUB_PC_ID, entry);
    this.setupPcCallbacks(PUB_PC_ID, pc, entry);

    const streams: webrtc.MediaStream[] = this.localStream != null ? [this.localStream] : [];
    if (this.localAudioTrack != null) {
      pc.addTransceiver(this.localAudioTrack, { direction: 'sendonly', streams: streams });
    }
    if (this.localVideoTrack != null) {
      pc.addTransceiver(this.localVideoTrack, { direction: 'sendonly', streams: streams });
    }

    try {
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      await this.waitIceComplete(pc);
      const local = pc.localDescription;
      if (local == null) {
        return;
      }
      this.sig?.send(this.joinMessage(PUB_PC_ID, this.roomId, this.participantId, local.sdp, 'publisher'));
      this.flushIce(PUB_PC_ID);
      this.configureAudio();
      this.onLog(`正在加入房间 ${this.roomId} ...`, LogLevel.INFO);
      setTimeout(() => {
        this.pollOutboundStats(PUB_PC_ID);
      }, 5000);
    } catch (e) {
      this.onLog(`创建发布 offer 失败: ${JSON.stringify(e)}`, LogLevel.ERROR);
    }
  }

  // ── 订阅 ──────────────────────────────────────────────────

  async subscribe(publisherId: string): Promise<void> {
    if (this.subPcByPublisher.has(publisherId)) {
      this.onLog(`已订阅 ${publisherId}`, LogLevel.WARN);
      return;
    }
    if (this.sig == null) {
      return;
    }
    if (publisherId === this.participantId || publisherId.includes('-recv-')) {
      this.onLog(`忽略非法订阅目标: ${publisherId}`, LogLevel.WARN);
      return;
    }
    const factory = this.factory;
    if (factory == null) {
      return;
    }

    this.onLog(`正在订阅 ${publisherId} ...`, LogLevel.INFO);
    this.onSubscriptionUpdate(publisherId, SubStatus.CONNECTING, null);

    const pcId = `sub-${publisherId}`;
    const pc = factory.createPeerConnection(this.rtcConfig());
    const entry: PcEntry = { pc: pc, role: 'subscriber', publisherId: publisherId };
    this.pcs.set(pcId, entry);
    this.subPcByPublisher.set(publisherId, pcId);
    this.setupPcCallbacks(pcId, pc, entry);

    // 音频在前 (mid=0), 视频在后 (mid=1), 与服务端发布轨道顺序一致
    pc.addTransceiver('audio', { direction: 'recvonly' });
    pc.addTransceiver('video', { direction: 'recvonly' });

    try {
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      await this.waitIceComplete(pc);
      const local = pc.localDescription;
      if (local == null) {
        return;
      }
      this.sig?.send(
        this.joinMessage(pcId, this.roomId, `${this.participantId}-recv-${publisherId}`, local.sdp, 'subscriber')
      );
      this.flushIce(pcId);
    } catch (e) {
      this.onLog(`创建订阅 offer 失败: ${JSON.stringify(e)}`, LogLevel.ERROR);
    }
  }

  unsubscribe(publisherId: string): void {
    const pcId = this.subPcByPublisher.get(publisherId);
    if (pcId == null) {
      return;
    }
    this.subPcByPublisher.delete(publisherId);
    const entry = this.pcs.get(pcId);
    this.pcs.delete(pcId);

    if (entry != null) {
      for (const tr of entry.pc.getTransceivers()) {
        if (tr.mid != null) {
          this.sig?.send(this.unsubscribeMessage(pcId, publisherId, tr.mid));
        }
      }
      this.sig?.send(this.leaveMessage(pcId));
      try {
        entry.pc.close();
      } catch (e) {
        // ignore
      }
    }

    this.remoteVideoTracks.delete(publisherId);
    this.onLog(`已取消订阅 ${publisherId}`, LogLevel.INFO);
    this.onSubscriptionUpdate(publisherId, SubStatus.IDLE, false);
  }

  // ── 离开 ──────────────────────────────────────────────────

  leave(): void {
    this.pcs.forEach((entry: PcEntry, pcId: string) => {
      this.sig?.send(this.leaveMessage(pcId));
    });

    this.pcs.forEach((entry: PcEntry) => {
      try {
        entry.pc.close();
      } catch (e) {
        // ignore
      }
    });

    this.pcs.clear();
    this.subPcByPublisher.clear();
    this.remoteVideoTracks.clear();
    this.readyPcs.clear();
    this.pendingIce.clear();

    this.stopLocalMedia();

    this.sig?.close();
    this.sig = null;
  }

  /** 释放资源 (页面销毁时调用, 之后不可复用) */
  dispose(): void {
    if (this.disposed) {
      return;
    }
    this.disposed = true;
    this.leave();
    this.factory = null;
    this.adm = null;
  }

  // ── 媒体控制 ──────────────────────────────────────────────

  toggleAudio(enabled: boolean): void {
    if (this.localAudioTrack != null) {
      this.localAudioTrack.enabled = enabled;
    }
  }

  toggleVideo(enabled: boolean): void {
    if (this.localVideoTrack != null) {
      this.localVideoTrack.enabled = enabled;
    }
  }

  async switchCamera(): Promise<void> {
    const factory = this.factory;
    if (factory == null) {
      return;
    }
    this.frontCamera = !this.frontCamera;
    try {
      // 与 createLocalMedia 保持一致: 在主线程重建采集源, 避免走 async worker
      const videoSource: webrtc.VideoSource = factory.createVideoSource({
        width: VIDEO_WIDTH,
        height: VIDEO_HEIGHT,
        frameRate: VIDEO_FPS,
        facingMode: this.frontCamera ? 'user' : 'environment'
      }, false);
      const newTrack = factory.createVideoTrack('video0', videoSource);
      if (newTrack == null) {
        return;
      }

      const pubEntry = this.pcs.get(PUB_PC_ID);
      if (pubEntry != null) {
        for (const sender of pubEntry.pc.getSenders()) {
          if (sender.track != null && sender.track.kind === 'video') {
            await sender.replaceTrack(newTrack);
          }
        }
      }

      const oldTrack = this.localVideoTrack;
      this.localVideoTrack = newTrack;
      if (oldTrack != null) {
        try {
          oldTrack.stop();
        } catch (e) {
          // ignore
        }
      }
      this.onLog('已切换摄像头', LogLevel.INFO);
    } catch (e) {
      this.onLog(`切换摄像头失败: ${JSON.stringify(e)}`, LogLevel.WARN);
    }
  }

  private stopLocalMedia(): void {
    const tracks: webrtc.MediaStreamTrack[] = [];
    if (this.localStream != null) {
      const streamTracks = this.localStream.getTracks();
      for (const t of streamTracks) {
        tracks.push(t);
      }
    }
    if (this.localVideoTrack != null) {
      tracks.push(this.localVideoTrack);
    }
    if (this.localAudioTrack != null) {
      tracks.push(this.localAudioTrack);
    }
    for (const t of tracks) {
      try {
        t.stop();
      } catch (e) {
        // ignore
      }
    }
    this.localStream = null;
    this.localVideoTrack = null;
    this.localAudioTrack = null;
  }

  // ── PeerConnection 回调 ──────────────────────────────────

  private setupPcCallbacks(pcId: string, pc: webrtc.RTCPeerConnection, entry: PcEntry): void {
    pc.onicecandidate = (event: webrtc.RTCPeerConnectionIceEvent) => {
      if (event.candidate != null) {
        this.sendIce(pcId, event.candidate);
      }
    };
    pc.onconnectionstatechange = () => {
      this.onConnectionStateChange(pc.connectionState);
    };
    pc.ontrack = (event: webrtc.RTCTrackEvent) => {
      this.maybeAttachRemoteTrack(entry, event.track);
    };
  }

  private maybeAttachRemoteTrack(entry: PcEntry, track: webrtc.MediaStreamTrack): void {
    if (track.kind !== 'video') {
      return;
    }
    const publisherId = entry.publisherId;
    if (publisherId == null) {
      return;
    }
    this.remoteVideoTracks.set(publisherId, track);
    this.onLog(`收到 ${publisherId} 的 video 轨道`, LogLevel.SUCCESS);
    this.onSubscriptionUpdate(publisherId, null, true);
  }

  private scanReceivers(entry: PcEntry): void {
    const receivers = entry.pc.getReceivers();
    for (const r of receivers) {
      if (r.track != null) {
        this.maybeAttachRemoteTrack(entry, r.track);
      }
    }
  }

  // ── 信令消息处理 ──────────────────────────────────────────

  private async handleMessage(msg: ServerMessage): Promise<void> {
    switch (msg.type) {
      case 'sdp_answer':
        await this.onSdpAnswer(msg);
        break;
      case 'ice_candidate': {
        const pcId = msg.pc_id;
        if (pcId == null) {
          return;
        }
        const entry = this.pcs.get(pcId);
        if (entry == null) {
          return;
        }
        const candidate = msg.candidate;
        if (candidate == null) {
          return;
        }
        try {
          await entry.pc.addIceCandidate({
            candidate: candidate,
            sdpMid: msg.sdp_mid ?? '0',
            sdpMLineIndex: 0
          });
        } catch (e) {
          // 忽略无法添加的候选
        }
        break;
      }
      case 'subscribed': {
        const pcId = msg.pc_id ?? '';
        const entry = this.pcs.get(pcId);
        const pid = entry?.publisherId ?? msg.publisher_id ?? '';
        this.onSubscriptionUpdate(pid, SubStatus.SUBSCRIBED, null);
        this.onLog(`订阅确认: ${pid}`, LogLevel.SUCCESS);
        break;
      }
      case 'participant_joined': {
        const pid = msg.participant_id ?? '';
        if (pid === this.participantId || pid.startsWith(`${this.participantId}-recv-`)) {
          return;
        }
        this.onLog(`参与者加入: ${pid}`, LogLevel.INFO);
        this.onParticipantJoined(pid);
        break;
      }
      case 'participant_left': {
        const pid = msg.participant_id ?? '';
        this.onLog(`参与者离开: ${pid}`, LogLevel.WARN);
        this.unsubscribe(pid);
        this.onParticipantLeft(pid);
        break;
      }
      case 'error': {
        const err = msg.error ?? '';
        const pcId = msg.pc_id;
        this.onLog(
          pcId != null ? `服务器错误 [${pcId}]: ${err}` : `服务器错误: ${err}`,
          LogLevel.ERROR
        );
        break;
      }
    }
  }

  private async onSdpAnswer(msg: ServerMessage): Promise<void> {
    const pcId = msg.pc_id ?? '';
    const sdp = msg.sdp ?? '';
    const entry = this.pcs.get(pcId);
    if (entry == null) {
      this.onLog(`收到未知 pc_id 的 sdp_answer: ${pcId}`, LogLevel.WARN);
      return;
    }

    try {
      await entry.pc.setRemoteDescription({ type: 'answer', sdp: sdp });
    } catch (e) {
      this.onLog(`SDP 设置失败: ${JSON.stringify(e)}`, LogLevel.ERROR);
      return;
    }

    // 服务器是 BUNDLE 单端口候选: 发布连接加到 mid 0; 订阅连接两个 mid 都尝试
    const targets: IceTarget[] = entry.role === 'subscriber'
      ? [{ mid: '0', idx: 0 }, { mid: '1', idx: 1 }]
      : [{ mid: '0', idx: 0 }];

    const arr = msg.ice_candidates;
    if (arr != null) {
      for (const c of arr) {
        if (c == null || c.length === 0) {
          continue;
        }
        for (const t of targets) {
          try {
            await entry.pc.addIceCandidate({ candidate: c, sdpMid: t.mid, sdpMLineIndex: t.idx });
          } catch (e) {
            // 忽略无法添加的候选
          }
        }
      }
    }

    this.scanReceivers(entry);

    if (entry.role === 'subscriber' && entry.publisherId != null) {
      for (const tr of entry.pc.getTransceivers()) {
        const subMid = tr.mid;
        if (subMid == null) {
          continue;
        }
        // 发布端约定: 音频 mid=0, 视频 mid=1;
        // 订阅端自身 mid 可能不同, 通过 subscriber_mid 单独告知服务端。
        let kind = '';
        const recv = tr.receiver;
        if (recv != null && recv.track != null) {
          kind = recv.track.kind;
        }
        let publisherMid = subMid;
        if (kind === 'audio') {
          publisherMid = '0';
        } else if (kind === 'video') {
          publisherMid = '1';
        }
        this.onLog(`sub_mid=${subMid} kind=${kind} -> pub_mid=${publisherMid}`, LogLevel.INFO);
        this.sig?.send(this.subscribeMessage(pcId, entry.publisherId, publisherMid, subMid));
      }
      this.onLog(`订阅 ${entry.publisherId} SDP 完成, 已发送 subscribe`, LogLevel.SUCCESS);
      setTimeout(() => {
        this.pollInboundStats(pcId, entry.publisherId ?? '');
      }, 4000);
    } else {
      this.onLog('SDP 协商完成, 已添加服务器候选', LogLevel.SUCCESS);
    }
  }

  /** 拉取发布连接的 outbound 统计, 判断本地媒体是否真的发出 (黑屏诊断用) */
  private pollOutboundStats(pcId: string): void {
    const entry = this.pcs.get(pcId);
    if (entry == null) {
      return;
    }
    entry.pc.getStats().then((report: webrtc.RTCStatsReport) => {
      let bytesSent: number = 0;
      let framesSent: number = 0;
      let found: boolean = false;
      report.stats.forEach((v: webrtc.RTCStats, k: string) => {
        const s = v as OutboundRtpStats;
        if (s.type === 'outbound-rtp' && s.kind === 'video') {
          found = true;
          bytesSent = s.bytesSent ?? 0;
          framesSent = s.framesSent ?? 0;
        }
      });
      this.onLog(`[stats pub] video bytesSent=${bytesSent} framesSent=${framesSent}${found ? '' : ' (no outbound video rtp)'}`, LogLevel.INFO);
    }).catch((e: Error) => {
      this.onLog(`[stats pub] 失败: ${JSON.stringify(e)}`, LogLevel.WARN);
    });
  }

  /** 拉取订阅连接的 inbound 统计, 判断 RTP 是否真的到达 (黑屏诊断用) */
  private pollInboundStats(pcId: string, publisherId: string): void {
    const entry = this.pcs.get(pcId);
    if (entry == null) {
      return;
    }
    entry.pc.getStats().then((report: webrtc.RTCStatsReport) => {
      let videoBytes: number = 0;
      let videoFrames: number = 0;
      let found: boolean = false;
      report.stats.forEach((v: webrtc.RTCStats, k: string) => {
        const s = v as InboundRtpStats;
        if (s.type === 'inbound-rtp' && s.kind === 'video') {
          found = true;
          videoBytes = s.bytesReceived ?? 0;
          videoFrames = s.framesDecoded ?? 0;
        }
      });
      if (found) {
        this.onLog(`[stats] ${publisherId} video bytes=${videoBytes} frames=${videoFrames}`, LogLevel.INFO);
      } else {
        this.onLog(`[stats] ${publisherId} 无 inbound video rtp`, LogLevel.WARN);
      }
    }).catch((e: Error) => {
      this.onLog(`[stats] ${publisherId} 失败: ${JSON.stringify(e)}`, LogLevel.WARN);
    });
  }

  // ── ICE / PC 辅助 ─────────────────────────────────────────

  private sendIce(pcId: string, candidate: webrtc.RTCIceCandidate): void {
    if (this.readyPcs.has(pcId)) {
      this.sig?.send(this.iceMessage(pcId, candidate));
    } else {
      let list = this.pendingIce.get(pcId);
      if (list == null) {
        list = [];
        this.pendingIce.set(pcId, list);
      }
      list.push(candidate);
    }
  }

  private flushIce(pcId: string): void {
    this.readyPcs.add(pcId);
    const list = this.pendingIce.get(pcId);
    if (list != null) {
      for (const c of list) {
        this.sig?.send(this.iceMessage(pcId, c));
      }
      this.pendingIce.delete(pcId);
    }
  }

  private rtcConfig(): webrtc.RTCConfiguration {
    return {
      iceServers: [],
      bundlePolicy: 'max-bundle',
      rtcpMuxPolicy: 'require',
      iceTransportPolicy: 'all'
    };
  }

  private async waitIceComplete(pc: webrtc.RTCPeerConnection, timeoutMs: number = 2000): Promise<void> {
    const start = Date.now();
    while (pc.iceGatheringState !== 'complete') {
      if (Date.now() - start > timeoutMs) {
        break;
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 50));
    }
  }

  private configureAudio(): void {
    // 音频会话策略在页面 aboutToAppear 中已激活, 此处无需额外处理。
  }

  // ── JSON 构造 ─────────────────────────────────────────────

  private joinMessage(pcId: string, roomId: string, participantId: string, sdp: string, role: string): string {
    return JSON.stringify({
      type: 'join',
      pc_id: pcId,
      room_id: roomId,
      participant_id: participantId,
      sdp: sdp,
      role: role
    });
  }

  private iceMessage(pcId: string, c: webrtc.RTCIceCandidate): string {
    return JSON.stringify({
      type: 'ice_candidate',
      pc_id: pcId,
      candidate: c.candidate,
      sdp_mid: c.sdpMid ?? null,
      sdp_mline_index: c.sdpMLineIndex ?? null
    });
  }

  private subscribeMessage(pcId: string, publisherId: string, trackMid: string, subscriberMid: string): string {
    return JSON.stringify({
      type: 'subscribe',
      pc_id: pcId,
      publisher_id: publisherId,
      track_mid: trackMid,
      subscriber_mid: subscriberMid
    });
  }

  private unsubscribeMessage(pcId: string, publisherId: string, mid: string): string {
    return JSON.stringify({
      type: 'unsubscribe',
      pc_id: pcId,
      publisher_id: publisherId,
      track_mid: mid
    });
  }

  private leaveMessage(pcId: string): string {
    return JSON.stringify({
      type: 'leave',
      pc_id: pcId
    });
  }
}
