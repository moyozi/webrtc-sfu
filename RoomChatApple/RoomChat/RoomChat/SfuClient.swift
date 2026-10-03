import Foundation
import AVFoundation
import CoreMedia
import WebRTC

/// 日志 / 参与者 / 订阅状态回调
@MainActor
protocol SfuClientDelegate: AnyObject {
    func sfu(_ client: SfuClient, didLog message: String, level: LogLevel)
    func sfu(_ client: SfuClient, didJoin participantId: String)
    func sfu(_ client: SfuClient, didLeave participantId: String)
    func sfu(_ client: SfuClient, didUpdate publisherId: String, status: SubStatus?, hasVideo: Bool?)
    func sfu(_ client: SfuClient, didChangeConnectionState state: String)
}

enum SfuError: Error {
    case generic(String)
}

/// 本地视频帧探针 (仅诊断): 挂到 localVideoTrack 上确认采集是否真的产帧,
/// 用于区分“采集无帧”与“RTCMTLVideoView 渲染问题”。
private final class LocalFrameProbe: NSObject, RTCVideoRenderer {
    private let onFirstFrame: () -> Void
    private var fired = false

    init(onFirstFrame: @escaping () -> Void) {
        self.onFirstFrame = onFirstFrame
    }

    func setSize(_ size: CGSize) {}

    func renderFrame(_ frame: RTCVideoFrame?) {
        guard frame != nil, !fired else { return }
        fired = true
        onFirstFrame()
    }
}

/// SFU 客户端 — 单条 WebSocket + 多个 RTCPeerConnection
///
/// 架构与 Web 端 RoomChatWeb/src/lib/sfu.ts 完全对应:
///   - pc_id = "pub"          发布连接 (sendonly), 加入房间
///   - pc_id = "sub-<远端id>" 订阅连接 (recvonly), 不加入房间
///
/// 所有 PC 复用同一条 WebSocket, 服务端按 pc_id 路由。
/// 音频轨道必须最先添加 (mid=0), 视频第二 (mid=1), 以匹配服务端的订阅 mid。
@MainActor
final class SfuClient: NSObject {

    static let pubPcId = "pub"
    /// 与 SFU 服务端 PROTOCOL_VERSION 对应。
    static let protocolVersion = 2

    private static let factory: RTCPeerConnectionFactory = {
        _ = RTCInitializeSSL()
        let encoder = RTCDefaultVideoEncoderFactory()
        let decoder = RTCDefaultVideoDecoderFactory()
        return RTCPeerConnectionFactory(encoderFactory: encoder, decoderFactory: decoder)
    }()

    weak var delegate: SfuClientDelegate?

    private var audioSource: RTCAudioSource?
    private var videoSource: RTCVideoSource?
    private(set) var localAudioTrack: RTCAudioTrack?
    private(set) var localVideoTrack: RTCVideoTrack?
    private var capturer: RTCVideoCapturer?
    private var localFrameProbe: LocalFrameProbe?
    private var isFrontCamera = true

    private var signaling: SignalingClient?
    private var roomId = ""
    private var participantId = ""

    private final class PcEntry {
        let pc: RTCPeerConnection
        let role: String
        let publisherId: String?
        init(pc: RTCPeerConnection, role: String, publisherId: String?) {
            self.pc = pc
            self.role = role
            self.publisherId = publisherId
        }
    }

    private var pcs: [String: PcEntry] = [:]
    private var pcIdsByObject: [ObjectIdentifier: String] = [:]
    private var subPcByPublisher: [String: String] = [:]
    private(set) var remoteVideoTracks: [String: RTCVideoTrack] = [:]
    private var readyPcs = Set<String>()
    private var pendingIce: [String: [RTCIceCandidate]] = [:]
    /// 各发布者已知的轨道 (mid + kind)。
    private var publisherTracks: [String: [TrackInfo]] = [:]
    /// 已发送 subscribe 的发布者 track_mid 集合 (去重 + 取消订阅用)。
    private var sentSubscribeMids: [String: Set<String>] = [:]

    // ── 加入 / 发布 ────────────────────────────────────────────

    func join(serverUrl: String, roomId: String, participantId: String) async throws {
        self.roomId = roomId
        self.participantId = participantId

        try await requestMediaPermissions()
        createLocalMedia()

        guard let url = URL(string: serverUrl) else {
            throw SfuError.generic("非法信令地址: \(serverUrl)")
        }
        let sig = SignalingClient(url: url)
        sig.onMessage = { [weak self] obj in
            Task { @MainActor in self?.handleMessage(obj) }
        }
        sig.onClosed = { [weak self] in
            Task { @MainActor in
                guard let self = self else { return }
                self.log("信令连接断开", .warn)
                self.delegate?.sfu(self, didChangeConnectionState: "disconnected")
            }
        }
        try await sig.connect()
        self.signaling = sig
        // 协议握手: 声明协议版本, 服务端回 welcome。
        signaling?.send([
            "type": "hello",
            "protocol_version": SfuClient.protocolVersion,
            "client": "roomchat-apple",
        ])

        try await createPublisher()
    }

    /// 先显式申请摄像头 / 麦克风权限, 再启动采集。
    ///
    /// iOS 上 RTCCameraVideoCapturer 不会自动弹出权限申请, 若未授权就 startCapture,
    /// AVCaptureSession 不会产出任何帧, 表现为“本地无画面”。与 Android / Web 端一致,
    /// 加入前必须拿到权限 (拒绝则直接抛错, 由 UI 提示)。
    private func requestMediaPermissions() async throws {
        let cameraGranted = await withCheckedContinuation { (cont: CheckedContinuation<Bool, Never>) in
            AVCaptureDevice.requestAccess(for: .video) { granted in
                cont.resume(returning: granted)
            }
        }
        guard cameraGranted else {
            throw SfuError.generic("需要摄像头权限进行视频通话")
        }

        let micGranted = await withCheckedContinuation { (cont: CheckedContinuation<Bool, Never>) in
            AVAudioApplication.requestRecordPermission { granted in
                cont.resume(returning: granted)
            }
        }
        guard micGranted else {
            throw SfuError.generic("需要麦克风权限进行语音通话")
        }
        log("摄像头/麦克风权限已就绪", .success)
    }

    private func createLocalMedia() {
        let audioConstraints = RTCMediaConstraints(mandatoryConstraints: nil, optionalConstraints: nil)
        let audioSource = SfuClient.factory.audioSource(with: audioConstraints)
        self.audioSource = audioSource
        self.localAudioTrack = SfuClient.factory.audioTrack(with: audioSource, trackId: "audio0")

        let videoSource = SfuClient.factory.videoSource()
        self.videoSource = videoSource
#if targetEnvironment(simulator)
        self.capturer = RTCFileVideoCapturer(delegate: videoSource)
#else
        self.capturer = RTCCameraVideoCapturer(delegate: videoSource)
#endif
        self.localVideoTrack = SfuClient.factory.videoTrack(with: videoSource, trackId: "video0")
        attachLocalFrameProbe()

        startCapture()
        log("本地媒体流已就绪", .success)
    }

    /// 诊断用: 挂一个只数帧的 renderer, 确认采集真的产帧 (与渲染问题区分)。
    private func attachLocalFrameProbe() {
        guard let track = localVideoTrack else { return }
        let probe = LocalFrameProbe { [weak self] in
            DispatchQueue.main.async {
                self?.log("收到本地视频帧 (采集正常)", .success)
            }
        }
        track.add(probe)
        localFrameProbe = probe
    }

    private func startCapture() {
        guard let capturer = capturer as? RTCCameraVideoCapturer else {
            log("模拟器无摄像头: 本地预览不可用 (请在真机运行)", .warn)
            return
        }
        let devices = RTCCameraVideoCapturer.captureDevices()
        guard let device = devices.first(where: { $0.position == .front }) ?? devices.first else {
            log("未找到可用摄像头", .error)
            return
        }
        isFrontCamera = device.position == .front
        let formats = RTCCameraVideoCapturer.supportedFormats(for: device)
        let format = formats.min { lhs, rhs in
            let l = CMVideoFormatDescriptionGetDimensions(lhs.formatDescription)
            let r = CMVideoFormatDescriptionGetDimensions(rhs.formatDescription)
            return abs(Int(l.width) - 640) + abs(Int(l.height) - 480)
                < abs(Int(r.width) - 640) + abs(Int(r.height) - 480)
        } ?? formats.first
        guard let format = format else {
            log("未找到可用采集格式", .error)
            return
        }
        let fps = format.videoSupportedFrameRateRanges
            .max { $0.maxFrameRate < $1.maxFrameRate }?.maxFrameRate ?? 30
        let dims = CMVideoFormatDescriptionGetDimensions(format.formatDescription)
        log("启动摄像头: \(device.localizedName) \(dims.width)x\(dims.height) @\(Int(fps))fps", .info)

        capturer.startCapture(
            with: device,
            format: format,
            fps: Int(fps)
        ) { [weak self] error in
            DispatchQueue.main.async {
                if let error = error {
                    self?.log("摄像头启动失败: \(error.localizedDescription)", .error)
                } else {
                    self?.log("摄像头采集已启动", .success)
                }
            }
        }

        // 延迟确认采集会话是否真的在运行 (诊断黑屏用)
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
            self?.log("captureSession.isRunning = \(capturer.captureSession.isRunning)", .info)
        }
    }

    private func createPublisher() async throws {
        guard let pc = SfuClient.factory.peerConnection(
            with: rtcConfig(),
            constraints: pcConstraints(),
            delegate: self
        ) else {
            throw SfuError.generic("创建发布连接失败")
        }
        let pcId = SfuClient.pubPcId
        register(pc, pcId: pcId, role: "publisher", publisherId: nil)

        if let audio = localAudioTrack {
            pc.add(audio, streamIds: ["stream0"])
        }
        if let video = localVideoTrack {
            pc.add(video, streamIds: ["stream0"])
        }

        let constraints = RTCMediaConstraints(mandatoryConstraints: nil, optionalConstraints: nil)
        let offer = try await makeOffer(pc, constraints: constraints)
        try await setLocal(pc, offer)
        sendJoin(pcId: pcId, pc: pc, participantId: participantId, role: "publisher")
        configureAudio()
        log("正在加入房间 \(roomId) ...", .info)

        // 5 秒后检查 outbound-rtp, 确认本地视频是否真的在发出 (诊断远端看不到 iOS 视频用)
        DispatchQueue.main.asyncAfter(deadline: .now() + 5.0) { [weak self] in
            self?.pollOutboundStats()
        }
    }

    /// 拉取发布连接的 outbound 统计: 若 framesSent/bytesSent 为 0, 说明视频根本没发出去
    /// (编码/协商问题); 若 > 0, 说明 iOS 已发流, 问题在 SFU 转发或订阅端。
    private func pollOutboundStats() {
        guard let entry = pcs[SfuClient.pubPcId] else { return }
        entry.pc.statistics { [weak self] report in
            var bytesSent = 0
            var framesSent = 0
            var found = false
            for (_, stat) in report.statistics {
                guard stat.type == "outbound-rtp" else { continue }
                let kind = stat.values["kind"] as? String ?? ""
                if kind == "video" {
                    found = true
                    bytesSent = (stat.values["bytesSent"] as? NSNumber)?.intValue ?? 0
                    framesSent = (stat.values["framesSent"] as? NSNumber)?.intValue ?? 0
                }
            }
            DispatchQueue.main.async {
                self?.log(
                    "[stats pub] video bytesSent=\(bytesSent) framesSent=\(framesSent)\(found ? "" : " (no outbound video rtp)")",
                    .info
                )
            }
        }
    }

    // ── 订阅 ──────────────────────────────────────────────────

    func subscribe(_ publisherId: String) {
        if subPcByPublisher[publisherId] != nil {
            log("已订阅 \(publisherId)", .warn)
            return
        }
        if publisherId == participantId || publisherId.contains("-recv-") {
            log("忽略非法订阅目标: \(publisherId)", .warn)
            return
        }
        guard signaling != nil else { return }

        log("正在订阅 \(publisherId) ...", .info)
        delegate?.sfu(self, didUpdate: publisherId, status: .connecting, hasVideo: nil)

        let pcId = "sub-\(publisherId)"
        guard let pc = SfuClient.factory.peerConnection(
            with: rtcConfig(),
            constraints: pcConstraints(),
            delegate: self
        ) else {
            delegate?.sfu(self, didUpdate: publisherId, status: .error, hasVideo: nil)
            return
        }
        register(pc, pcId: pcId, role: "subscriber", publisherId: publisherId)
        subPcByPublisher[publisherId] = pcId

        Task {
            do {
                // 音频在前 (mid=0), 视频在后 (mid=1)
                let constraints = RTCMediaConstraints(mandatoryConstraints: [
                    kRTCMediaConstraintsOfferToReceiveAudio: kRTCMediaConstraintsValueTrue,
                    kRTCMediaConstraintsOfferToReceiveVideo: kRTCMediaConstraintsValueTrue,
                ], optionalConstraints: nil)
                let offer = try await makeOffer(pc, constraints: constraints)
                try await setLocal(pc, offer)
                sendJoin(
                    pcId: pcId,
                    pc: pc,
                    participantId: "\(participantId)-recv-\(publisherId)",
                    role: "subscriber"
                )
            } catch {
                log("创建订阅 offer 失败: \(error)", .error)
            }
        }
    }

    func unsubscribe(_ publisherId: String) {
        guard let pcId = subPcByPublisher.removeValue(forKey: publisherId),
              let entry = pcs.removeValue(forKey: pcId) else { return }
        unregister(entry.pc)

        // 按发布者实际 mid 取消订阅 (与 subscribe 时发送的 track_mid 对应)。
        for trackMid in sentSubscribeMids.removeValue(forKey: publisherId) ?? [] {
            signaling?.send([
                "type": "unsubscribe",
                "pc_id": pcId,
                "publisher_id": publisherId,
                "track_mid": trackMid,
            ])
        }
        signaling?.send(["type": "leave", "pc_id": pcId])
        entry.pc.close()
        remoteVideoTracks[publisherId] = nil

        log("已取消订阅 \(publisherId)", .info)
        delegate?.sfu(self, didUpdate: publisherId, status: .idle, hasVideo: false)
    }

    // ── 离开 ──────────────────────────────────────────────────

    func leave() {
        for pcId in pcs.keys {
            signaling?.send(["type": "leave", "pc_id": pcId])
        }
        for entry in pcs.values {
            entry.pc.close()
        }
        pcs.removeAll()
        pcIdsByObject.removeAll()
        subPcByPublisher.removeAll()
        remoteVideoTracks.removeAll()
        readyPcs.removeAll()
        pendingIce.removeAll()
        publisherTracks.removeAll()
        sentSubscribeMids.removeAll()

        stopLocalMedia()
        signaling?.close()
        signaling = nil
    }

    func dispose() {
        leave()
    }

    private func stopLocalMedia() {
        (capturer as? RTCCameraVideoCapturer)?.stopCapture()
        capturer = nil
        localAudioTrack = nil
        localVideoTrack = nil
        videoSource = nil
        audioSource = nil
    }

    // ── 媒体控制 ──────────────────────────────────────────────

    func toggleAudio(_ enabled: Bool) {
        localAudioTrack?.isEnabled = enabled
    }

    func toggleVideo(_ enabled: Bool) {
        localVideoTrack?.isEnabled = enabled
    }

    func switchCamera() {
        guard let capturer = capturer as? RTCCameraVideoCapturer else { return }
        let devices = RTCCameraVideoCapturer.captureDevices()
        guard devices.count > 1 else { return }
        guard let device = devices.first(where: { $0.position == (isFrontCamera ? .back : .front) }) ?? devices.first else { return }
        isFrontCamera = device.position == .front
        let formats = RTCCameraVideoCapturer.supportedFormats(for: device)
        let format = formats.min { lhs, rhs in
            let l = CMVideoFormatDescriptionGetDimensions(lhs.formatDescription)
            let r = CMVideoFormatDescriptionGetDimensions(rhs.formatDescription)
            return abs(Int(l.width) - 640) + abs(Int(l.height) - 480)
                < abs(Int(r.width) - 640) + abs(Int(r.height) - 480)
        } ?? formats.first
        guard let format = format else { return }
        let fps = format.videoSupportedFrameRateRanges
            .max { $0.maxFrameRate < $1.maxFrameRate }?.maxFrameRate ?? 30
        capturer.startCapture(with: device, format: format, fps: Int(fps))
    }

    // ── 信令消息处理 ──────────────────────────────────────────

    private func handleMessage(_ obj: [String: Any]) {
        guard let type = obj["type"] as? String else { return }
        switch type {
        case "sdp_answer":
            Task { await self.onSdpAnswer(obj) }

        case "ice_candidate":
            guard let pcId = obj["pc_id"] as? String,
                  let entry = pcs[pcId],
                  let candidate = obj["candidate"] as? String else { return }
            let mid = obj["sdp_mid"] as? String ?? "0"
            let ice = RTCIceCandidate(sdp: candidate, sdpMLineIndex: 0, sdpMid: mid)
            Task { try? await entry.pc.add(ice) }

        case "joined":
            if let arr = obj["participants"] as? [[String: Any]] {
                for p in arr {
                    guard let pid = p["participant_id"] as? String else { continue }
                    if pid == participantId || pid.hasPrefix("\(participantId)-recv-") { continue }
                    publisherTracks[pid] = parseTracks(p["tracks"])
                    delegate?.sfu(self, didJoin: pid)
                }
            }

        case "subscribed":
            let pcId = obj["pc_id"] as? String ?? ""
            let pid = pcs[pcId]?.publisherId ?? (obj["publisher_id"] as? String ?? "")
            delegate?.sfu(self, didUpdate: pid, status: .subscribed, hasVideo: nil)
            log("订阅确认: \(pid)", .success)

        case "unsubscribed":
            log("已取消订阅: \(obj["publisher_id"] as? String ?? "")", .info)

        case "participant_joined":
            guard let pid = obj["participant_id"] as? String else { return }
            if pid == participantId || pid.hasPrefix("\(participantId)-recv-") { return }
            publisherTracks[pid] = parseTracks(obj["tracks"])
            log("参与者加入: \(pid)", .info)
            delegate?.sfu(self, didJoin: pid)

        case "participant_tracks":
            guard let pid = obj["participant_id"] as? String else { return }
            if pid == participantId || pid.hasPrefix("\(participantId)-recv-") { return }
            publisherTracks[pid] = parseTracks(obj["tracks"])
            if let pcId = subPcByPublisher[pid] {
                sendSubscribes(pcId: pcId, publisherId: pid)
            } else {
                // 之前因无轨道未订阅, 现在有轨道了 → 发起订阅
                delegate?.sfu(self, didJoin: pid)
            }

        case "participant_updated":
            let pid = obj["participant_id"] as? String ?? ""
            let audio = obj["audio_enabled"] as? Bool ?? true
            let video = obj["video_enabled"] as? Bool ?? true
            log("参与者状态更新: \(pid) (audio=\(audio), video=\(video))", .info)

        case "participant_left":
            guard let pid = obj["participant_id"] as? String else { return }
            log("参与者离开: \(pid)", .warn)
            publisherTracks[pid] = nil
            unsubscribe(pid)
            delegate?.sfu(self, didLeave: pid)

        case "welcome":
            let server = obj["server"] as? String ?? ""
            let version = obj["protocol_version"] as? Int ?? 0
            log("信令握手: \(server) (v\(version))", .info)

        case "left":
            log("连接已关闭: \(obj["pc_id"] as? String ?? "")", .info)

        case "room_state":
            let count = (obj["participants"] as? [[String: Any]])?.count ?? 0
            log("房间快照: \(count) 位成员", .info)

        case "pong":
            break

        case "error":
            let err = obj["error"] as? String ?? "unknown"
            if let pcId = obj["pc_id"] as? String {
                log("服务器错误 [\(pcId)]: \(err)", .error)
            } else {
                log("服务器错误: \(err)", .error)
            }

        default:
            break
        }
    }

    private func onSdpAnswer(_ obj: [String: Any]) async {
        guard let pcId = obj["pc_id"] as? String,
              let sdpString = obj["sdp"] as? String,
              let entry = pcs[pcId] else {
            log("收到未知 pc_id 的 sdp_answer", .warn)
            return
        }

        let sdp = RTCSessionDescription(type: .answer, sdp: sdpString)
        do {
            try await setRemote(entry.pc, sdp)
        } catch {
            log("SDP 设置失败: \(error)", .error)
            return
        }

        let targets: [(String, Int32)] = entry.role == "subscriber"
            ? [("0", 0), ("1", 1)] : [("0", 0)]
        if let candidates = obj["ice_candidates"] as? [String] {
            for candidate in candidates where !candidate.isEmpty {
                for (mid, index) in targets {
                    try? await entry.pc.add(
                        RTCIceCandidate(sdp: candidate, sdpMLineIndex: index, sdpMid: mid)
                    )
                }
            }
        }

        scanReceivers(entry)

        if entry.role == "subscriber", let publisherId = entry.publisherId {
            sendSubscribes(pcId: pcId, publisherId: publisherId)
        } else {
            log("SDP 协商完成, 已添加服务器候选", .success)
        }
    }

    /// 依据发布者已知轨道发送 subscribe: track_mid = 发布者 mid,
    /// subscriber_mid = 本地 recvonly mid (音频=0, 视频=1)。
    /// 可重复调用, 只补发尚未发送的 mid。
    private func sendSubscribes(pcId: String, publisherId: String) {
        guard let tracks = publisherTracks[publisherId] else { return }
        var sent = sentSubscribeMids[publisherId] ?? Set<String>()
        var want: [(String, String)] = []
        if let a = tracks.first(where: { $0.kind == "audio" }) { want.append((a.mid, "0")) }
        if let v = tracks.first(where: { $0.kind == "video" }) { want.append((v.mid, "1")) }
        let fresh = want.filter { !$0.0.isEmpty && !sent.contains($0.0) }
        if fresh.isEmpty { return }
        for (trackMid, subscriberMid) in fresh {
            signaling?.send([
                "type": "subscribe",
                "pc_id": pcId,
                "publisher_id": publisherId,
                "track_mid": trackMid,
                "subscriber_mid": subscriberMid,
            ])
        }
        sent.formUnion(fresh.map { $0.0 })
        sentSubscribeMids[publisherId] = sent
        log("订阅 \(publisherId) SDP 完成, 已发送 \(fresh.count) 条 subscribe", .success)
    }

    /// 解析 `tracks` JSON 数组。
    private func parseTracks(_ any: Any?) -> [TrackInfo] {
        guard let arr = any as? [[String: Any]] else { return [] }
        return arr.compactMap { d in
            guard let mid = d["mid"] as? String, let kind = d["kind"] as? String else { return nil }
            return TrackInfo(mid: mid, kind: kind)
        }
    }

    private func scanReceivers(_ entry: PcEntry) {
        for transceiver in entry.pc.transceivers {
            if let video = transceiver.receiver.track as? RTCVideoTrack {
                attachRemoteVideo(entry: entry, track: video)
            }
        }
    }

    private func attachRemoteVideo(entry: PcEntry, track: RTCVideoTrack) {
        guard let publisherId = entry.publisherId else { return }
        remoteVideoTracks[publisherId] = track
        log("收到 \(publisherId) 的 video 轨道", .success)
        delegate?.sfu(self, didUpdate: publisherId, status: nil, hasVideo: true)
    }

    // ── ICE / PC 辅助 ─────────────────────────────────────────

    private func register(_ pc: RTCPeerConnection, pcId: String, role: String, publisherId: String?) {
        pcs[pcId] = PcEntry(pc: pc, role: role, publisherId: publisherId)
        pcIdsByObject[ObjectIdentifier(pc)] = pcId
    }

    private func unregister(_ pc: RTCPeerConnection) {
        pcIdsByObject[ObjectIdentifier(pc)] = nil
    }

    private func sendJoin(pcId: String, pc: RTCPeerConnection, participantId: String, role: String) {
        guard let sdp = pc.localDescription?.sdp else { return }
        signaling?.send([
            "type": "join",
            "pc_id": pcId,
            "room_id": roomId,
            "participant_id": participantId,
            "sdp": sdp,
            "role": role,
        ])
        readyPcs.insert(pcId)
        if let queued = pendingIce[pcId] {
            for candidate in queued {
                signaling?.send(iceJson(pcId, candidate))
            }
        }
        pendingIce[pcId] = nil
    }

    private func handleLocalCandidate(_ pc: RTCPeerConnection, _ candidate: RTCIceCandidate) {
        guard let pcId = pcIdsByObject[ObjectIdentifier(pc)] else { return }
        if readyPcs.contains(pcId) {
            signaling?.send(iceJson(pcId, candidate))
        } else {
            pendingIce[pcId, default: []].append(candidate)
        }
    }

    private func iceJson(_ pcId: String, _ candidate: RTCIceCandidate) -> [String: Any] {
        [
            "type": "ice_candidate",
            "pc_id": pcId,
            "candidate": candidate.sdp,
            "sdp_mid": candidate.sdpMid ?? "0",
        ]
    }

    private func rtcConfig() -> RTCConfiguration {
        let config = RTCConfiguration()
        config.iceServers = []
        config.sdpSemantics = .unifiedPlan
        config.continualGatheringPolicy = .gatherContinually
        return config
    }

    private func pcConstraints() -> RTCMediaConstraints {
        RTCMediaConstraints(
            mandatoryConstraints: nil,
            optionalConstraints: ["DtlsSrtpKeyAgreement": kRTCMediaConstraintsValueTrue]
        )
    }

    private func configureAudio() {
#if os(iOS) || os(visionOS)
        // RTCAudioSession.setActive 会同步激活 AVAudioSession, 在主线程调用会导致
        // UI 卡顿 (WebRTC 内部会打印 "This method can lead to UI unresponsiveness")。
        // 因此整个音频会话配置挪到后台线程执行。
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            let session = RTCAudioSession.sharedInstance()
            session.lockForConfiguration()
            do {
                try session.setCategory(.playAndRecord)
                try session.setMode(.voiceChat)
                try session.overrideOutputAudioPort(.speaker)
                try session.setActive(true)
            } catch {
                DispatchQueue.main.async {
                    self?.log("音频会话配置失败: \(error)", .warn)
                }
            }
            session.unlockForConfiguration()
        }
#endif
    }

    private func log(_ message: String, _ level: LogLevel) {
        delegate?.sfu(self, didLog: message, level: level)
    }

    // ── 异步封装 ──────────────────────────────────────────────

    private func makeOffer(
        _ pc: RTCPeerConnection,
        constraints: RTCMediaConstraints
    ) async throws -> RTCSessionDescription {
        try await withCheckedThrowingContinuation { cont in
            pc.offer(for: constraints) { sdp, error in
                if let sdp = sdp {
                    cont.resume(returning: sdp)
                } else {
                    cont.resume(throwing: error ?? SfuError.generic("createOffer 失败"))
                }
            }
        }
    }

    private func setLocal(_ pc: RTCPeerConnection, _ sdp: RTCSessionDescription) async throws {
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            pc.setLocalDescription(sdp) { error in
                if let error = error {
                    cont.resume(throwing: error)
                } else {
                    cont.resume()
                }
            }
        }
    }

    private func setRemote(_ pc: RTCPeerConnection, _ sdp: RTCSessionDescription) async throws {
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            pc.setRemoteDescription(sdp) { error in
                if let error = error {
                    cont.resume(throwing: error)
                } else {
                    cont.resume()
                }
            }
        }
    }
}

// MARK: - RTCPeerConnectionDelegate

extension SfuClient: RTCPeerConnectionDelegate {

    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didChange stateChanged: RTCSignalingState) {}

    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didAdd stream: RTCMediaStream) {
        Task { @MainActor in
            guard let pcId = self.pcIdsByObject[ObjectIdentifier(peerConnection)],
                  let entry = self.pcs[pcId] else { return }
            self.scanReceivers(entry)
        }
    }

    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didRemove stream: RTCMediaStream) {}

    nonisolated func peerConnectionShouldNegotiate(_ peerConnection: RTCPeerConnection) {}

    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didChange newState: RTCIceConnectionState) {
        Task { @MainActor in
            self.delegate?.sfu(self, didChangeConnectionState: SfuClient.iceStateString(newState))
        }
    }

    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didChange newState: RTCIceGatheringState) {}

    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didGenerate candidate: RTCIceCandidate) {
        Task { @MainActor in
            self.handleLocalCandidate(peerConnection, candidate)
        }
    }

    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didRemove candidates: [RTCIceCandidate]) {}

    nonisolated func peerConnection(_ peerConnection: RTCPeerConnection, didOpen dataChannel: RTCDataChannel) {}

    private static func iceStateString(_ state: RTCIceConnectionState) -> String {
        switch state {
        case .new: return "new"
        case .checking: return "checking"
        case .connected: return "connected"
        case .completed: return "completed"
        case .failed: return "failed"
        case .disconnected: return "disconnected"
        case .closed: return "closed"
        case .count: return "count"
        @unknown default: return "unknown"
        }
    }
}
