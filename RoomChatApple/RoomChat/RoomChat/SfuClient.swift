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

/// SFU 客户端 — 单条 WebSocket + 多个 RTCPeerConnection
///
/// 架构与 Web 端 demo/src/lib/sfu.ts 完全对应:
///   - pc_id = "pub"          发布连接 (sendonly), 加入房间
///   - pc_id = "sub-<远端id>" 订阅连接 (recvonly), 不加入房间
///
/// 所有 PC 复用同一条 WebSocket, 服务端按 pc_id 路由。
/// 音频轨道必须最先添加 (mid=0), 视频第二 (mid=1), 以匹配服务端的订阅 mid。
@MainActor
final class SfuClient: NSObject {

    static let pubPcId = "pub"

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

    // ── 加入 / 发布 ────────────────────────────────────────────

    func join(serverUrl: String, roomId: String, participantId: String) async throws {
        self.roomId = roomId
        self.participantId = participantId

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

        try await createPublisher()
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

        startCapture()
        log("本地媒体流已就绪", .success)
    }

    private func startCapture() {
        guard let capturer = capturer as? RTCCameraVideoCapturer else { return }
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
        guard let format = format else { return }
        let fps = format.videoSupportedFrameRateRanges
            .max { $0.maxFrameRate < $1.maxFrameRate }?.maxFrameRate ?? 30
        capturer.startCapture(with: device, format: format, fps: Int(fps))
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

        for transceiver in entry.pc.transceivers {
            let mid = transceiver.mid
            if !mid.isEmpty {
                signaling?.send([
                    "type": "unsubscribe",
                    "pc_id": pcId,
                    "publisher_id": publisherId,
                    "track_mid": mid,
                ])
            }
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

        case "subscribed":
            let pcId = obj["pc_id"] as? String ?? ""
            let pid = pcs[pcId]?.publisherId ?? (obj["publisher_id"] as? String ?? "")
            delegate?.sfu(self, didUpdate: pid, status: .subscribed, hasVideo: nil)
            log("订阅确认: \(pid)", .success)

        case "participant_joined":
            guard let pid = obj["participant_id"] as? String else { return }
            if pid == participantId || pid.hasPrefix("\(participantId)-recv-") { return }
            log("参与者加入: \(pid)", .info)
            delegate?.sfu(self, didJoin: pid)

        case "participant_left":
            guard let pid = obj["participant_id"] as? String else { return }
            log("参与者离开: \(pid)", .warn)
            unsubscribe(pid)
            delegate?.sfu(self, didLeave: pid)

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
            for transceiver in entry.pc.transceivers {
                let mid = transceiver.mid
                if !mid.isEmpty {
                    signaling?.send([
                        "type": "subscribe",
                        "pc_id": pcId,
                        "publisher_id": publisherId,
                        "track_mid": mid,
                    ])
                }
            }
            log("订阅 \(publisherId) SDP 完成, 已发送 subscribe", .success)
        } else {
            log("SDP 协商完成, 已添加服务器候选", .success)
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
            "sdp_mline_index": Int(candidate.sdpMLineIndex),
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
        let session = RTCAudioSession.sharedInstance()
        session.lockForConfiguration()
        do {
            try session.setCategory(.playAndRecord)
            try session.setMode(.voiceChat)
            try session.overrideOutputAudioPort(.speaker)
            try session.setActive(true)
        } catch {
            log("音频会话配置失败: \(error)", .warn)
        }
        session.unlockForConfiguration()
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
