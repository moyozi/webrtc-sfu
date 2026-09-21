import Foundation
import Combine
import WebRTC

/// 通话状态持有者 — 桥接 SfuClient 与 SwiftUI。
@MainActor
final class SfuViewModel: ObservableObject {

    @Published var inCall = false
    @Published var joining = false
    @Published var roomId = ""
    @Published var participantId = ""
    @Published var participants: [RemoteParticipant] = []
    @Published var logs: [LogEntry] = []
    @Published var audioEnabled = true
    @Published var videoEnabled = true
    @Published private(set) var localVideoTrack: RTCVideoTrack?

    private var sfu: SfuClient?

    private static let timeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        return formatter
    }()

    func join(serverUrl: String, room: String, name: String) {
        guard !joining else { return }
        joining = true
        roomId = room
        participantId = name
        addLog("正在获取摄像头和麦克风...", .info)

        let client = SfuClient()
        client.delegate = self
        sfu = client

        Task {
            do {
                try await client.join(serverUrl: serverUrl, roomId: room, participantId: name)
                localVideoTrack = client.localVideoTrack
                inCall = true
            } catch {
                addLog("加入失败: \(error)", .error)
                client.dispose()
                sfu = nil
            }
            joining = false
        }
    }

    func leave() {
        sfu?.dispose()
        sfu = nil
        inCall = false
        localVideoTrack = nil
        participants = []
        audioEnabled = true
        videoEnabled = true
        addLog("已离开房间", .warn)
    }

    func subscribe(_ publisherId: String) {
        if !participants.contains(where: { $0.id == publisherId }) {
            participants.append(RemoteParticipant(id: publisherId))
        }
        sfu?.subscribe(publisherId)
    }

    func unsubscribe(_ publisherId: String) {
        sfu?.unsubscribe(publisherId)
    }

    func toggleAudio(_ enabled: Bool) {
        audioEnabled = enabled
        sfu?.toggleAudio(enabled)
    }

    func toggleVideo(_ enabled: Bool) {
        videoEnabled = enabled
        sfu?.toggleVideo(enabled)
    }

    func switchCamera() {
        sfu?.switchCamera()
    }

    func remoteVideoTrack(_ publisherId: String) -> RTCVideoTrack? {
        sfu?.remoteVideoTracks[publisherId]
    }

    private func addLog(_ message: String, _ level: LogLevel) {
        let time = SfuViewModel.timeFormatter.string(from: Date())
        logs.append(LogEntry(time: time, msg: message, level: level))
        if logs.count > 200 {
            logs.removeFirst(logs.count - 200)
        }
    }
}

extension SfuViewModel: SfuClientDelegate {

    func sfu(_ client: SfuClient, didLog message: String, level: LogLevel) {
        addLog(message, level)
    }

    func sfu(_ client: SfuClient, didJoin participantId: String) {
        if !participants.contains(where: { $0.id == participantId }) {
            participants.append(RemoteParticipant(id: participantId))
        }
        Task {
            try? await Task.sleep(nanoseconds: 500_000_000)
            client.subscribe(participantId)
        }
    }

    func sfu(_ client: SfuClient, didLeave participantId: String) {
        participants.removeAll { $0.id == participantId }
    }

    func sfu(_ client: SfuClient, didUpdate publisherId: String, status: SubStatus?, hasVideo: Bool?) {
        if let index = participants.firstIndex(where: { $0.id == publisherId }) {
            if let status = status { participants[index].status = status }
            if let hasVideo = hasVideo { participants[index].hasVideo = hasVideo }
        } else {
            participants.append(RemoteParticipant(
                id: publisherId,
                status: status ?? .idle,
                hasVideo: hasVideo ?? false
            ))
        }
    }

    func sfu(_ client: SfuClient, didChangeConnectionState state: String) {
        if state == "connected" || state == "completed" {
            addLog("WebRTC 连接已建立", .success)
        }
    }
}
