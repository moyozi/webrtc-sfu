import SwiftUI
import WebRTC

struct CallView: View {
    @ObservedObject var viewModel: SfuViewModel
    @State private var showParticipants = false

    private let columns = [GridItem(.adaptive(minimum: 160), spacing: 8)]

    var body: some View {
        VStack(spacing: 0) {
            header
            Divider()
            ScrollView {
                LazyVGrid(columns: columns, spacing: 8) {
                    VideoTileView(
                        title: "我",
                        isLocal: true,
                        track: viewModel.videoEnabled ? viewModel.localVideoTrack : nil,
                        mirror: true,
                        placeholder: viewModel.videoEnabled ? "等待视频..." : "摄像头已关闭"
                    )
                    ForEach(viewModel.participants) { participant in
                        VideoTileView(
                            title: participant.id,
                            isLocal: false,
                            track: participant.hasVideo ? viewModel.remoteVideoTrack(participant.id) : nil,
                            mirror: false,
                            placeholder: placeholder(for: participant)
                        )
                    }
                }
                .padding(8)
            }
            Divider()
            controls
        }
        .sheet(isPresented: $showParticipants) {
            ParticipantSheet(viewModel: viewModel)
        }
    }

    private var header: some View {
        HStack {
            Text("房间 \(viewModel.roomId)")
                .font(.headline)
                .lineLimit(1)
            Text("· \(viewModel.participants.count + 1) 人")
                .font(.subheadline)
                .foregroundStyle(.secondary)
            Spacer()
            Button {
                showParticipants = true
            } label: {
                Image(systemName: "person.2.fill")
            }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 12)
    }

    private var controls: some View {
        HStack(spacing: 16) {
            controlButton(
                systemName: viewModel.audioEnabled ? "mic.fill" : "mic.slash.fill",
                active: viewModel.audioEnabled
            ) {
                viewModel.toggleAudio(!viewModel.audioEnabled)
            }
            controlButton(
                systemName: viewModel.videoEnabled ? "video.fill" : "video.slash.fill",
                active: viewModel.videoEnabled
            ) {
                viewModel.toggleVideo(!viewModel.videoEnabled)
            }
            controlButton(systemName: "arrow.triangle.2.circlepath.camera", active: true) {
                viewModel.switchCamera()
            }
            Button {
                viewModel.leave()
            } label: {
                Label("挂断", systemImage: "phone.down.fill")
                    .padding(.horizontal, 12)
                    .padding(.vertical, 10)
            }
            .buttonStyle(.borderedProminent)
            .tint(.red)
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 10)
    }

    private func controlButton(
        systemName: String,
        active: Bool,
        action: @escaping () -> Void
    ) -> some View {
        Button(action: action) {
            Image(systemName: systemName)
                .font(.system(size: 18))
                .frame(width: 44, height: 44)
                .background(active ? Color(.secondarySystemBackground) : Color.red.opacity(0.2))
                .clipShape(Circle())
        }
        .buttonStyle(.plain)
    }

    private func placeholder(for participant: RemoteParticipant) -> String {
        switch participant.status {
        case .connecting: return "连接中..."
        case .error: return "连接失败"
        case .subscribed: return "等待视频..."
        case .idle: return "未订阅"
        }
    }
}

private struct VideoTileView: View {
    let title: String
    let isLocal: Bool
    let track: RTCVideoTrack?
    let mirror: Bool
    let placeholder: String

    var body: some View {
        ZStack {
            Color.black

            if let track = track {
                RTCTrackView(track: track, mirror: mirror)
            } else {
                Color(white: 0.13)
                Text(placeholder)
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.6))
            }

            VStack {
                Spacer()
                HStack {
                    Text(title)
                        .font(.caption)
                        .foregroundStyle(.white)
                        .lineLimit(1)
                    if isLocal {
                        Text("本地")
                            .font(.caption2)
                            .foregroundStyle(Color.accentColor)
                    }
                    Spacer()
                }
                .padding(8)
                .background(Color.black.opacity(0.45))
            }
        }
        .aspectRatio(16.0 / 9.0, contentMode: .fit)
        .clipShape(RoundedRectangle(cornerRadius: 12))
    }
}

private struct ParticipantSheet: View {
    @ObservedObject var viewModel: SfuViewModel
    @State private var manualId = ""

    var body: some View {
        NavigationStack {
            List {
                Section("参与者") {
                    HStack(spacing: 8) {
                        StatusDot(status: .subscribed)
                        Text("我 (本地)")
                    }
                    ForEach(viewModel.participants) { participant in
                        HStack(spacing: 8) {
                            StatusDot(status: participant.status)
                            Text(participant.id)
                                .lineLimit(1)
                            Spacer()
                            switch participant.status {
                            case .subscribed:
                                Button("取消") { viewModel.unsubscribe(participant.id) }
                                    .buttonStyle(.borderless)
                            case .connecting:
                                Text("连接中...")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            default:
                                Button("订阅") { viewModel.subscribe(participant.id) }
                                    .buttonStyle(.borderless)
                            }
                        }
                    }
                }

                Section("手动订阅") {
                    HStack {
                        TextField("参与者 ID", text: $manualId)
                            .autocorrectionDisabled()
                            .textInputAutocapitalization(.never)
                        Button {
                            let id = manualId.trimmingCharacters(in: .whitespacesAndNewlines)
                            if !id.isEmpty {
                                viewModel.subscribe(id)
                                manualId = ""
                            }
                        } label: {
                            Image(systemName: "paperplane.fill")
                        }
                        .buttonStyle(.borderless)
                    }
                }

                Section("事件日志") {
                    ForEach(viewModel.logs) { log in
                        HStack(alignment: .top, spacing: 6) {
                            Text(log.time)
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                            Text(log.msg)
                                .font(.caption2)
                                .foregroundStyle(color(for: log.level))
                        }
                    }
                }
            }
            .navigationTitle("房间详情")
            .navigationBarTitleDisplayMode(.inline)
        }
    }

    private func color(for level: LogLevel) -> Color {
        switch level {
        case .info: return .accentColor
        case .success: return .green
        case .warn: return .yellow
        case .error: return .red
        }
    }
}

private struct StatusDot: View {
    let status: SubStatus

    var body: some View {
        Circle()
            .fill(color)
            .frame(width: 10, height: 10)
    }

    private var color: Color {
        switch status {
        case .connecting: return .yellow
        case .subscribed: return .green
        case .error: return .red
        case .idle: return .gray.opacity(0.4)
        }
    }
}
