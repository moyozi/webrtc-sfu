import SwiftUI

struct JoinView: View {
    @ObservedObject var viewModel: SfuViewModel

    @State private var serverUrl = "ws://192.168.31.112:5174"
    @State private var roomId = "room-1"
    @State private var name = JoinView.randomName()
    @State private var error = ""

    private static func randomName() -> String {
        let chars = Array("abcdefghijklmnopqrstuvwxyz0123456789")
        let suffix = String((0 ..< 6).map { _ in chars.randomElement()! })
        return "user-\(suffix)"
    }

    var body: some View {
        ScrollView {
            VStack(spacing: 20) {
                HStack(spacing: 10) {
                    Image(systemName: "video.fill")
                        .font(.system(size: 32))
                        .foregroundStyle(.tint)
                    VStack(alignment: .leading) {
                        Text("WebRTC SFU")
                            .font(.title.bold())
                        Text("多人视频通话 (iOS)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                }

                VStack(spacing: 14) {
                    LabeledField(title: "信令服务器", text: $serverUrl, placeholder: "ws://host:port")
                    LabeledField(title: "房间号", text: $roomId, placeholder: "room-1")
                    LabeledField(title: "你的昵称", text: $name, placeholder: "输入昵称")
                }

                Button {
                    join()
                } label: {
                    HStack {
                        if viewModel.joining {
                            ProgressView()
                                .tint(.white)
                        } else {
                            Image(systemName: "arrow.right.circle.fill")
                        }
                        Text(viewModel.joining ? "加入中..." : "加入房间")
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 12)
                }
                .buttonStyle(.borderedProminent)
                .disabled(viewModel.joining)

                if !error.isEmpty {
                    Text(error)
                        .font(.footnote)
                        .foregroundStyle(.red)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }

                Spacer(minLength: 8)
                Text("SFU 媒体端口 UDP 5175+ · 信令端口 WS 5174")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
            .padding(24)
        }
    }

    private func join() {
        let trimmedName = name.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedName.isEmpty else {
            error = "请输入昵称"
            return
        }
        error = ""
        viewModel.join(
            serverUrl: serverUrl.trimmingCharacters(in: .whitespacesAndNewlines),
            room: roomId.trimmingCharacters(in: .whitespacesAndNewlines),
            name: trimmedName
        )
    }
}

private struct LabeledField: View {
    let title: String
    @Binding var text: String
    let placeholder: String

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
            TextField(placeholder, text: $text)
                .textInputAutocapitalization(.never)
                .autocorrectionDisabled()
                .padding(10)
                .background(Color(.secondarySystemBackground))
                .clipShape(RoundedRectangle(cornerRadius: 8))
        }
    }
}
