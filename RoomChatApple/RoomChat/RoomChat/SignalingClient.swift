import Foundation

/// 信令客户端 — WebSocket 封装 (对应 Web 端 RoomChatWeb/src/lib/signaling.ts)
///
/// 与 SFU 信令服务器交换 JSON 文本消息。整个会话只维护一条 WebSocket,
/// 所有 RTCPeerConnection 复用该连接 (通过 pc_id 区分)。
final class SignalingClient: NSObject {

    private let url: URL
    private var session: URLSession?
    private var task: URLSessionWebSocketTask?
    private var openContinuation: CheckedContinuation<Void, Error>?

    /// 收到服务器 JSON 消息 (在 URLSession 回调线程)
    var onMessage: (([String: Any]) -> Void)?
    /// 连接关闭 / 失败
    var onClosed: (() -> Void)?

    init(url: URL) {
        self.url = url
    }

    /// 建立连接, 挂起直到握手完成或失败
    func connect() async throws {
        let configuration = URLSessionConfiguration.default
        let session = URLSession(
            configuration: configuration,
            delegate: self,
            delegateQueue: nil
        )
        self.session = session

        let task = session.webSocketTask(with: url)
        self.task = task

        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            self.openContinuation = cont
            task.resume()
        }

        receiveLoop()
    }

    private func receiveLoop() {
        task?.receive { [weak self] result in
            guard let self = self else { return }
            switch result {
            case .failure:
                self.onClosed?()
            case .success(let message):
                switch message {
                case .string(let text):
                    if let data = text.data(using: .utf8),
                       let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                        self.onMessage?(obj)
                    }
                case .data(let data):
                    if let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                        self.onMessage?(obj)
                    }
                @unknown default:
                    break
                }
                self.receiveLoop()
            }
        }
    }

    func send(_ obj: [String: Any]) {
        guard let data = try? JSONSerialization.data(withJSONObject: obj),
              let text = String(data: data, encoding: .utf8) else { return }
        task?.send(.string(text)) { _ in }
    }

    func close() {
        task?.cancel(with: .normalClosure, reason: nil)
        task = nil
        session?.invalidateAndCancel()
        session = nil
    }
}

extension SignalingClient: URLSessionWebSocketDelegate {

    func urlSession(
        _ session: URLSession,
        webSocketTask: URLSessionWebSocketTask,
        didOpenWithProtocol protocol: String?
    ) {
        openContinuation?.resume()
        openContinuation = nil
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didCompleteWithError error: Error?
    ) {
        if let continuation = openContinuation {
            continuation.resume(throwing: error ?? URLError(.cannotConnectToHost))
            openContinuation = nil
        }
        onClosed?()
    }
}
