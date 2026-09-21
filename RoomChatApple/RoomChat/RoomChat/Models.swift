import Foundation

/// 日志级别 (与 Web 端 / Android 端一致)
enum LogLevel {
    case info
    case success
    case warn
    case error
}

/// 远端订阅状态
enum SubStatus {
    case idle
    case connecting
    case subscribed
    case error
}

/// 一条事件日志
struct LogEntry: Identifiable {
    let id = UUID()
    let time: String
    let msg: String
    let level: LogLevel
}

/// UI 中展示的远端参与者
struct RemoteParticipant: Identifiable {
    let id: String
    var status: SubStatus = .idle
    var hasVideo: Bool = false
}
