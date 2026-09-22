package com.moyozi.roomchat.webrtc

/** 日志级别 (与 Web 端一致) */
enum class LogLevel { INFO, SUCCESS, WARN, ERROR }

/** 远端订阅状态 */
enum class SubStatus { IDLE, CONNECTING, SUBSCRIBED, ERROR }

/** 一条事件日志 */
data class LogEntry(
    val time: String,
    val msg: String,
    val level: LogLevel,
)

/** UI 中展示的远端参与者 */
data class RemoteParticipant(
    val id: String,
    val status: SubStatus = SubStatus.IDLE,
    val hasVideo: Boolean = false,
)
