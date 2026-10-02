package com.moyozi.roomchat.webrtc

/** 日志级别 (与 Web 端一致) */
enum class LogLevel { INFO, SUCCESS, WARN, ERROR }

/** 远端订阅状态 */
enum class SubStatus { IDLE, CONNECTING, SUBSCRIBED, ERROR }

/** 发布者的一条媒体轨道 (mid + kind), 与信令协议 `TrackInfo` 对应。 */
data class TrackInfo(
    val mid: String,
    val kind: String,
)

/** 房间成员信息 (成员快照 / 广播共用), 与信令协议 `ParticipantInfo` 对应。 */
data class ParticipantInfo(
    val id: String,
    val audioEnabled: Boolean,
    val videoEnabled: Boolean,
    val tracks: List<TrackInfo>,
)

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
