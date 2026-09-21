package com.moyozi.roomchat.ui

import android.app.Application
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.moyozi.roomchat.webrtc.LogEntry
import com.moyozi.roomchat.webrtc.LogLevel
import com.moyozi.roomchat.webrtc.RemoteParticipant
import com.moyozi.roomchat.webrtc.SfuClient
import com.moyozi.roomchat.webrtc.SubStatus
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.webrtc.EglBase
import org.webrtc.VideoTrack
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/**
 * 通话状态持有者 — 桥接 SfuClient 与 Compose UI。
 */
class SfuViewModel(app: Application) : AndroidViewModel(app) {

    private var sfu: SfuClient? = null

    var joining by mutableStateOf(false)
        private set
    var inCall by mutableStateOf(false)
        private set
    var roomId by mutableStateOf("")
        private set
    var participantId by mutableStateOf("")
        private set
    var localVideoTrack by mutableStateOf<VideoTrack?>(null)
        private set
    var participants by mutableStateOf<List<RemoteParticipant>>(emptyList())
        private set
    var logs by mutableStateOf<List<LogEntry>>(emptyList())
        private set
    var audioEnabled by mutableStateOf(true)
        private set
    var videoEnabled by mutableStateOf(true)
        private set

    val eglContext: EglBase.Context?
        get() = sfu?.eglContext

    private val callbacks = object : SfuClient.Callbacks {
        override fun onLog(msg: String, level: LogLevel) = addLog(msg, level)

        override fun onParticipantJoined(id: String) {
            if (participants.none { it.id == id }) {
                participants = participants + RemoteParticipant(id)
            }
            // 自动发起订阅 (与 Web 端一致, 稍等片刻等发布者就绪)
            viewModelScope.launch {
                delay(500)
                sfu?.subscribe(id)
            }
        }

        override fun onParticipantLeft(id: String) {
            participants = participants.filterNot { it.id == id }
        }

        override fun onSubscriptionUpdate(
            publisherId: String,
            status: SubStatus?,
            hasVideo: Boolean?,
        ) {
            val existing = participants.firstOrNull { it.id == publisherId }
            participants = if (existing != null) {
                participants.map { p ->
                    if (p.id == publisherId) p.copy(
                        status = status ?: p.status,
                        hasVideo = hasVideo ?: p.hasVideo,
                    ) else p
                }
            } else {
                participants + RemoteParticipant(
                    id = publisherId,
                    status = status ?: SubStatus.IDLE,
                    hasVideo = hasVideo ?: false,
                )
            }
        }

        override fun onConnectionStateChange(state: String) {
            if (state == "CONNECTED") addLog("WebRTC 连接已建立", LogLevel.SUCCESS)
        }
    }

    fun join(serverUrl: String, room: String, name: String) {
        if (joining) return
        joining = true
        roomId = room
        participantId = name

        // 每次通话使用全新的 SfuClient (leave 后 scope 已取消)
        val client = SfuClient(getApplication(), callbacks)
        sfu = client

        viewModelScope.launch {
            try {
                client.join(serverUrl, room, name)
                localVideoTrack = client.localVideoTrack
                inCall = true
            } catch (e: Exception) {
                addLog("加入失败: ${e.message}", LogLevel.ERROR)
                client.dispose()
                sfu = null
            } finally {
                joining = false
            }
        }
    }

    fun leave() {
        sfu?.dispose()
        sfu = null
        inCall = false
        localVideoTrack = null
        participants = emptyList()
        audioEnabled = true
        videoEnabled = true
        addLog("已离开房间", LogLevel.WARN)
    }

    fun subscribe(publisherId: String) {
        if (participants.none { it.id == publisherId }) {
            participants = participants + RemoteParticipant(publisherId)
        }
        viewModelScope.launch { sfu?.subscribe(publisherId) }
    }

    fun unsubscribe(publisherId: String) {
        sfu?.unsubscribe(publisherId)
    }

    fun toggleAudio(enabled: Boolean) {
        audioEnabled = enabled
        sfu?.toggleAudio(enabled)
    }

    fun toggleVideo(enabled: Boolean) {
        videoEnabled = enabled
        sfu?.toggleVideo(enabled)
    }

    fun switchCamera() {
        sfu?.switchCamera()
    }

    fun remoteVideoTrack(publisherId: String): VideoTrack? =
        sfu?.remoteVideoTrackOrNull(publisherId)

    private fun addLog(msg: String, level: LogLevel) {
        val time = SimpleDateFormat("HH:mm:ss", Locale.getDefault()).format(Date())
        logs = (logs + LogEntry(time, msg, level)).takeLast(200)
    }

    override fun onCleared() {
        super.onCleared()
        sfu?.dispose()
        sfu = null
    }
}
