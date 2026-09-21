package com.moyozi.roomchat.webrtc

import android.content.Context
import android.media.AudioManager
import android.os.Handler
import android.os.Looper
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import org.json.JSONArray
import org.json.JSONObject
import org.webrtc.AudioSource
import org.webrtc.AudioTrack
import org.webrtc.Camera1Enumerator
import org.webrtc.Camera2Enumerator
import org.webrtc.CameraEnumerator
import org.webrtc.CameraVideoCapturer
import org.webrtc.DataChannel
import org.webrtc.DefaultVideoDecoderFactory
import org.webrtc.DefaultVideoEncoderFactory
import org.webrtc.EglBase
import org.webrtc.IceCandidate
import org.webrtc.MediaConstraints
import org.webrtc.MediaStream
import org.webrtc.MediaStreamTrack
import org.webrtc.PeerConnection
import org.webrtc.PeerConnectionFactory
import org.webrtc.RtpReceiver
import org.webrtc.RtpTransceiver
import org.webrtc.SdpObserver
import org.webrtc.SessionDescription
import org.webrtc.SurfaceTextureHelper
import org.webrtc.VideoSource
import org.webrtc.VideoTrack
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.math.abs

/**
 * SFU 客户端 — 单条 WebSocket + 多个 RTCPeerConnection
 *
 * 架构与 Web 端 demo/src/lib/sfu.ts 完全对应:
 *   - pc_id = "pub"          发布连接 (sendonly), 加入房间
 *   - pc_id = "sub-<远端id>" 订阅连接 (recvonly), 不加入房间
 *
 * 所有 PC 复用同一条 WebSocket, 服务端按 pc_id 路由。
 * 音频轨道必须最先添加 (mid=0), 视频第二 (mid=1), 以匹配服务端的订阅 mid。
 */
class SfuClient(
    private val appContext: Context,
    private val callbacks: Callbacks,
) {

    interface Callbacks {
        fun onLog(msg: String, level: LogLevel)
        fun onParticipantJoined(id: String)
        fun onParticipantLeft(id: String)
        fun onSubscriptionUpdate(publisherId: String, status: SubStatus?, hasVideo: Boolean?)
        fun onConnectionStateChange(state: String)
    }

    companion object {
        const val PUB_PC_ID = "pub"
        private const val TAG = "SfuClient"
        private const val VIDEO_WIDTH = 640
        private const val VIDEO_HEIGHT = 480
        private const val VIDEO_FPS = 30
    }

    val eglBase: EglBase = EglBase.create()
    val eglContext: EglBase.Context get() = eglBase.eglBaseContext

    private var factory: PeerConnectionFactory? = null
    private var audioSource: AudioSource? = null
    private var videoSource: VideoSource? = null
    var localAudioTrack: AudioTrack? = null
        private set
    var localVideoTrack: VideoTrack? = null
        private set
    private var surfaceTextureHelper: SurfaceTextureHelper? = null
    private var videoCapturer: CameraVideoCapturer? = null
    private var frontCamera = true

    private var sig: SignalingClient? = null
    private var roomId = ""
    private var participantId = ""

    private class PcEntry(
        val pc: PeerConnection,
        val role: String,
        val publisherId: String?,
    )

    private val pcs = ConcurrentHashMap<String, PcEntry>()
    private val subPcByPublisher = ConcurrentHashMap<String, String>()
    private val remoteVideoTracks = ConcurrentHashMap<String, VideoTrack>()
    private val readyPcs: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val pendingIce = ConcurrentHashMap<String, MutableList<IceCandidate>>()

    private val mainHandler = Handler(Looper.getMainLooper())
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main)
    private var disposed = false

    // ── 加入 / 发布 ────────────────────────────────────────────

    suspend fun join(serverUrl: String, room: String, participant: String) {
        roomId = room
        participantId = participant

        initFactory()
        createLocalMedia()

        val s = SignalingClient(serverUrl)
        s.onMessage = { msg -> postMain { handleMessage(msg) } }
        s.onClosed = {
            postMain {
                callbacks.onLog("信令连接断开", LogLevel.WARN)
                callbacks.onConnectionStateChange("disconnected")
            }
        }
        s.connect()
        sig = s

        createPublisher()
    }

    private fun initFactory() {
        if (factory != null) return
        PeerConnectionFactory.initialize(
            PeerConnectionFactory.InitializationOptions.builder(appContext)
                .setEnableInternalTracer(false)
                .createInitializationOptions()
        )
        factory = PeerConnectionFactory.builder()
            .setOptions(PeerConnectionFactory.Options())
            .setVideoEncoderFactory(
                DefaultVideoEncoderFactory(eglBase.eglBaseContext, true, true)
            )
            .setVideoDecoderFactory(
                DefaultVideoDecoderFactory(eglBase.eglBaseContext)
            )
            .createPeerConnectionFactory()
    }

    private fun createLocalMedia() {
        val f = factory ?: return

        audioSource = f.createAudioSource(MediaConstraints())
        localAudioTrack = f.createAudioTrack("audio0", audioSource)

        val enumerator: CameraEnumerator =
            if (Camera2Enumerator.isSupported(appContext)) Camera2Enumerator(appContext)
            else Camera1Enumerator(true)

        frontCamera = true
        val deviceName = enumerator.deviceNames.firstOrNull { enumerator.isFrontFacing(it) }
            ?: enumerator.deviceNames.firstOrNull()
            ?: return
        val capturer = enumerator.createCapturer(deviceName, null)
        if (capturer == null) {
            callbacks.onLog("无法创建摄像头采集器", LogLevel.ERROR)
            return
        }
        videoCapturer = capturer

        surfaceTextureHelper = SurfaceTextureHelper.create("CaptureThread", eglBase.eglBaseContext)
        videoSource = f.createVideoSource(false)
        capturer.initialize(surfaceTextureHelper, appContext, videoSource!!.capturerObserver)

        val format = chooseFormat(enumerator, deviceName)
        capturer.startCapture(format.first, format.second, VIDEO_FPS)
        localVideoTrack = f.createVideoTrack("video0", videoSource)

        callbacks.onLog("本地媒体流已就绪", LogLevel.SUCCESS)
    }

    /** 选择最接近 640x480 的采集格式 */
    private fun chooseFormat(enumerator: CameraEnumerator, deviceName: String): Triple<Int, Int, Int> {
        val formats = enumerator.getSupportedFormats(deviceName) ?: emptyList()
        val best = formats
            .filter { it.width <= 1280 && it.height <= 720 }
            .minByOrNull { abs(it.width - VIDEO_WIDTH) + abs(it.height - VIDEO_HEIGHT) }
            ?: formats.firstOrNull()
        return if (best != null) Triple(best.width, best.height, VIDEO_FPS)
        else Triple(VIDEO_WIDTH, VIDEO_HEIGHT, VIDEO_FPS)
    }

    private suspend fun createPublisher() {
        val f = factory ?: return
        val pc = f.createPeerConnection(rtcConfig(), createObserver(PUB_PC_ID, "publisher", null))
        if (pc == null) {
            callbacks.onLog("创建发布连接失败", LogLevel.ERROR)
            return
        }
        pcs[PUB_PC_ID] = PcEntry(pc, "publisher", null)

        val streamIds = listOf("stream0")
        val audioInit = RtpTransceiver.RtpTransceiverInit(
            RtpTransceiver.RtpTransceiverDirection.SEND_ONLY, streamIds
        )
        val videoInit = RtpTransceiver.RtpTransceiverInit(
            RtpTransceiver.RtpTransceiverDirection.SEND_ONLY, streamIds
        )
        localAudioTrack?.let { pc.addTransceiver(it, audioInit) }
        localVideoTrack?.let { pc.addTransceiver(it, videoInit) }

        try {
            val offer = pc.createOfferSuspend()
            pc.setLocalDescriptionSuspend(offer)
            awaitIceGathering(pc)
            val local = pc.localDescription ?: return
            sig?.send(
                joinMessage(PUB_PC_ID, roomId, participantId, local.description, "publisher")
            )
            flushIce(PUB_PC_ID)
            configureAudio()
            callbacks.onLog("正在加入房间 $roomId ...", LogLevel.INFO)
        } catch (e: Exception) {
            callbacks.onLog("创建发布 offer 失败: ${e.message}", LogLevel.ERROR)
        }
    }

    // ── 订阅 ──────────────────────────────────────────────────

    suspend fun subscribe(publisherId: String) {
        if (subPcByPublisher.containsKey(publisherId)) {
            callbacks.onLog("已订阅 $publisherId", LogLevel.WARN)
            return
        }
        if (sig == null) return
        if (publisherId == participantId || publisherId.contains("-recv-")) {
            callbacks.onLog("忽略非法订阅目标: $publisherId", LogLevel.WARN)
            return
        }
        val f = factory ?: return

        callbacks.onLog("正在订阅 $publisherId ...", LogLevel.INFO)
        callbacks.onSubscriptionUpdate(publisherId, SubStatus.CONNECTING, null)

        val pcId = "sub-$publisherId"
        val pc = f.createPeerConnection(rtcConfig(), createObserver(pcId, "subscriber", publisherId))
        if (pc == null) {
            callbacks.onSubscriptionUpdate(publisherId, SubStatus.ERROR, null)
            return
        }
        pcs[pcId] = PcEntry(pc, "subscriber", publisherId)
        subPcByPublisher[publisherId] = pcId

        // 音频在前 (mid=0), 视频在后 (mid=1), 与服务端发布轨道顺序一致
        pc.addTransceiver(
            MediaStreamTrack.MediaType.MEDIA_TYPE_AUDIO,
            RtpTransceiver.RtpTransceiverInit(RtpTransceiver.RtpTransceiverDirection.RECV_ONLY)
        )
        pc.addTransceiver(
            MediaStreamTrack.MediaType.MEDIA_TYPE_VIDEO,
            RtpTransceiver.RtpTransceiverInit(RtpTransceiver.RtpTransceiverDirection.RECV_ONLY)
        )

        try {
            val offer = pc.createOfferSuspend()
            pc.setLocalDescriptionSuspend(offer)
            awaitIceGathering(pc)
            val local = pc.localDescription ?: return
            sig?.send(
                joinMessage(
                    pcId,
                    roomId,
                    "$participantId-recv-$publisherId",
                    local.description,
                    "subscriber"
                )
            )
            flushIce(pcId)
        } catch (e: Exception) {
            callbacks.onLog("创建订阅 offer 失败: ${e.message}", LogLevel.ERROR)
        }
    }

    fun unsubscribe(publisherId: String) {
        val pcId = subPcByPublisher.remove(publisherId) ?: return
        val entry = pcs.remove(pcId)

        entry?.pc?.transceivers?.forEach { tr ->
            tr.mid?.let {
                sig?.send(unsubscribeMessage(pcId, publisherId, it))
            }
        }
        sig?.send(leaveMessage(pcId))

        entry?.pc?.close()
        entry?.pc?.dispose()
        remoteVideoTracks.remove(publisherId)

        callbacks.onLog("已取消订阅 $publisherId", LogLevel.INFO)
        callbacks.onSubscriptionUpdate(publisherId, SubStatus.IDLE, false)
    }

    // ── 离开 ──────────────────────────────────────────────────

    fun leave() {
        pcs.keys.forEach { sig?.send(leaveMessage(it)) }
        pcs.values.forEach {
            it.pc.close()
            it.pc.dispose()
        }
        pcs.clear()
        subPcByPublisher.clear()
        remoteVideoTracks.clear()
        readyPcs.clear()
        pendingIce.clear()

        stopLocalMedia()

        sig?.close()
        sig = null
        scope.cancel()
    }

    private fun stopLocalMedia() {
        try {
            videoCapturer?.stopCapture()
        } catch (_: InterruptedException) {
            // ignore
        }
        videoCapturer?.dispose()
        videoCapturer = null
        surfaceTextureHelper?.dispose()
        surfaceTextureHelper = null

        localVideoTrack?.dispose()
        localVideoTrack = null
        videoSource?.dispose()
        videoSource = null

        localAudioTrack?.dispose()
        localAudioTrack = null
        audioSource?.dispose()
        audioSource = null
    }

    /** 释放工厂与 EGL (ViewModel 销毁时调用, 之后不可复用) */
    fun dispose() {
        if (disposed) return
        disposed = true
        leave()
        factory?.dispose()
        factory = null
        eglBase.release()
    }

    // ── 媒体控制 ──────────────────────────────────────────────

    fun toggleAudio(enabled: Boolean) {
        localAudioTrack?.setEnabled(enabled)
    }

    fun toggleVideo(enabled: Boolean) {
        localVideoTrack?.setEnabled(enabled)
    }

    fun switchCamera() {
        val capturer = videoCapturer ?: return
        capturer.switchCamera(object : CameraVideoCapturer.CameraSwitchHandler {
            override fun onCameraSwitchDone(isFrontFacing: Boolean) {
                frontCamera = isFrontFacing
            }

            override fun onCameraSwitchError(errorDescription: String?) {
                callbacks.onLog("切换摄像头失败: $errorDescription", LogLevel.WARN)
            }
        })
    }

    // ── 供 UI 绑定渲染 ────────────────────────────────────────

    fun remoteVideoTrackOrNull(publisherId: String): VideoTrack? = remoteVideoTracks[publisherId]

    // ── 信令消息处理 ──────────────────────────────────────────

    private fun handleMessage(msg: JSONObject) {
        scope.launch { handleMessageInternal(msg) }
    }

    private suspend fun handleMessageInternal(msg: JSONObject) {
        when (msg.optString("type")) {
            "sdp_answer" -> onSdpAnswer(msg)
            "ice_candidate" -> {
                val pcId = msg.optString("pc_id")
                val entry = pcs[pcId] ?: return
                val candidate = msg.optString("candidate")
                val mid = if (msg.isNull("sdp_mid")) null else msg.optString("sdp_mid")
                entry.pc.addIceCandidate(IceCandidate(mid ?: "0", 0, candidate))
            }
            "subscribed" -> {
                val pcId = msg.optString("pc_id")
                val entry = pcs[pcId]
                val pid = entry?.publisherId ?: msg.optString("publisher_id")
                callbacks.onSubscriptionUpdate(pid, SubStatus.SUBSCRIBED, null)
                callbacks.onLog("订阅确认: $pid", LogLevel.SUCCESS)
            }
            "participant_joined" -> {
                val pid = msg.optString("participant_id")
                if (pid == participantId || pid.startsWith("$participantId-recv-")) return
                callbacks.onLog("参与者加入: $pid", LogLevel.INFO)
                callbacks.onParticipantJoined(pid)
            }
            "participant_left" -> {
                val pid = msg.optString("participant_id")
                callbacks.onLog("参与者离开: $pid", LogLevel.WARN)
                unsubscribe(pid)
                callbacks.onParticipantLeft(pid)
            }
            "error" -> {
                val err = msg.optString("error")
                val pcId = if (msg.isNull("pc_id")) null else msg.optString("pc_id")
                callbacks.onLog(
                    if (pcId != null) "服务器错误 [$pcId]: $err" else "服务器错误: $err",
                    LogLevel.ERROR
                )
            }
        }
    }

    private suspend fun onSdpAnswer(msg: JSONObject) {
        val pcId = msg.optString("pc_id")
        val sdp = msg.optString("sdp")
        val entry = pcs[pcId] ?: run {
            callbacks.onLog("收到未知 pc_id 的 sdp_answer: $pcId", LogLevel.WARN)
            return
        }

        try {
            entry.pc.setRemoteDescriptionSuspend(
                SessionDescription(SessionDescription.Type.ANSWER, sdp)
            )
        } catch (e: Exception) {
            callbacks.onLog("SDP 设置失败: ${e.message}", LogLevel.ERROR)
            return
        }

        // 服务器是 BUNDLE 单端口候选: 发布连接加到 mid 0; 订阅连接两个 mid 都尝试
        val targets: List<Pair<String, Int>> =
            if (entry.role == "subscriber") listOf("0" to 0, "1" to 1)
            else listOf("0" to 0)

        val arr: JSONArray? = msg.optJSONArray("ice_candidates")
        if (arr != null) {
            for (i in 0 until arr.length()) {
                val c = arr.optString(i)
                if (c.isNullOrEmpty()) continue
                targets.forEach { (mid, idx) ->
                    try {
                        entry.pc.addIceCandidate(IceCandidate(mid, idx, c))
                    } catch (_: Exception) {
                        // 忽略无法添加的候选
                    }
                }
            }
        }

        scanReceivers(entry)

        if (entry.role == "subscriber" && entry.publisherId != null) {
            entry.pc.transceivers.forEach { tr ->
                val mid = tr.mid ?: return@forEach
                sig?.send(subscribeMessage(pcId, entry.publisherId, mid))
            }
            callbacks.onLog(
                "订阅 ${entry.publisherId} SDP 完成, 已发送 subscribe",
                LogLevel.SUCCESS
            )
        } else {
            callbacks.onLog("SDP 协商完成, 已添加服务器候选", LogLevel.SUCCESS)
        }
    }

    private fun scanReceivers(entry: PcEntry) {
        entry.pc.receivers.forEach { receiver ->
            receiver.track()?.let { maybeAttachRemoteTrack(entry, it) }
        }
    }

    private fun maybeAttachRemoteTrack(entry: PcEntry, track: MediaStreamTrack) {
        if (track.kind() != MediaStreamTrack.VIDEO_TRACK_KIND) return
        val publisherId = entry.publisherId ?: return
        val videoTrack = track as? VideoTrack ?: return
        remoteVideoTracks[publisherId] = videoTrack
        postMain {
            callbacks.onLog("收到 $publisherId 的 video 轨道", LogLevel.SUCCESS)
            callbacks.onSubscriptionUpdate(publisherId, null, true)
        }
    }

    // ── ICE / PC 辅助 ─────────────────────────────────────────

    private fun sendIce(pcId: String, candidate: IceCandidate) {
        if (readyPcs.contains(pcId)) {
            sig?.send(iceMessage(pcId, candidate))
        } else {
            pendingIce.computeIfAbsent(pcId) { mutableListOf() }.add(candidate)
        }
    }

    private fun flushIce(pcId: String) {
        readyPcs.add(pcId)
        pendingIce.remove(pcId)?.forEach { sig?.send(iceMessage(pcId, it)) }
    }

    private fun rtcConfig(): PeerConnection.RTCConfiguration =
        PeerConnection.RTCConfiguration(emptyList()).apply {
            sdpSemantics = PeerConnection.SdpSemantics.UNIFIED_PLAN
            bundlePolicy = PeerConnection.BundlePolicy.MAXBUNDLE
            rtcpMuxPolicy = PeerConnection.RtcpMuxPolicy.REQUIRE
            continualGatheringPolicy = PeerConnection.ContinualGatheringPolicy.GATHER_CONTINUALLY
            enableCpuOveruseDetection = false
        }

    private fun createObserver(
        pcId: String,
        role: String,
        publisherId: String?,
    ): PeerConnection.Observer = object : PeerConnection.Observer {
        override fun onSignalingChange(state: PeerConnection.SignalingState?) = Unit

        override fun onIceConnectionChange(state: PeerConnection.IceConnectionState?) {
            postMain { callbacks.onConnectionStateChange(state?.name ?: "unknown") }
        }

        override fun onConnectionChange(newState: PeerConnection.PeerConnectionState?) {
            postMain { callbacks.onConnectionStateChange(newState?.name ?: "unknown") }
        }

        override fun onIceConnectionReceivingChange(receiving: Boolean) = Unit

        override fun onIceGatheringChange(state: PeerConnection.IceGatheringState?) = Unit

        override fun onIceCandidate(candidate: IceCandidate?) {
            candidate?.let { sendIce(pcId, it) }
        }

        override fun onIceCandidatesRemoved(candidates: Array<out IceCandidate>?) = Unit

        override fun onAddStream(stream: MediaStream?) = Unit

        override fun onRemoveStream(stream: MediaStream?) = Unit

        override fun onDataChannel(channel: DataChannel?) = Unit

        override fun onRenegotiationNeeded() = Unit

        override fun onAddTrack(receiver: RtpReceiver?, mediaStreams: Array<out MediaStream>?) {
            val entry = pcs[pcId] ?: return
            receiver?.track()?.let { maybeAttachRemoteTrack(entry, it) }
        }

        override fun onTrack(transceiver: RtpTransceiver?) {
            val entry = pcs[pcId] ?: return
            transceiver?.receiver?.track()?.let { maybeAttachRemoteTrack(entry, it) }
        }
    }

    private fun configureAudio() {
        try {
            val am = appContext.getSystemService(Context.AUDIO_SERVICE) as AudioManager
            am.mode = AudioManager.MODE_IN_COMMUNICATION
            am.isSpeakerphoneOn = true
        } catch (_: Exception) {
            // 某些设备可能不支持, 忽略
        }
    }

    // ── 协程封装 ──────────────────────────────────────────────

    private open class SdpObserverAdapter : SdpObserver {
        override fun onCreateSuccess(sdp: SessionDescription?) = Unit
        override fun onSetSuccess() = Unit
        override fun onCreateFailure(s: String?) = Unit
        override fun onSetFailure(s: String?) = Unit
    }

    private suspend fun PeerConnection.createOfferSuspend(): SessionDescription =
        suspendCancellableCoroutine { cont ->
            createOffer(object : SdpObserverAdapter() {
                override fun onCreateSuccess(sdp: SessionDescription?) {
                    if (sdp != null) cont.resume(sdp)
                    else cont.resumeWithException(RuntimeException("createOffer 返回空 SDP"))
                }

                override fun onCreateFailure(s: String?) {
                    cont.resumeWithException(RuntimeException("createOffer 失败: $s"))
                }
            }, MediaConstraints())
        }

    private suspend fun PeerConnection.setLocalDescriptionSuspend(sdp: SessionDescription) =
        suspendCancellableCoroutine { cont ->
            setLocalDescription(object : SdpObserverAdapter() {
                override fun onSetSuccess() {
                    cont.resume(Unit)
                }

                override fun onSetFailure(s: String?) {
                    cont.resumeWithException(RuntimeException("setLocalDescription 失败: $s"))
                }
            }, sdp)
        }

    private suspend fun PeerConnection.setRemoteDescriptionSuspend(sdp: SessionDescription) =
        suspendCancellableCoroutine { cont ->
            setRemoteDescription(object : SdpObserverAdapter() {
                override fun onSetSuccess() {
                    cont.resume(Unit)
                }

                override fun onSetFailure(s: String?) {
                    cont.resumeWithException(RuntimeException("setRemoteDescription 失败: $s"))
                }
            }, sdp)
        }

    private suspend fun awaitIceGathering(pc: PeerConnection, timeoutMs: Long = 2000) {
        val start = System.currentTimeMillis()
        while (pc.iceGatheringState() != PeerConnection.IceGatheringState.COMPLETE) {
            if (System.currentTimeMillis() - start > timeoutMs) break
            delay(50)
        }
    }

    // ── JSON 构造 ─────────────────────────────────────────────

    private fun joinMessage(
        pcId: String,
        roomId: String,
        participantId: String,
        sdp: String,
        role: String,
    ): JSONObject = JSONObject().apply {
        put("type", "join")
        put("pc_id", pcId)
        put("room_id", roomId)
        put("participant_id", participantId)
        put("sdp", sdp)
        put("role", role)
    }

    private fun iceMessage(pcId: String, c: IceCandidate): JSONObject = JSONObject().apply {
        put("type", "ice_candidate")
        put("pc_id", pcId)
        put("candidate", c.sdp)
        put("sdp_mid", c.sdpMid)
        put("sdp_mline_index", c.sdpMLineIndex)
    }

    private fun subscribeMessage(pcId: String, publisherId: String, mid: String): JSONObject =
        JSONObject().apply {
            put("type", "subscribe")
            put("pc_id", pcId)
            put("publisher_id", publisherId)
            put("track_mid", mid)
        }

    private fun unsubscribeMessage(pcId: String, publisherId: String, mid: String): JSONObject =
        JSONObject().apply {
            put("type", "unsubscribe")
            put("pc_id", pcId)
            put("publisher_id", publisherId)
            put("track_mid", mid)
        }

    private fun leaveMessage(pcId: String): JSONObject = JSONObject().apply {
        put("type", "leave")
        put("pc_id", pcId)
    }

    private fun postMain(block: () -> Unit) {
        if (Looper.myLooper() == Looper.getMainLooper()) block()
        else mainHandler.post(block)
    }
}
