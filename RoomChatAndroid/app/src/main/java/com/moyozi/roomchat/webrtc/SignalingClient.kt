package com.moyozi.roomchat.webrtc

import kotlinx.coroutines.CompletableDeferred
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject
import java.util.concurrent.TimeUnit

/**
 * 信令客户端 — WebSocket 封装 (对应 Web 端 RoomChatWeb/src/lib/signaling.ts)
 *
 * 与 SFU 信令服务器交换 JSON 文本消息。整个会话只维护一条 WebSocket,
 * 所有 RTCPeerConnection 复用该连接 (通过 pc_id 区分)。
 */
class SignalingClient(private val url: String) {

    private val client = OkHttpClient.Builder()
        .pingInterval(20, TimeUnit.SECONDS)
        .build()

    private var ws: WebSocket? = null
    private val opened = CompletableDeferred<Unit>()

    /** 收到服务器文本消息时回调 (在 OkHttp 线程) */
    var onMessage: ((JSONObject) -> Unit)? = null

    /** 连接关闭时回调 */
    var onClosed: (() -> Unit)? = null

    /** 建立连接, 挂起直到握手完成或失败 */
    suspend fun connect() {
        val request = Request.Builder().url(url).build()
        ws = client.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                if (!opened.isCompleted) opened.complete(Unit)
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                try {
                    onMessage?.invoke(JSONObject(text))
                } catch (_: Exception) {
                    // 忽略无法解析的消息
                }
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                if (!opened.isCompleted) opened.completeExceptionally(t)
                onClosed?.invoke()
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                onClosed?.invoke()
            }
        })
        opened.await()
    }

    /** 发送一条 JSON 消息 */
    fun send(msg: JSONObject) {
        ws?.send(msg.toString())
    }

    fun close() {
        ws?.close(1000, null)
        ws = null
        client.dispatcher.executorService.shutdown()
    }
}
