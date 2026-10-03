package com.moyozi.roomchat.ui

import android.content.Context
import android.content.ContextWrapper
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.viewinterop.AndroidView
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.LifecycleOwner
import org.webrtc.EglBase
import org.webrtc.RendererCommon
import org.webrtc.SurfaceViewRenderer
import org.webrtc.VideoTrack

/**
 * 将 WebRTC VideoTrack 渲染到 Compose 中的 SurfaceViewRenderer。
 *
 * track 变化时自动 removeSink/addSink; 应用从后台回到前台时重新挂载 sink,
 * 避免渲染器/EGL 上下文重建导致的画面黑屏。离开组合时释放渲染器。
 */
@Composable
fun WebRtcVideoView(
    eglContext: EglBase.Context,
    track: VideoTrack?,
    mirror: Boolean,
    modifier: Modifier = Modifier,
) {
    val boundTrack = remember { arrayOfNulls<VideoTrack>(1) }
    val context = LocalContext.current

    val renderer = remember(eglContext) {
        SurfaceViewRenderer(context).apply {
            init(eglContext, null)
            setScalingType(RendererCommon.ScalingType.SCALE_ASPECT_FILL)
            setEnableHardwareScaler(true)
            setMirror(mirror)
        }
    }

    AndroidView(
        factory = { renderer },
        modifier = modifier.background(Color.Black),
        update = { r ->
            r.setMirror(mirror)
            val current = boundTrack[0]
            if (current !== track) {
                current?.removeSink(r)
                track?.addSink(r)
                boundTrack[0] = track
            }
        },
    )

    val lifecycleOwner = remember(context) { context.findLifecycleOwner() }
    DisposableEffect(lifecycleOwner, renderer) {
        val observer = LifecycleEventObserver { _, event ->
            if (event == Lifecycle.Event.ON_START) {
                // 回到前台后重新挂载, 强制刷新一帧
                boundTrack[0]?.let { t ->
                    t.removeSink(renderer)
                    t.addSink(renderer)
                }
            }
        }
        lifecycleOwner?.lifecycle?.addObserver(observer)
        onDispose {
            lifecycleOwner?.lifecycle?.removeObserver(observer)
            boundTrack[0]?.removeSink(renderer)
            boundTrack[0] = null
            renderer.release()
        }
    }
}

private fun Context.findLifecycleOwner(): LifecycleOwner? {
    var ctx: Context? = this
    while (ctx is ContextWrapper) {
        if (ctx is LifecycleOwner) {
            return ctx
        }
        ctx = ctx.baseContext
    }
    return ctx as? LifecycleOwner
}

/** 无视频时的占位背景 */
@Composable
fun VideoPlaceholder(modifier: Modifier = Modifier) {
    Box(modifier = modifier.fillMaxSize().background(Color(0xFF202124)))
}
