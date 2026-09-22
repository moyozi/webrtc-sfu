package com.moyozi.roomchat.ui

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
import org.webrtc.EglBase
import org.webrtc.RendererCommon
import org.webrtc.SurfaceViewRenderer
import org.webrtc.VideoTrack

/**
 * 将 WebRTC VideoTrack 渲染到 Compose 中的 SurfaceViewRenderer。
 *
 * track 变化时自动 removeSink/addSink, 离开组合时释放渲染器。
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

    DisposableEffect(renderer) {
        onDispose {
            boundTrack[0]?.removeSink(renderer)
            boundTrack[0] = null
            renderer.release()
        }
    }
}

/** 无视频时的占位背景 */
@Composable
fun VideoPlaceholder(modifier: Modifier = Modifier) {
    Box(modifier = modifier.fillMaxSize().background(Color(0xFF202124)))
}
