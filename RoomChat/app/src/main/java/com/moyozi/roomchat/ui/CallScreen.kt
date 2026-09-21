package com.moyozi.roomchat.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.Send
import androidx.compose.material.icons.filled.CallEnd
import androidx.compose.material.icons.filled.Cameraswitch
import androidx.compose.material.icons.filled.Mic
import androidx.compose.material.icons.filled.MicOff
import androidx.compose.material.icons.filled.People
import androidx.compose.material.icons.filled.Videocam
import androidx.compose.material.icons.filled.VideocamOff
import androidx.compose.material3.Button
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FilledTonalIconButton
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.rememberModalBottomSheetState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import com.moyozi.roomchat.webrtc.LogEntry
import com.moyozi.roomchat.webrtc.LogLevel
import com.moyozi.roomchat.webrtc.RemoteParticipant
import com.moyozi.roomchat.webrtc.SubStatus
import org.webrtc.EglBase
import org.webrtc.VideoTrack

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun CallScreen(vm: SfuViewModel) {
    var showSheet by remember { mutableStateOf(false) }

    Scaffold(
        topBar = {
            TopAppBar(
                title = {
                    Text(
                        "房间 ${vm.roomId} · ${vm.participants.size + 1} 人",
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                    )
                },
                actions = {
                    IconButton(onClick = { showSheet = true }) {
                        Icon(Icons.Default.People, contentDescription = "参与者")
                    }
                },
            )
        },
        bottomBar = {
            ControlBar(
                audioEnabled = vm.audioEnabled,
                videoEnabled = vm.videoEnabled,
                onToggleAudio = { vm.toggleAudio(!vm.audioEnabled) },
                onToggleVideo = { vm.toggleVideo(!vm.videoEnabled) },
                onSwitchCamera = { vm.switchCamera() },
                onLeave = { vm.leave() },
            )
        },
    ) { padding ->
        VideoGrid(vm = vm, modifier = Modifier.padding(padding))
    }

    if (showSheet) {
        val sheetState = rememberModalBottomSheetState()
        ModalBottomSheet(
            onDismissRequest = { showSheet = false },
            sheetState = sheetState,
        ) {
            ParticipantPanel(vm = vm)
        }
    }
}

@Composable
private fun VideoGrid(vm: SfuViewModel, modifier: Modifier = Modifier) {
    val egl = vm.eglContext
    LazyVerticalGrid(
        columns = GridCells.Adaptive(minSize = 160.dp),
        contentPadding = PaddingValues(8.dp),
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
        modifier = modifier.fillMaxSize(),
    ) {
        item(key = "__local__") {
            VideoTile(
                title = "我",
                isLocal = true,
                track = if (vm.videoEnabled) vm.localVideoTrack else null,
                eglContext = egl,
                mirror = true,
                placeholder = if (vm.videoEnabled) "等待视频..." else "摄像头已关闭",
            )
        }
        items(vm.participants, key = { it.id }) { p ->
            VideoTile(
                title = p.id,
                isLocal = false,
                track = if (p.hasVideo) vm.remoteVideoTrack(p.id) else null,
                eglContext = egl,
                mirror = false,
                placeholder = when (p.status) {
                    SubStatus.CONNECTING -> "连接中..."
                    SubStatus.ERROR -> "连接失败"
                    SubStatus.SUBSCRIBED -> "等待视频..."
                    SubStatus.IDLE -> "未订阅"
                },
            )
        }
    }
}

@Composable
private fun VideoTile(
    title: String,
    isLocal: Boolean,
    track: VideoTrack?,
    eglContext: EglBase.Context?,
    mirror: Boolean,
    placeholder: String,
) {
    Box(
        modifier = Modifier
            .fillMaxWidth()
            .aspectRatio(16f / 9f)
            .clip(RoundedCornerShape(12.dp))
            .background(Color.Black),
    ) {
        if (eglContext != null) {
            WebRtcVideoView(
                eglContext = eglContext,
                track = track,
                mirror = mirror,
                modifier = Modifier.fillMaxSize(),
            )
        }

        if (track == null) {
            Box(
                modifier = Modifier.fillMaxSize().background(Color(0xFF202124)),
                contentAlignment = Alignment.Center,
            ) {
                Text(
                    placeholder,
                    color = Color.White.copy(alpha = 0.6f),
                    style = MaterialTheme.typography.bodySmall,
                )
            }
        }

        Box(
            modifier = Modifier
                .align(Alignment.BottomStart)
                .fillMaxWidth()
                .background(Color.Black.copy(alpha = 0.45f))
                .padding(horizontal = 10.dp, vertical = 6.dp),
        ) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(
                    title,
                    color = Color.White,
                    style = MaterialTheme.typography.labelMedium,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                    modifier = Modifier.weight(1f, fill = false),
                )
                if (isLocal) {
                    Spacer(Modifier.width(6.dp))
                    Text(
                        "本地",
                        color = MaterialTheme.colorScheme.primary,
                        style = MaterialTheme.typography.labelSmall,
                    )
                }
            }
        }
    }
}

@Composable
private fun ControlBar(
    audioEnabled: Boolean,
    videoEnabled: Boolean,
    onToggleAudio: () -> Unit,
    onToggleVideo: () -> Unit,
    onSwitchCamera: () -> Unit,
    onLeave: () -> Unit,
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 16.dp, vertical = 10.dp),
        horizontalArrangement = Arrangement.spacedBy(14.dp, Alignment.CenterHorizontally),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        FilledTonalIconButton(onClick = onToggleAudio) {
            Icon(
                if (audioEnabled) Icons.Default.Mic else Icons.Default.MicOff,
                contentDescription = "麦克风",
            )
        }
        FilledTonalIconButton(onClick = onToggleVideo) {
            Icon(
                if (videoEnabled) Icons.Default.Videocam else Icons.Default.VideocamOff,
                contentDescription = "摄像头",
            )
        }
        FilledTonalIconButton(onClick = onSwitchCamera) {
            Icon(Icons.Default.Cameraswitch, contentDescription = "切换摄像头")
        }
        Button(
            onClick = onLeave,
            colors = androidx.compose.material3.ButtonDefaults.buttonColors(
                containerColor = MaterialTheme.colorScheme.error,
                contentColor = MaterialTheme.colorScheme.onError,
            ),
        ) {
            Icon(Icons.Default.CallEnd, contentDescription = null)
            Spacer(Modifier.width(6.dp))
            Text("挂断")
        }
    }
}

@Composable
private fun ParticipantPanel(vm: SfuViewModel) {
    var manualId by remember { mutableStateOf("") }

    Column(modifier = Modifier.fillMaxWidth().padding(horizontal = 20.dp)) {
        Text("参与者", style = MaterialTheme.typography.titleSmall)
        Spacer(Modifier.height(8.dp))

        Row(verticalAlignment = Alignment.CenterVertically) {
            StatusDot(SubStatus.SUBSCRIBED)
            Spacer(Modifier.width(8.dp))
            Text("我 (本地)", modifier = Modifier.weight(1f))
        }

        vm.participants.forEach { p ->
            Spacer(Modifier.height(6.dp))
            Row(verticalAlignment = Alignment.CenterVertically) {
                StatusDot(p.status)
                Spacer(Modifier.width(8.dp))
                Text(
                    p.id,
                    modifier = Modifier.weight(1f),
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                )
                when (p.status) {
                    SubStatus.SUBSCRIBED -> TextButton(
                        onClick = { vm.unsubscribe(p.id) }
                    ) { Text("取消") }
                    SubStatus.CONNECTING -> Text("连接中...", style = MaterialTheme.typography.labelSmall)
                    else -> TextButton(
                        onClick = { vm.subscribe(p.id) }
                    ) { Text("订阅") }
                }
            }
        }

        Spacer(Modifier.height(16.dp))
        Text("手动订阅", style = MaterialTheme.typography.titleSmall)
        Spacer(Modifier.height(6.dp))
        Row(verticalAlignment = Alignment.CenterVertically) {
            OutlinedTextField(
                value = manualId,
                onValueChange = { manualId = it },
                label = { Text("参与者 ID") },
                singleLine = true,
                modifier = Modifier.weight(1f),
            )
            Spacer(Modifier.width(8.dp))
            IconButton(
                onClick = {
                    val id = manualId.trim()
                    if (id.isNotEmpty()) {
                        vm.subscribe(id)
                        manualId = ""
                    }
                }
            ) {
                Icon(Icons.AutoMirrored.Filled.Send, contentDescription = "订阅")
            }
        }

        Spacer(Modifier.height(16.dp))
        Text("事件日志", style = MaterialTheme.typography.titleSmall)
        Spacer(Modifier.height(6.dp))
        LazyColumn(
            modifier = Modifier
                .fillMaxWidth()
                .height(200.dp)
        ) {
            items(vm.logs) { log -> LogRow(log) }
        }
    }
}

@Composable
private fun LogRow(log: LogEntry) {
    val color = when (log.level) {
        LogLevel.INFO -> MaterialTheme.colorScheme.primary
        LogLevel.SUCCESS -> Color(0xFF2E7D32)
        LogLevel.WARN -> Color(0xFFF9A825)
        LogLevel.ERROR -> MaterialTheme.colorScheme.error
    }
    Row(modifier = Modifier.padding(vertical = 1.dp)) {
        Text(
            log.time,
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Spacer(Modifier.width(6.dp))
        Text(log.msg, style = MaterialTheme.typography.labelSmall, color = color)
    }
}

@Composable
private fun StatusDot(status: SubStatus) {
    val color = when (status) {
        SubStatus.CONNECTING -> Color(0xFFF9A825)
        SubStatus.SUBSCRIBED -> Color(0xFF2E7D32)
        SubStatus.ERROR -> MaterialTheme.colorScheme.error
        SubStatus.IDLE -> MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.3f)
    }
    Box(
        modifier = Modifier
            .size(10.dp)
            .clip(CircleShape)
            .background(color)
    )
}
