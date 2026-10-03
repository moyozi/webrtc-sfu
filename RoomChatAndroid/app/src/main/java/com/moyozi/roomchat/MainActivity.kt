package com.moyozi.roomchat

import android.Manifest
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.view.WindowManager
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.ui.platform.LocalContext
import androidx.core.content.ContextCompat
import androidx.lifecycle.viewmodel.compose.viewModel
import com.moyozi.roomchat.ui.CallScreen
import com.moyozi.roomchat.ui.JoinScreen
import com.moyozi.roomchat.ui.SfuViewModel
import com.moyozi.roomchat.ui.theme.RoomChatTheme

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        // 通话期间保持屏幕常亮
        window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
        setContent {
            RoomChatTheme {
                RoomChatApp()
            }
        }
    }
}

@Composable
fun RoomChatApp(vm: SfuViewModel = viewModel()) {
    val context = LocalContext.current

    // Android 13+ 需要通知权限才能显示前台服务通知
    val notificationLauncher = rememberLauncherForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { /* 即使被拒绝也继续, 前台服务仍会尝试启动 */ }

    // 进入通话 -> 启动前台服务 (进程常驻, 退到后台/锁屏不被回收)
    // 离开通话 -> 停止前台服务
    LaunchedEffect(vm.inCall) {
        if (vm.inCall) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
                ContextCompat.checkSelfPermission(
                    context,
                    Manifest.permission.POST_NOTIFICATIONS,
                ) != PackageManager.PERMISSION_GRANTED
            ) {
                notificationLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
            }
            CallService.start(context)
        } else {
            CallService.stop(context)
        }
    }

    if (vm.inCall) {
        CallScreen(vm = vm)
    } else {
        JoinScreen(
            joining = vm.joining,
            onJoin = { serverUrl, roomId, name -> vm.join(serverUrl, roomId, name) },
        )
    }
}
