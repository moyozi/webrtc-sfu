package com.moyozi.roomchat

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.runtime.Composable
import androidx.lifecycle.viewmodel.compose.viewModel
import com.moyozi.roomchat.ui.CallScreen
import com.moyozi.roomchat.ui.JoinScreen
import com.moyozi.roomchat.ui.SfuViewModel
import com.moyozi.roomchat.ui.theme.RoomChatTheme

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent {
            RoomChatTheme {
                RoomChatApp()
            }
        }
    }
}

@Composable
fun RoomChatApp(vm: SfuViewModel = viewModel()) {
    if (vm.inCall) {
        CallScreen(vm = vm)
    } else {
        JoinScreen(
            joining = vm.joining,
            onJoin = { serverUrl, roomId, name -> vm.join(serverUrl, roomId, name) },
        )
    }
}
