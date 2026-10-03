import SwiftUI
import WebRTC

/// 将 RTCVideoTrack 渲染到 SwiftUI (基于 RTCMTLVideoView)
struct RTCTrackView: UIViewRepresentable {
    let track: RTCVideoTrack?
    let mirror: Bool

    final class Coordinator: NSObject, RTCVideoViewDelegate {
        var boundTrack: RTCVideoTrack?

        /// renderer 收到尺寸变化说明它确实在收帧 (诊断: 区分“未挂 renderer”与“Metal 没画出来”)
        func videoView(_ videoView: RTCVideoRenderer, didChangeVideoSize size: CGSize) {
            NSLog("[RoomChat] RTCMTLVideoView 收到视频帧, 尺寸 %.0fx%.0f", size.width, size.height)
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeUIView(context: Context) -> RTCMTLVideoView {
        let view = RTCMTLVideoView()
        view.videoContentMode = .scaleAspectFill
        view.clipsToBounds = true
        view.delegate = context.coordinator
        return view
    }

    func updateUIView(_ uiView: RTCMTLVideoView, context: Context) {
        if context.coordinator.boundTrack !== track {
            context.coordinator.boundTrack?.remove(uiView)
            if let track = track {
                track.add(uiView)
            }
            context.coordinator.boundTrack = track
        }
        uiView.transform = mirror ? CGAffineTransform(scaleX: -1, y: 1) : .identity

        // RTCMTLVideoView 若首次 drawable 尺寸为 0, Metal 会丢帧并一直黑屏。
        // 这里强制一次布局, 让 CAMetalLayer 拿到正确的 bounds。
        uiView.setNeedsLayout()
        uiView.layoutIfNeeded()
    }

    static func dismantleUIView(_ uiView: RTCMTLVideoView, coordinator: Coordinator) {
        coordinator.boundTrack?.remove(uiView)
        coordinator.boundTrack = nil
    }
}
