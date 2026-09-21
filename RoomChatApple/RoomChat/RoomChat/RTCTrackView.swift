import SwiftUI
import WebRTC

/// 将 RTCVideoTrack 渲染到 SwiftUI (基于 RTCMTLVideoView)
struct RTCTrackView: UIViewRepresentable {
    let track: RTCVideoTrack?
    let mirror: Bool

    final class Coordinator {
        var boundTrack: RTCVideoTrack?
    }

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeUIView(context: Context) -> RTCMTLVideoView {
        let view = RTCMTLVideoView()
        view.videoContentMode = .scaleAspectFill
        view.clipsToBounds = true
        return view
    }

    func updateUIView(_ uiView: RTCMTLVideoView, context: Context) {
        if context.coordinator.boundTrack !== track {
            context.coordinator.boundTrack?.remove(uiView)
            track?.add(uiView)
            context.coordinator.boundTrack = track
        }
        uiView.transform = mirror ? CGAffineTransform(scaleX: -1, y: 1) : .identity
    }

    static func dismantleUIView(_ uiView: RTCMTLVideoView, coordinator: Coordinator) {
        coordinator.boundTrack?.remove(uiView)
        coordinator.boundTrack = nil
    }
}
