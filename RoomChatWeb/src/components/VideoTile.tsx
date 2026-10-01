import { createEffect } from "solid-js";

interface VideoTileProps {
  stream: MediaStream | null;
  label: string;
  isLocal?: boolean;
  placeholder?: string;
  /** 麦克风是否开启 (默认 true) */
  audioEnabled?: boolean;
  /** 摄像头是否开启 (默认 true) */
  videoEnabled?: boolean;
}

export default function VideoTile(props: VideoTileProps) {
  let videoEl: HTMLVideoElement | undefined;

  const audioEnabled = () => props.audioEnabled !== false;
  const videoEnabled = () => props.videoEnabled !== false;

  // 当 stream 变化时绑定到 video 元素
  createEffect(() => {
    if (videoEl && props.stream !== undefined) {
      videoEl.srcObject = props.stream;
    }
  });

  return (
    <div class="relative bg-black rounded-xl overflow-hidden aspect-video border border-base-300 shadow-lg">
      <video
        ref={videoEl}
        autoplay
        playsinline
        muted={props.isLocal}
        class="w-full h-full object-cover"
        classList={{ "scale-x-[-1]": props.isLocal }}
      />

      {/* 无视频占位 */}
      {!props.stream && (
        <div class="absolute inset-0 flex items-center justify-center text-base-content/30">
          <div class="text-center">
            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" class="mx-auto mb-2">
              <polygon points="23 7 16 12 23 17 23 7" />
              <rect x="1" y="5" width="15" height="14" rx="2" ry="2" />
            </svg>
            <p class="text-sm">{props.placeholder ?? "等待视频..."}</p>
          </div>
        </div>
      )}

      {/* 摄像头关闭遮罩 */}
      {!videoEnabled() && (
        <div class="absolute inset-0 flex items-center justify-center bg-black/60 text-base-content/60">
          <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
            <path d="M16 16v1a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h2m5.66 0H14a2 2 0 0 1 2 2v3.34l1 1L23 7v10" />
            <line x1="1" y1="1" x2="23" y2="23" />
          </svg>
        </div>
      )}

      {/* 底部标签栏 */}
      <div class="absolute bottom-0 left-0 right-0 p-3 bg-gradient-to-t from-black/70 to-transparent flex items-center justify-between">
        <span class="text-white text-sm font-medium drop-shadow flex items-center gap-1.5">
          {props.label}
          {!audioEnabled() && (
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="text-error">
              <line x1="1" y1="1" x2="23" y2="23" />
              <path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6" />
              <path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23" />
            </svg>
          )}
        </span>
        {props.isLocal && (
          <span class="badge badge-primary badge-sm">本地</span>
        )}
      </div>
    </div>
  );
}
