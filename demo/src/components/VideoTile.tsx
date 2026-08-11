import { createEffect } from "solid-js";

interface VideoTileProps {
  stream: MediaStream | null;
  label: string;
  isLocal?: boolean;
  placeholder?: string;
}

export default function VideoTile(props: VideoTileProps) {
  let videoEl: HTMLVideoElement | undefined;

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

      {/* 底部标签栏 */}
      <div class="absolute bottom-0 left-0 right-0 p-3 bg-gradient-to-t from-black/70 to-transparent flex items-center justify-between">
        <span class="text-white text-sm font-medium drop-shadow">{props.label}</span>
        {props.isLocal && (
          <span class="badge badge-primary badge-sm">本地</span>
        )}
      </div>
    </div>
  );
}
