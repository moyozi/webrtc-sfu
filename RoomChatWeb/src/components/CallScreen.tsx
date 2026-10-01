import { For, createSignal, Show } from "solid-js";
import VideoTile from "./VideoTile";
import Sidebar from "./Sidebar";
import type { LogEntry, RemoteParticipant } from "../lib/types";

interface CallScreenProps {
  roomId: string;
  localStream: MediaStream | null;
  participants: RemoteParticipant[];
  logs: LogEntry[];
  onSubscribe: (publisherId: string) => void;
  onUnsubscribe: (publisherId: string) => void;
  onLeave: () => void;
  onToggleAudio: (enabled: boolean) => void;
  onToggleVideo: (enabled: boolean) => void;
}

export default function CallScreen(props: CallScreenProps) {
  const [audioOn, setAudioOn] = createSignal(true);
  const [videoOn, setVideoOn] = createSignal(true);

  const toggleAudio = () => {
    const v = !audioOn();
    setAudioOn(v);
    props.onToggleAudio(v);
  };

  const toggleVideo = () => {
    const v = !videoOn();
    setVideoOn(v);
    props.onToggleVideo(v);
  };

  // 计算总人数 (本地 + 远端)
  const totalCount = () => props.participants.length + 1;

  return (
    <div class="h-screen flex flex-col bg-base-300">
      {/* ── 顶栏 ── */}
      <header class="h-14 flex items-center justify-between px-5 bg-base-100 border-b border-base-300 shrink-0">
        <div class="flex items-center gap-2 text-sm">
          <span class="text-base-content/50">房间</span>
          <span class="font-semibold">{props.roomId}</span>
          <span class="badge badge-ghost badge-sm ml-2">{totalCount()} 人</span>
        </div>

        <div class="flex items-center gap-2">
          {/* 麦克风 */}
          <button
            class="btn btn-circle btn-sm"
            classList={{ "btn-error": !audioOn() }}
            onClick={toggleAudio}
            title={audioOn() ? "静音" : "取消静音"}
          >
            <Show
              when={audioOn()}
              fallback={
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <line x1="1" y1="1" x2="23" y2="23" />
                  <path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6" />
                  <path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23" />
                  <line x1="12" y1="19" x2="12" y2="23" />
                  <line x1="8" y1="23" x2="16" y2="23" />
                </svg>
              }
            >
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z" />
                <path d="M19 10v2a7 7 0 0 1-14 0v-2" />
                <line x1="12" y1="19" x2="12" y2="23" />
                <line x1="8" y1="23" x2="16" y2="23" />
              </svg>
            </Show>
          </button>

          {/* 摄像头 */}
          <button
            class="btn btn-circle btn-sm"
            classList={{ "btn-error": !videoOn() }}
            onClick={toggleVideo}
            title={videoOn() ? "关闭摄像头" : "打开摄像头"}
          >
            <Show
              when={videoOn()}
              fallback={
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M16 16v1a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h2m5.66 0H14a2 2 0 0 1 2 2v3.34l1 1L23 7v10" />
                  <line x1="1" y1="1" x2="23" y2="23" />
                </svg>
              }
            >
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <polygon points="23 7 16 12 23 17 23 7" />
                <rect x="1" y="5" width="15" height="14" rx="2" ry="2" />
              </svg>
            </Show>
          </button>

          {/* 挂断 */}
          <button class="btn btn-error btn-sm gap-1" onClick={props.onLeave}>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M10.68 13.31a16 16 0 0 0 3.41 2.6l1.27-1.27a2 2 0 0 1 2.11-.45 12.84 12.84 0 0 0 2.81.7 2 2 0 0 1 1.72 2v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72 12.84 12.84 0 0 0 .7 2.81 2 2 0 0 1-.45 2.11L8.09 9.91" />
              <line x1="23" y1="1" x2="1" y2="23" />
            </svg>
            挂断
          </button>
        </div>
      </header>

      {/* ── 主区域 ── */}
      <main class="flex-1 flex overflow-hidden">
        {/* 视频网格 */}
        <div class="flex-1 overflow-auto p-4">
          <div class="grid gap-3 max-w-full mx-auto" style={{
            "grid-template-columns": "repeat(auto-fit, minmax(320px, 1fr))",
          }}>
            {/* 本地视频 */}
            <VideoTile
              stream={props.localStream}
              label="我"
              isLocal={true}
              placeholder="摄像头未开启"
              audioEnabled={audioOn()}
              videoEnabled={videoOn()}
            />

            {/* 远端视频 */}
            <For each={props.participants}>
              {(p) => (
                <VideoTile
                  stream={p.stream}
                  label={p.id}
                  audioEnabled={p.audioEnabled}
                  videoEnabled={p.videoEnabled}
                  placeholder={
                    p.status === "connecting" ? "连接中..." :
                    p.status === "error" ? "连接失败" :
                    "未订阅"
                  }
                />
              )}
            </For>
          </div>
        </div>

        {/* 侧栏 */}
        <Sidebar
          participants={props.participants}
          logs={props.logs}
          onSubscribe={props.onSubscribe}
          onUnsubscribe={props.onUnsubscribe}
        />
      </main>
    </div>
  );
}
