import { For, createSignal } from "solid-js";
import type { LogEntry, RemoteParticipant } from "../lib/types";

interface SidebarProps {
  participants: RemoteParticipant[];
  logs: LogEntry[];
  onSubscribe: (publisherId: string) => void;
  onUnsubscribe: (publisherId: string) => void;
}

export default function Sidebar(props: SidebarProps) {
  const [subInput, setSubInput] = createSignal("");

  const handleSubscribe = () => {
    const id = subInput().trim();
    if (id) {
      props.onSubscribe(id);
      setSubInput("");
    }
  };

  return (
    <aside class="w-72 bg-base-200 border-l border-base-300 flex flex-col overflow-hidden shrink-0">
      {/* 参与者列表 */}
      <div class="p-4 border-b border-base-300">
        <h3 class="text-xs uppercase tracking-wider text-base-content/50 mb-3">参与者</h3>
        <div class="space-y-2">
          {/* 本地 */}
          <div class="flex items-center gap-2 text-sm">
            <span class="w-2 h-2 rounded-full bg-success" />
            <span class="flex-1">我</span>
            <span class="badge badge-ghost badge-sm">本地</span>
          </div>

          {/* 远端 */}
          <For each={props.participants}>
            {(p) => (
              <div class="flex items-center gap-2 text-sm">
                <span
                  class="w-2 h-2 rounded-full"
                  classList={{
                    "bg-warning": p.status === "connecting",
                    "bg-success": p.status === "subscribed",
                    "bg-error": p.status === "error",
                    "bg-base-content/20": p.status === "idle",
                  }}
                />
                <span class="flex-1 truncate" title={p.id}>{p.id}</span>
                {p.status === "subscribed" ? (
                  <button
                    class="btn btn-ghost btn-xs"
                    onClick={() => props.onUnsubscribe(p.id)}
                  >
                    取消
                  </button>
                ) : p.status !== "connecting" ? (
                  <button
                    class="btn btn-ghost btn-xs text-primary"
                    onClick={() => props.onSubscribe(p.id)}
                  >
                    订阅
                  </button>
                ) : (
                  <span class="loading loading-spinner loading-xs" />
                )}
              </div>
            )}
          </For>
        </div>
      </div>

      {/* 手动订阅 */}
      <div class="p-4 border-b border-base-300">
        <h3 class="text-xs uppercase tracking-wider text-base-content/50 mb-2">手动订阅</h3>
        <p class="text-xs text-base-content/40 mb-2">输入参与者 ID 订阅其音视频</p>
        <div class="flex gap-1">
          <input
            type="text"
            class="input input-bordered input-xs flex-1"
            placeholder="参与者 ID"
            value={subInput()}
            onInput={(e) => setSubInput(e.currentTarget.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSubscribe()}
          />
          <button class="btn btn-secondary btn-xs" onClick={handleSubscribe}>
            订阅
          </button>
        </div>
      </div>

      {/* 事件日志 */}
      <div class="flex-1 flex flex-col overflow-hidden p-4">
        <h3 class="text-xs uppercase tracking-wider text-base-content/50 mb-2">事件日志</h3>
        <div class="flex-1 overflow-y-auto log-mono text-xs space-y-0.5 pr-1">
          <For each={props.logs}>
            {(log) => (
              <div
                classList={{
                  "text-primary": log.level === "info",
                  "text-success": log.level === "success",
                  "text-warning": log.level === "warn",
                  "text-error": log.level === "error",
                }}
              >
                <span class="text-base-content/30 mr-1.5">{log.time}</span>
                {log.msg}
              </div>
            )}
          </For>
        </div>
      </div>
    </aside>
  );
}
