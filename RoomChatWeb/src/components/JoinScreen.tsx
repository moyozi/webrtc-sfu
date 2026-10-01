import { createSignal } from "solid-js";

interface JoinScreenProps {
  onJoin: (serverUrl: string, roomId: string, name: string, token: string) => void;
}

/** 随机生成一个昵称, 例如 user-7f3k2a */
function randomName(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let s = "";
  for (let i = 0; i < 6; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return `user-${s}`;
}

export default function JoinScreen(props: JoinScreenProps) {
  const [serverUrl, setServerUrl] = createSignal("ws://192.168.31.112:5174");
  const [roomId, setRoomId] = createSignal("room-1");
  const [name, setName] = createSignal(randomName());
  const [token, setToken] = createSignal("");
  const [error, setError] = createSignal("");
  const [joining, setJoining] = createSignal(false);

  const handleJoin = () => {
    const n = name().trim();
    if (!n) {
      setError("请输入昵称");
      return;
    }
    setError("");
    setJoining(true);
    props.onJoin(serverUrl(), roomId().trim(), n, token().trim());
  };

  return (
    <div class="min-h-screen flex items-center justify-center bg-base-300">
      <div class="card w-96 max-w-[90vw] bg-base-100 shadow-xl border border-base-300">
        <div class="card-body">
          {/* Logo */}
          <div class="flex items-center gap-3 mb-1">
            <div class="text-primary">
              <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <polygon points="23 7 16 12 23 17 23 7" />
                <rect x="1" y="5" width="15" height="14" rx="2" ry="2" />
              </svg>
            </div>
            <h1 class="text-2xl font-bold">WebRTC SFU</h1>
          </div>
          <p class="text-sm text-base-content/60 mb-6">多人视频通话 Demo</p>

          {/* Form */}
          <label class="form-control w-full mb-3">
            <div class="label py-1">
              <span class="label-text text-xs">信令服务器</span>
            </div>
            <input
              type="text"
              class="input input-bordered input-sm w-full"
              value={serverUrl()}
              onInput={(e) => setServerUrl(e.currentTarget.value)}
              placeholder="ws://host:port"
            />
          </label>

          <label class="form-control w-full mb-3">
            <div class="label py-1">
              <span class="label-text text-xs">房间号</span>
            </div>
            <input
              type="text"
              class="input input-bordered input-sm w-full"
              value={roomId()}
              onInput={(e) => setRoomId(e.currentTarget.value)}
            />
          </label>

          <label class="form-control w-full mb-3">
            <div class="label py-1">
              <span class="label-text text-xs">鉴权 Token（可选）</span>
            </div>
            <input
              type="password"
              class="input input-bordered input-sm w-full"
              value={token()}
              onInput={(e) => setToken(e.currentTarget.value)}
              placeholder="服务端启用 JWT 鉴权时填写"
            />
          </label>

          <label class="form-control w-full mb-4">
            <div class="label py-1">
              <span class="label-text text-xs">你的昵称</span>
            </div>
            <input
              type="text"
              class="input input-bordered input-sm w-full"
              value={name()}
              onInput={(e) => setName(e.currentTarget.value)}
              placeholder="输入昵称"
              maxlength="20"
              onKeyDown={(e) => e.key === "Enter" && handleJoin()}
            />
          </label>

          <button
            class="btn btn-primary w-full"
            classList={{ "loading": joining() }}
            disabled={joining()}
            onClick={handleJoin}
          >
            {joining() ? (
              <span class="loading loading-spinner loading-sm" />
            ) : (
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M15 10l5 5-5 5" />
                <path d="M4 4v7a4 4 0 0 0 4 4h11" />
              </svg>
            )}
            加入房间
          </button>

          {error() && <p class="text-error text-sm text-center mt-2">{error()}</p>}

          {/* Server info */}
          <div class="mt-6 pt-4 border-t border-base-300 text-center">
            <p class="text-xs text-base-content/50">
              SFU 媒体端口 <code class="kbd kbd-xs">UDP 5175+</code>　信令端口 <code class="kbd kbd-xs">WS 5174</code>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
