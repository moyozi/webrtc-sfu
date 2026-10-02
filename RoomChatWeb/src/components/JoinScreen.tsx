import { createSignal } from "solid-js";

interface JoinScreenProps {
  onJoin: (
    endpoint: string,
    roomId: string,
    name: string,
    token: string
  ) => Promise<void>;
}

/** 随机生成一个昵称, 例如 user-7f3k2a */
function randomName(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let s = "";
  for (let i = 0; i < 6; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return `user-${s}`;
}

export default function JoinScreen(props: JoinScreenProps) {
  // 默认填分配器地址；也可直接填 ws:// 信令地址绕过分配器。
  const [endpoint, setEndpoint] = createSignal("http://192.168.31.112:5190");
  const [roomId, setRoomId] = createSignal("room-1");
  const [name, setName] = createSignal(randomName());
  const [token, setToken] = createSignal("");
  const [error, setError] = createSignal("");
  const [joining, setJoining] = createSignal(false);

  const handleJoin = async () => {
    const n = name().trim();
    if (!n) {
      setError("请输入昵称");
      return;
    }
    if (!roomId().trim()) {
      setError("请输入房间号");
      return;
    }
    setError("");
    setJoining(true);
    try {
      await props.onJoin(endpoint().trim(), roomId().trim(), n, token().trim());
      // 成功时 App 会切换到通话界面, 本组件卸载
    } catch (e: any) {
      setError(e?.message ? String(e.message) : String(e));
      setJoining(false);
    }
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
              <span class="label-text text-xs">信令分配器地址</span>
            </div>
            <input
              type="text"
              class="input input-bordered input-sm w-full"
              value={endpoint()}
              onInput={(e) => setEndpoint(e.currentTarget.value)}
              placeholder="http://host:5190"
            />
            <div class="label py-1">
              <span class="label-text-alt text-[10px] text-base-content/50">
                加入时按房间号向分配器请求信令地址。也可直接填 ws://host:5174 绕过分配器。
              </span>
            </div>
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
              分配器 <code class="kbd kbd-xs">HTTP 5190</code>　信令 <code class="kbd kbd-xs">WS 5174</code>　媒体 <code class="kbd kbd-xs">UDP 5175+</code>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
