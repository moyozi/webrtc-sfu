/**
 * 信令分配器 (Dispatcher) 客户端。
 *
 * 设计约定: 客户端**固定先请求分配器**拿到本房间应连接的信令地址, 再连信令。
 * 这样信令多机部署时, 同一 `room_id` 始终落到同一台信令机 (房间亲和)。
 *
 * 地址兼容规则 (见 `normalizeEndpoint`):
 *   - `http://` / `https://`  → 视为分配器地址, 走 `POST /allocate`;
 *   - `ws://` / `wss://`      → 视为直连信令地址, 跳过分配器 (便于单机调试);
 *   - 裸 `host:port`          → 补全为 `http://host:port` 当分配器。
 */

export interface AllocateResult {
  roomId: string;
  /** 分配到的信令节点 id (`(direct)` 表示直连模式) */
  nodeId: string;
  /** 实际要连接的信令 WebSocket 地址 */
  wsUrl: string;
  quicUrl?: string;
  whipUrl?: string;
  whepUrl?: string;
}

/** 规范化用户输入的地址: 去掉末尾斜杠, 缺省补 `http://`。 */
export function normalizeEndpoint(raw: string): string {
  const s = raw.trim().replace(/\/+$/, "");
  if (!s) throw new Error("请填写信令分配器地址");
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(s)) return s;
  return `http://${s}`;
}

function isDirectWs(url: string): boolean {
  return /^wss?:\/\//i.test(url);
}

/**
 * 向分配器请求房间对应的信令地址。
 *
 * @param endpoint 用户填写的地址 (分配器 http(s) 或直连 ws(s))
 * @param roomId   房间号 (大小写/空格敏感, 同房间必须一致)
 * @param timeoutMs 分配器请求超时 (毫秒)
 */
export async function allocateSignaling(
  endpoint: string,
  roomId: string,
  timeoutMs = 5000
): Promise<AllocateResult> {
  const base = normalizeEndpoint(endpoint);

  // 直连模式: 用户直接给了 ws:// 地址, 不去请求分配器。
  if (isDirectWs(base)) {
    return { roomId, nodeId: "(direct)", wsUrl: base };
  }

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  let resp: Response;
  try {
    resp = await fetch(`${base}/allocate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ room_id: roomId }),
      signal: ctrl.signal,
    });
  } catch (e: any) {
    throw new Error(
      e?.name === "AbortError"
        ? `分配器请求超时: ${base}/allocate`
        : `无法连接分配器 ${base}: ${e?.message || e}`
    );
  } finally {
    clearTimeout(timer);
  }

  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    const hint = resp.status === 503 ? " (当前无健康信令节点)" : "";
    throw new Error(`分配器返回 ${resp.status}${hint}: ${text || resp.statusText}`);
  }

  let data: any;
  try {
    data = await resp.json();
  } catch {
    throw new Error("分配器响应不是合法 JSON");
  }
  if (!data.ws_url) {
    throw new Error("分配器未返回 ws_url (该信令节点可能未启用 WebSocket)");
  }

  return {
    roomId: typeof data.room_id === "string" ? data.room_id : roomId,
    nodeId: typeof data.node_id === "string" ? data.node_id : "?",
    wsUrl: data.ws_url,
    quicUrl: data.quic_url,
    whipUrl: data.whip_url,
    whepUrl: data.whep_url,
  };
}
