import type { ClientMessage, ServerMessage } from "./types";

/** 在 WebSocket URL 上追加 `token` 查询参数 (保留已有查询串)。 */
function withToken(url: string, token: string): string {
  const sep = url.includes("?") ? "&" : "?";
  return `${url}${sep}token=${encodeURIComponent(token)}`;
}

/**
 * 信令客户端 — WebSocket 封装
 *
 * 负责与 SFU 信令服务器的 JSON 消息收发。
 * 每个 SfuClient 实例维护一个独立的 WebSocket 连接。
 */
export class SignalingClient {
  private ws: WebSocket | null = null;
  public url: string;
  public onMessage: ((msg: ServerMessage) => void) | null = null;
  public onClose: (() => void) | null = null;

  constructor(url: string, token?: string) {
    // 浏览器 WebSocket 无法自定义请求头, 故通过 `?token=` 传递给服务端鉴权。
    this.url = token ? withToken(url, token) : url;
  }

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.url);

      this.ws.onopen = () => resolve();
      this.ws.onerror = () => reject(new Error(`WebSocket 连接失败: ${this.url}`));

      this.ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data) as ServerMessage;
          this.onMessage?.(msg);
        } catch (e) {
          console.error("消息解析失败:", event.data, e);
        }
      };

      this.ws.onclose = () => {
        this.ws = null;
        this.onClose?.();
      };
    });
  }

  send(msg: ClientMessage): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  close(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  get isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }
}
