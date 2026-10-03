import webSocket from '@ohos.net.webSocket';
import util from '@ohos.util';

/**
 * 信令客户端 — WebSocket 封装 (对应 Android 端 webrtc/SignalingClient.kt)
 *
 * 与 SFU 信令服务器交换 JSON 文本消息。整个会话只维护一条 WebSocket,
 * 所有 RTCPeerConnection 复用该连接 (通过 pc_id 区分)。
 */

/** 服务器 -> 客户端消息 (与 Rust 服务端 SignalingResponse 对应) */
export interface ServerMessage {
  type: string;
  pc_id?: string;
  sdp?: string;
  ice_candidates?: string[];
  candidate?: string;
  sdp_mid?: string | null;
  sdp_mline_index?: number | null;
  participant_id?: string;
  publisher_id?: string;
  error?: string;
}

export type SignalHandler = (msg: ServerMessage) => void;

export class SignalingClient {
  private ws: webSocket.WebSocket = webSocket.createWebSocket();
  private opened: boolean = false;
  private textDecoder: util.TextDecoder = util.TextDecoder.create('utf-8');

  /** 收到服务器文本消息时回调 */
  onMessage: SignalHandler | null = null;

  /** 连接关闭/失败时回调 */
  onClosed: (() => void) | null = null;

  /** 建立连接, 挂起直到握手完成或失败 (带超时, 避免一直卡在 joining) */
  connect(url: string, timeoutMs: number = 10000): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const timer = setTimeout(() => {
        if (settled) {
          return;
        }
        settled = true;
        try {
          this.ws.close((e, s) => {
          });
        } catch (e) {
          // ignore
        }
        reject(new Error('WebSocket 连接超时'));
      }, timeoutMs);

      const ok = (): void => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        this.opened = true;
        resolve();
      };
      const fail = (msg: string): void => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        reject(new Error(msg));
      };

      this.ws.on('open', (err) => {
        if (err) {
          fail(`WebSocket 打开失败: ${JSON.stringify(err)}`);
          return;
        }
        ok();
      });

      this.ws.on('message', (err, data) => {
        if (err) {
          return;
        }
        const text = this.dataToText(data as string | ArrayBuffer);
        if (text.length === 0) {
          return;
        }
        try {
          const msg = JSON.parse(text) as ServerMessage;
          if (this.onMessage != null) {
            this.onMessage(msg);
          }
        } catch (e) {
          // 忽略无法解析的消息
        }
      });

      this.ws.on('close', () => {
        this.opened = false;
        if (this.onClosed != null) {
          this.onClosed();
        }
      });

      this.ws.on('error', () => {
        this.opened = false;
        if (this.onClosed != null) {
          this.onClosed();
        }
      });

      this.ws.connect(url, (err, success) => {
        if (!success) {
          fail(`WebSocket 连接失败: ${JSON.stringify(err)}`);
        }
      });
    });
  }

  /** 发送一条 JSON 消息 */
  send(text: string): void {
    if (!this.opened) {
      return;
    }
    this.ws.send(text, (err, success) => {
      // 发送失败暂不处理, 由 close/error 事件兜底
    });
  }

  close(): void {
    try {
      this.ws.close((err, success) => {
      });
    } catch (e) {
      // ignore
    }
    this.ws.off('open');
    this.ws.off('message');
    this.ws.off('close');
    this.ws.off('error');
    this.opened = false;
  }

  isConnected(): boolean {
    return this.opened;
  }

  private dataToText(data: string | ArrayBuffer): string {
    if (typeof data === 'string') {
      return data;
    }
    return this.textDecoder.decodeToString(new Uint8Array(data as ArrayBuffer));
  }
}
