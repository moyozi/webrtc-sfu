## Usage

```bash
$ npm install # or pnpm install or yarn install
```

### Learn more on the [Solid Website](https://solidjs.com) and come chat with us on our [Discord](https://discord.com/invite/solidjs)

## Available Scripts

In the project directory, you can run:

### `npm run dev`

Runs the app in the development mode.<br>
Open [http://localhost:5173](http://localhost:5173) to view it in the browser.

### `npm run build`

Builds the app for production to the `dist` folder.<br>
It correctly bundles Solid in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.<br>
Your app is ready to be deployed!

## 连接方式（信令分配器优先）

客户端加入房间时**固定先请求信令分配器**拿到本房间的信令地址，再建立 WebSocket：

```text
POST http://<dispatcher>/allocate  {"room_id":"room-1"}
  → {"node_id":"sig-a","ws_url":"ws://<signaling>:5174"}
  → 连接 ws_url, 发送 join
```

界面「信令分配器地址」支持三种写法（见 `src/lib/dispatcher.ts`）：

| 填写 | 行为 |
|---|---|
| `http://host:5190` | 走分配器（推荐） |
| `host:5190` | 自动补 `http://` 当分配器 |
| `ws://host:5174` | 直连信令，跳过分配器（单机调试方便） |

同一 `room_id` 会稳定分配到同一台信令机（房间亲和）；分配失败/超时会给出错误提示。
服务端可用 `SFU_DISPATCHER_COHOST=on` 把分配器与信令/媒体同进程启动。

## Deployment

Learn more about deploying your application with the [documentations](https://vite.dev/guide/static-deploy.html)
