import { createSignal, onCleanup } from "solid-js";
import JoinScreen from "./components/JoinScreen";
import CallScreen from "./components/CallScreen";
import { SfuClient } from "./lib/sfu";
import type { LogEntry, RemoteParticipant, TrackInfo } from "./lib/types";

function App() {
  const [screen, setScreen] = createSignal<"join" | "call">("join");
  const [roomId, setRoomId] = createSignal("");
  const [localStream, setLocalStream] = createSignal<MediaStream | null>(null);
  const [participants, setParticipants] = createSignal<RemoteParticipant[]>([]);
  const [logs, setLogs] = createSignal<LogEntry[]>([]);
  // 分配器为本房间分配的"信令节点 · 地址"，用于界面展示
  const [signalingNode, setSignalingNode] = createSignal("");
  const [screenSharing, setScreenSharing] = createSignal(false);

  let sfu: SfuClient | null = null;

  const addLog = (msg: string, level: LogEntry["level"] = "info") => {
    const time = new Date().toLocaleTimeString("zh-CN", { hour12: false });
    setLogs((prev) => [...prev, { time, msg, level }].slice(-200));
  };

  const handleJoin = async (endpoint: string, rid: string, name: string, token: string) => {
    try {
      sfu = new SfuClient({
        onLog: addLog,
        onParticipantJoined: (id, tracks) => {
          // 自动订阅新参与者 (按其发布的全部轨道: 音频 + 摄像头 + 屏幕共享)
          setParticipants((prev) =>
            prev.find((p) => p.id === id)
              ? prev.map((p) => (p.id === id ? { ...p, tracks } : p))
              : [...prev, { id, status: "idle", stream: null, tracks }]
          );
          setTimeout(() => sfu?.subscribe(id, tracks), 500);
        },
        onParticipantLeft: (id) => {
          setParticipants((prev) => prev.filter((p) => p.id !== id));
        },
        onParticipantTracks: (id, tracks) => {
          setParticipants((prev) =>
            prev.map((p) => (p.id === id ? { ...p, tracks } : p))
          );
        },
        onSubscriptionUpdate: (publisherId, update) => {
          setParticipants((prev) =>
            prev.map((p) =>
              p.id === publisherId ? { ...p, ...update } : p
            )
          );
        },
        onParticipantUpdated: (id, media) => {
          setParticipants((prev) =>
            prev.map((p) => (p.id === id ? { ...p, ...media } : p))
          );
        },
        onWelcome: (info) => {
          addLog(`信令协议 v${info.protocol_version} (${info.features.join(", ")})`);
        },
        onAllocated: (alloc) => {
          setSignalingNode(`${alloc.nodeId} · ${alloc.wsUrl}`);
        },
        onConnectionStateChange: (state) => {
          if (state === "connected") addLog("WebRTC 连接已建立", "success");
        },
      });

      await sfu.join(endpoint, rid, name, token);
      setRoomId(rid);
      setLocalStream(sfu.getLocalStream());
      setScreen("call");
    } catch (e: any) {
      const msg = e?.message || String(e);
      addLog(`加入失败: ${msg}`, "error");
      sfu?.leave();
      sfu = null;
      // 抛给 JoinScreen 展示在表单下方
      throw new Error(msg);
    }
  };

  const handleLeave = () => {
    sfu?.leave();
    sfu = null;
    setScreen("join");
    setParticipants([]);
    setLocalStream(null);
    setSignalingNode("");
    setScreenSharing(false);
  };

  const handleSubscribe = (publisherId: string) => {
    // 如果参与者不在列表中, 先添加
    let tracks: TrackInfo[] = [];
    setParticipants((prev) => {
      const found = prev.find((p) => p.id === publisherId);
      if (found) {
        tracks = found.tracks ?? [];
        return prev;
      }
      return [...prev, { id: publisherId, status: "idle", stream: null }];
    });
    sfu?.subscribe(publisherId, tracks);
  };

  const handleStartScreen = async () => {
    try {
      await sfu?.startScreenShare();
      setScreenSharing(true);
    } catch (e: any) {
      addLog(`屏幕共享失败: ${e?.message || e}`, "error");
    }
  };

  const handleStopScreen = async () => {
    await sfu?.stopScreenShare();
    setScreenSharing(false);
  };

  const handleUnsubscribe = (publisherId: string) => {
    sfu?.unsubscribe(publisherId);
  };

  const handleToggleAudio = (enabled: boolean) => {
    sfu?.toggleAudio(enabled);
  };

  const handleToggleVideo = (enabled: boolean) => {
    sfu?.toggleVideo(enabled);
  };

  onCleanup(() => {
    sfu?.leave();
  });

  return (
    <>
      {screen() === "join" && <JoinScreen onJoin={handleJoin} />}
      {screen() === "call" && (
        <CallScreen
          roomId={roomId()}
          signalingNode={signalingNode()}
          localStream={localStream()}
          participants={participants()}
          logs={logs()}
          onSubscribe={handleSubscribe}
          onUnsubscribe={handleUnsubscribe}
          onLeave={handleLeave}
          onToggleAudio={handleToggleAudio}
          onToggleVideo={handleToggleVideo}
          isScreenSharing={screenSharing()}
          onStartScreen={handleStartScreen}
          onStopScreen={handleStopScreen}
        />
      )}
    </>
  );
}

export default App;
