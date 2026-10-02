import { createSignal, onCleanup } from "solid-js";
import JoinScreen from "./components/JoinScreen";
import CallScreen from "./components/CallScreen";
import { SfuClient } from "./lib/sfu";
import type { LogEntry, RemoteParticipant } from "./lib/types";

function App() {
  const [screen, setScreen] = createSignal<"join" | "call">("join");
  const [roomId, setRoomId] = createSignal("");
  const [localStream, setLocalStream] = createSignal<MediaStream | null>(null);
  const [participants, setParticipants] = createSignal<RemoteParticipant[]>([]);
  const [logs, setLogs] = createSignal<LogEntry[]>([]);
  // 分配器为本房间分配的"信令节点 · 地址"，用于界面展示
  const [signalingNode, setSignalingNode] = createSignal("");

  let sfu: SfuClient | null = null;

  const addLog = (msg: string, level: LogEntry["level"] = "info") => {
    const time = new Date().toLocaleTimeString("zh-CN", { hour12: false });
    setLogs((prev) => [...prev, { time, msg, level }].slice(-200));
  };

  const handleJoin = async (endpoint: string, rid: string, name: string, token: string) => {
    try {
      sfu = new SfuClient({
        onLog: addLog,
        onParticipantJoined: (id) => {
          // 自动订阅新参与者
          setParticipants((prev) =>
            prev.find((p) => p.id === id)
              ? prev
              : [...prev, { id, status: "idle", stream: null }]
          );
          // 自动发起订阅
          setTimeout(() => sfu?.subscribe(id), 500);
        },
        onParticipantLeft: (id) => {
          setParticipants((prev) => prev.filter((p) => p.id !== id));
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
  };

  const handleSubscribe = (publisherId: string) => {
    // 如果参与者不在列表中, 先添加
    setParticipants((prev) =>
      prev.find((p) => p.id === publisherId)
        ? prev
        : [...prev, { id: publisherId, status: "idle", stream: null }]
    );
    sfu?.subscribe(publisherId);
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
        />
      )}
    </>
  );
}

export default App;
