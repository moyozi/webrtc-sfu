/**
 * 数据模型 — 与 Android 端 webrtc/Models.kt 保持一致
 */

/** 日志级别 (与 Web 端一致) */
export enum LogLevel { INFO, SUCCESS, WARN, ERROR }

/** 远端订阅状态 */
export enum SubStatus { IDLE, CONNECTING, SUBSCRIBED, ERROR }

/** 发布者的一条媒体轨道 (mid + kind), 与信令协议 `TrackInfo` 对应。 */
export interface TrackInfo {
  mid: string;
  kind: string;
}

/** 房间成员信息 (成员快照 / 广播共用), 与信令协议 `ParticipantInfo` 对应。 */
export interface ParticipantInfo {
  participant_id: string;
  audio_enabled: boolean;
  video_enabled: boolean;
  tracks: TrackInfo[];
}

/** 一条事件日志 */
export class LogEntry {
  time: string;
  msg: string;
  level: LogLevel;

  constructor(time: string, msg: string, level: LogLevel) {
    this.time = time;
    this.msg = msg;
    this.level = level;
  }
}

/**
 * UI 中展示的参与者 (含本地)。
 *
 * 使用 @Observed: 状态/是否有画面会随订阅过程变化, 需要在 ForEach 复用时
 * 仍能驱动子组件刷新 (子组件用 @ObjectLink 接收)。
 */
@Observed
export class Participant {
  id: string;
  status: SubStatus;
  hasVideo: boolean;

  constructor(id: string, status: SubStatus = SubStatus.IDLE, hasVideo: boolean = false) {
    this.id = id;
    this.status = status;
    this.hasVideo = hasVideo;
  }
}
