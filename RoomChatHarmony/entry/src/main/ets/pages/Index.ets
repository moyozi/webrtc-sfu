import webrtc, { VideoRenderController, ScalingMode, Logging, LoggingSeverity } from '@ohos/webrtc';
import abilityAccessCtrl, { PermissionRequestResult } from '@ohos.abilityAccessCtrl';
import audio from '@ohos.multimedia.audio';
import common from '@ohos.app.ability.common';
import { CallBackgroundTask } from '../utils/BackgroundTask';
import { BusinessError } from '@kit.BasicServicesKit';
import { SfuClient } from '../webrtc/SfuClient';
import { LogLevel, LogEntry, SubStatus, Participant } from '../model/Models';
import { VideoTile } from '../components/VideoTile';
import { ParticipantRow } from '../components/ParticipantRow';

const TAG: string = '[RoomChat]';

function randomName(): string {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let suffix = '';
  for (let i = 0; i < 6; i++) {
    suffix += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return 'user-' + suffix;
}

@Entry
@Component
struct Index {
  @State inCall: boolean = false;
  @State joining: boolean = false;
  @State serverUrl: string = 'ws://192.168.31.112:5174';
  @State roomId: string = 'room-1';
  @State name: string = randomName();
  @State error: string = '';
  @State audioEnabled: boolean = true;
  @State videoEnabled: boolean = true;
  @State participants: Participant[] = [];
  @State logs: LogEntry[] = [];
  @State showSheet: boolean = false;
  @State manualId: string = '';
  // 本地画面也用同一套 Participant/VideoTile, 便于占位/标签随状态刷新
  @State localParticipant: Participant = new Participant('我', SubStatus.SUBSCRIBED, false);

  private sfu: SfuClient | null = null;
  private localController: VideoRenderController = new VideoRenderController();
  private localVideoTrack: webrtc.MediaStreamTrack | null = null;
  private controllers: Map<string, VideoRenderController> = new Map();

  aboutToAppear(): void {
    // 打开原生库日志 (输出到 hilog), 便于排查渲染/媒体问题
    Logging.enableLogThreads();
    Logging.enableLogTimeStamps();
    Logging.enableLogToDebugOutput(LoggingSeverity.WARNING);
    this.activateAudioSession();
    this.requestPermissions();
  }

  aboutToDisappear(): void {
    this.stopCallBackgroundTask();
    this.sfu?.dispose();
    this.sfu = null;
  }

  /** 页面回到前台 (含后台->前台) 时重新绑定渲染器, 避免黑屏 */
  onPageShow(): void {
    if (this.inCall) {
      this.rebindVideo();
      // 回到前台后重新确保走扬声器
      this.setSpeakerOn();
    }
  }

  private rebindVideo(): void {
    if (this.localVideoTrack != null) {
      this.localController.setVideoTrack(this.localVideoTrack);
    }
    for (const p of this.participants) {
      if (p.hasVideo) {
        const track = this.sfu?.remoteVideoTrack(p.id);
        if (track != null) {
          this.controllerFor(p.id).setVideoTrack(track);
        }
      }
    }
  }

  private startCallBackgroundTask(): void {
    const ctx = this.getUIContext().getHostContext() as common.UIAbilityContext;
    if (ctx == null) {
      return;
    }
    CallBackgroundTask.start(ctx);
    CallBackgroundTask.keepScreenOn(ctx, true);
    this.setSpeakerOn();
  }

  /** 通话默认走扬声器 (而非听筒) */
  private setSpeakerOn(): void {
    try {
      const routing = audio.getAudioManager().getRoutingManager();
      routing.setCommunicationDevice(audio.CommunicationDeviceType.SPEAKER, true)
        .then(() => {
          console.info(`${TAG} 已切换到扬声器`);
        })
        .catch((err: BusinessError) => {
          console.error(`${TAG} setCommunicationDevice 失败: ${JSON.stringify(err)}`);
        });
    } catch (e) {
      console.error(`${TAG} setCommunicationDevice 异常: ${JSON.stringify(e)}`);
    }
  }

  private stopCallBackgroundTask(): void {
    const ctx = this.getUIContext().getHostContext() as common.UIAbilityContext;
    if (ctx == null) {
      return;
    }
    CallBackgroundTask.stop(ctx);
    CallBackgroundTask.keepScreenOn(ctx, false);
  }

  // ── 生命周期辅助 ──────────────────────────────────────────

  private activateAudioSession(): void {
    try {
      const audioManager = audio.getAudioManager();
      const sessionManager = audioManager.getSessionManager();
      const strategy: audio.AudioSessionStrategy = {
        concurrencyMode: audio.AudioConcurrencyMode.CONCURRENCY_MIX_WITH_OTHERS
      };
      sessionManager.activateAudioSession(strategy).then(() => {
        console.info(TAG, 'activateAudioSession SUCCESS');
      }).catch((err: BusinessError) => {
        console.error(TAG, `activateAudioSession ERROR: ${JSON.stringify(err)}`);
      });
    } catch (e) {
      console.error(TAG, `activateAudioSession exception: ${JSON.stringify(e)}`);
    }
  }

  private requestPermissions(): void {
    const atManager = abilityAccessCtrl.createAtManager();
    const context = this.getUIContext().getHostContext();
    if (context === undefined) {
      this.error = '无法获取应用上下文';
      return;
    }
    atManager.requestPermissionsFromUser(context,
      ['ohos.permission.CAMERA', 'ohos.permission.MICROPHONE'])
      .then((result: PermissionRequestResult) => {
        let granted = true;
        for (let i = 0; i < result.permissions.length; i++) {
          if (result.authResults[i] !== abilityAccessCtrl.GrantStatus.PERMISSION_GRANTED) {
            granted = false;
          }
        }
        if (!granted) {
          this.error = '需要摄像头和麦克风权限';
        }
      })
      .catch((err: BusinessError) => {
        console.error(TAG, `requestPermissions ERROR: ${JSON.stringify(err)}`);
      });
  }

  // ── 通话操作 ──────────────────────────────────────────────

  private join(): void {
    if (this.joining) {
      return;
    }
    const trimmedName = this.name.trim();
    if (trimmedName.length === 0) {
      this.error = '请输入昵称';
      return;
    }
    this.error = '';
    this.joining = true;

    const client = new SfuClient();
    client.onLog = (msg: string, level: LogLevel): void => {
      this.addLog(msg, level);
    };
    client.onParticipantJoined = (id: string): void => {
      this.addParticipant(id);
      // 自动发起订阅 (与 Web / Android 端一致, 稍等片刻等发布者就绪)
      setTimeout(() => {
        this.sfu?.subscribe(id);
      }, 500);
    };
    client.onParticipantLeft = (id: string): void => {
      this.removeParticipant(id);
    };
    client.onSubscriptionUpdate = (publisherId: string, status: SubStatus | null, hasVideo: boolean | null): void => {
      this.updateSubscription(publisherId, status, hasVideo);
    };
    client.onConnectionStateChange = (state: string): void => {
      if (state === 'connected') {
        this.addLog('WebRTC 连接已建立', LogLevel.SUCCESS);
      }
    };
    this.sfu = client;

    client.join(this.serverUrl.trim(), this.roomId.trim(), trimmedName)
      .then(() => {
        this.localVideoTrack = client.getLocalVideoTrack();
        if (this.localVideoTrack != null) {
          this.localController.setVideoTrack(this.localVideoTrack);
        }
        this.localController.setMirror(true);
        this.localController.setScalingMode(ScalingMode.AspectFill);
        this.localParticipant.hasVideo = this.videoEnabled && this.localVideoTrack != null;
        this.inCall = true;
        // 进入通话: 申请长时任务 + 屏幕常亮, 退到后台不被回收
        this.startCallBackgroundTask();
      })
      .catch((e: Error) => {
        const msg = (e != null && e.message != null && e.message.length > 0) ? e.message : JSON.stringify(e);
        this.addLog(`加入失败: ${msg}`, LogLevel.ERROR);
        this.error = `加入失败: ${msg}`;
        client.dispose();
        this.sfu = null;
      })
      .finally(() => {
        this.joining = false;
      });
  }

  private leave(): void {
    this.stopCallBackgroundTask();
    this.sfu?.dispose();
    this.sfu = null;
    this.inCall = false;
    this.localVideoTrack = null;
    this.participants = [];
    this.controllers.clear();
    this.audioEnabled = true;
    this.videoEnabled = true;
    this.localParticipant.hasVideo = false;
    this.showSheet = false;
    this.addLog('已离开房间', LogLevel.WARN);
  }

  private subscribe(publisherId: string): void {
    if (!this.participants.some((p: Participant) => p.id === publisherId)) {
      this.participants = this.participants.concat(new Participant(publisherId));
      this.controllers.set(publisherId, new VideoRenderController());
    }
    this.sfu?.subscribe(publisherId);
  }

  private unsubscribe(publisherId: string): void {
    this.sfu?.unsubscribe(publisherId);
  }

  private manualSubscribe(): void {
    const id = this.manualId.trim();
    if (id.length === 0) {
      return;
    }
    this.subscribe(id);
    this.manualId = '';
  }

  private toggleAudio(enabled: boolean): void {
    this.audioEnabled = enabled;
    this.sfu?.toggleAudio(enabled);
  }

  private toggleVideo(enabled: boolean): void {
    this.videoEnabled = enabled;
    this.localParticipant.hasVideo = enabled && this.localVideoTrack != null;
    this.sfu?.toggleVideo(enabled);
  }

  private async switchCamera(): Promise<void> {
    await this.sfu?.switchCamera();
    const track = this.sfu?.getLocalVideoTrack();
    if (track != null) {
      this.localVideoTrack = track;
      this.localController.setVideoTrack(track);
    }
  }

  // ── 状态更新 ──────────────────────────────────────────────

  private addParticipant(id: string): void {
    if (this.participants.some((p: Participant) => p.id === id)) {
      return;
    }
    this.participants = this.participants.concat(new Participant(id));
    this.controllers.set(id, new VideoRenderController());
  }

  private removeParticipant(id: string): void {
    this.participants = this.participants.filter((p: Participant) => p.id !== id);
    this.controllers.delete(id);
  }

  private updateSubscription(publisherId: string, status: SubStatus | null, hasVideo: boolean | null): void {
    // 直接修改 @Observed 对象字段, 让 @ObjectLink 子组件刷新;
    // 不要用新对象替换数组元素 —— ForEach 按 key 复用子组件时不会更新 @Prop。
    const existing = this.participants.find((p: Participant) => p.id === publisherId);
    if (existing != null) {
      if (status != null) {
        existing.status = status;
      }
      if (hasVideo != null) {
        existing.hasVideo = hasVideo;
      }
    } else {
      this.participants = this.participants.concat(
        new Participant(publisherId, status ?? SubStatus.IDLE, hasVideo ?? false)
      );
      this.controllers.set(publisherId, new VideoRenderController());
    }

    // 远端视频轨道到达时绑定到渲染器
    if (hasVideo === true) {
      const track = this.sfu?.remoteVideoTrack(publisherId);
      const controller = this.controllers.get(publisherId);
      if (track != null && controller != null) {
        controller.setVideoTrack(track);
        controller.setMirror(false);
        controller.setScalingMode(ScalingMode.AspectFill);
        this.addLog(`已绑定 ${publisherId} 视频到渲染器`, LogLevel.INFO);
      } else {
        this.addLog(`绑定视频失败 ${publisherId} (track=${track != null}, ctrl=${controller != null})`, LogLevel.WARN);
      }
    }
  }

  private addLog(msg: string, level: LogLevel): void {
    const time = this.nowTime();
    // 最新日志置顶, 方便直接看到 [stats] 诊断行
    this.logs = [new LogEntry(time, msg, level)].concat(this.logs).slice(0, 200);
  }

  private controllerFor(id: string): VideoRenderController {
    let c = this.controllers.get(id);
    if (c == null) {
      c = new VideoRenderController();
      this.controllers.set(id, c);
    }
    return c;
  }

  private nowTime(): string {
    const d = new Date();
    return `${this.pad2(d.getHours())}:${this.pad2(d.getMinutes())}:${this.pad2(d.getSeconds())}`;
  }

  private pad2(n: number): string {
    return n < 10 ? `0${n}` : `${n}`;
  }

  private colorForLog(level: LogLevel): string {
    switch (level) {
      case LogLevel.SUCCESS:
        return '#2E7D32';
      case LogLevel.WARN:
        return '#F9A825';
      case LogLevel.ERROR:
        return '#EF5350';
      case LogLevel.INFO:
      default:
        return '#4FC3F7';
    }
  }

  // ── UI ────────────────────────────────────────────────────

  build() {
    Stack() {
      if (this.inCall) {
        this.callScreen()
      } else {
        this.joinScreen()
      }

      if (this.showSheet) {
        this.sheetOverlay()
      }
    }
    .width('100%')
    .height('100%')
    .backgroundColor('#121212')
  }

  @Builder
  joinScreen() {
    Scroll() {
      Column({ space: 14 }) {
        Column({ space: 6 }) {
          Text('WebRTC SFU')
            .fontSize(26)
            .fontWeight(FontWeight.Bold)
            .fontColor('#FFFFFF')
          Text('多人视频通话 (HarmonyOS)')
            .fontSize(13)
            .fontColor('#9E9E9E')
        }
        .alignItems(HorizontalAlign.Center)
        .width('100%')
        .margin({ top: 40 })

        Column({ space: 12 }) {
          TextInput({ placeholder: '信令服务器 ws://host:port', text: this.serverUrl })
            .onChange((value: string) => { this.serverUrl = value; })
            .height(48)
            .fontSize(14)
            .fontColor('#FFFFFF')
            .placeholderColor('#757575')
            .backgroundColor('#1E1E1E')
            .borderRadius(12)
            .width('100%')

          TextInput({ placeholder: '房间号', text: this.roomId })
            .onChange((value: string) => { this.roomId = value; })
            .height(48)
            .fontSize(14)
            .fontColor('#FFFFFF')
            .placeholderColor('#757575')
            .backgroundColor('#1E1E1E')
            .borderRadius(12)
            .width('100%')

          TextInput({ placeholder: '你的昵称', text: this.name })
            .onChange((value: string) => { this.name = value; })
            .onSubmit(() => { this.join(); })
            .height(48)
            .fontSize(14)
            .fontColor('#FFFFFF')
            .placeholderColor('#757575')
            .backgroundColor('#1E1E1E')
            .borderRadius(12)
            .width('100%')

          Button() {
            if (this.joining) {
              LoadingProgress()
                .width(20)
                .height(20)
                .color('#FFFFFF')
            } else {
              Text('加入房间')
                .fontSize(16)
                .fontColor('#FFFFFF')
            }
          }
          .width('100%')
          .height(48)
          .backgroundColor('#2563EB')
          .borderRadius(24)
          .enabled(!this.joining)
          .onClick(() => { this.join(); })

          if (this.error.length > 0) {
            Text(this.error)
              .fontSize(12)
              .fontColor('#EF5350')
              .width('100%')
          }

          Text('SFU 媒体端口 UDP 5175+ · 信令端口 WS 5174')
            .fontSize(11)
            .fontColor('#757575')
            .width('100%')
            .margin({ top: 4 })
        }
        .width('100%')
      }
      .width('100%')
      .constraintSize({ maxWidth: 420 })
      .padding({ left: 24, right: 24, top: 24, bottom: 24 })
    }
    .width('100%')
    .height('100%')
    .align(Alignment.Top)
    .scrollBar(BarState.Off)
  }

  @Builder
  callScreen() {
    Column() {
      // 顶部栏
      Row() {
        Text(`房间 ${this.roomId} · ${this.participants.length + 1} 人`)
          .fontSize(17)
          .fontWeight(FontWeight.Bold)
          .fontColor('#FFFFFF')
          .maxLines(1)
          .textOverflow({ overflow: TextOverflow.Ellipsis })
          .layoutWeight(1)

        Button() {
          Text('👥')
            .fontSize(18)
        }
        .width(40)
        .height(40)
        .backgroundColor('#1E1E1E')
        .borderRadius(20)
        .onClick(() => { this.showSheet = true; })
      }
      .width('100%')
      .padding({ left: 16, right: 16, top: 10, bottom: 10 })

      // 视频网格
      Grid() {
        GridItem() {
          VideoTile({
            controller: this.localController,
            xcomponentId: 'x-local',
            participant: this.localParticipant,
            isLocal: true,
            localPlaceholder: this.videoEnabled ? '等待视频...' : '摄像头已关闭'
          })
        }

        ForEach(this.participants, (p: Participant) => {
          GridItem() {
            VideoTile({
              controller: this.controllerFor(p.id),
              xcomponentId: 'x-' + p.id,
              participant: p,
              isLocal: false,
              localPlaceholder: ''
            })
          }
        }, (p: Participant) => p.id)
      }
      .columnsTemplate('1fr 1fr')
      .columnsGap(8)
      .rowsGap(8)
      .padding(8)
      .width('100%')
      .layoutWeight(1)

      // 底部控制栏
      this.controlBar()
    }
    .width('100%')
    .height('100%')
  }

  @Builder
  controlBar() {
    Row({ space: 12 }) {
      Button() {
        Text(this.audioEnabled ? '🎤' : '🔇')
          .fontSize(18)
      }
      .width(44)
      .height(44)
      .backgroundColor('#2A2A2A')
      .borderRadius(22)
      .onClick(() => { this.toggleAudio(!this.audioEnabled); })

      Button() {
        Text(this.videoEnabled ? '📹' : '🚫')
          .fontSize(18)
      }
      .width(44)
      .height(44)
      .backgroundColor('#2A2A2A')
      .borderRadius(22)
      .onClick(() => { this.toggleVideo(!this.videoEnabled); })

      Button() {
        Text('🔄')
          .fontSize(18)
      }
      .width(44)
      .height(44)
      .backgroundColor('#2A2A2A')
      .borderRadius(22)
      .onClick(() => { this.switchCamera(); })

      Button() {
        Row({ space: 4 }) {
          Text('📞')
            .fontSize(16)
          Text('挂断')
            .fontSize(15)
            .fontColor('#FFFFFF')
        }
      }
      .height(44)
      .backgroundColor('#E53935')
      .borderRadius(22)
      .padding({ left: 16, right: 16 })
      .onClick(() => { this.leave(); })
    }
    .width('100%')
    .justifyContent(FlexAlign.Center)
    .padding({ top: 8, bottom: 12 })
  }

  @Builder
  sheetOverlay() {
    Column() {
      Blank()
        .layoutWeight(1)
        .onClick(() => { this.showSheet = false; })

      Column() {
        Row() {
          Text('参与者')
            .fontSize(16)
            .fontWeight(FontWeight.Bold)
            .fontColor('#FFFFFF')
          Blank().layoutWeight(1)
          Button('关闭')
            .height(32)
            .fontSize(13)
            .onClick(() => { this.showSheet = false; })
        }
        .width('100%')
        .margin({ bottom: 10 })

        this.participantPanel()
      }
      .width('100%')
      .height(600)
      .backgroundColor('#1E1E1E')
      .borderRadius({ topLeft: 16, topRight: 16 })
      .padding(16)
    }
    .width('100%')
    .height('100%')
    .backgroundColor('#66000000')
  }

  @Builder
  participantPanel() {
    Column({ space: 10 }) {
      // 本地
      Row({ space: 8 }) {
        Text('●')
          .fontSize(10)
          .fontColor('#2E7D32')
        Text('我 (本地)')
          .fontSize(14)
          .fontColor('#FFFFFF')
      }
      .width('100%')

      ForEach(this.participants, (p: Participant) => {
        ParticipantRow({
          participant: p,
          onSubscribe: (id: string): void => { this.subscribe(id); },
          onUnsubscribe: (id: string): void => { this.unsubscribe(id); }
        })
      }, (p: Participant) => p.id)

      Text('手动订阅')
        .fontSize(13)
        .fontWeight(FontWeight.Bold)
        .fontColor('#FFFFFF')

      Row({ space: 8 }) {
        TextInput({ placeholder: '参与者 ID', text: this.manualId })
          .onChange((value: string) => { this.manualId = value; })
          .height(38)
          .fontSize(13)
          .fontColor('#FFFFFF')
          .placeholderColor('#757575')
          .backgroundColor('#2A2A2A')
          .borderRadius(10)
          .layoutWeight(1)
        Button('订阅')
          .height(38)
          .fontSize(13)
          .onClick(() => { this.manualSubscribe(); })
      }
      .width('100%')

      Text('事件日志')
        .fontSize(13)
        .fontWeight(FontWeight.Bold)
        .fontColor('#FFFFFF')

      List() {
        ForEach(this.logs, (log: LogEntry, index: number) => {
          ListItem() {
            Row({ space: 6 }) {
              Text(log.time)
                .fontSize(11)
                .fontColor('#9E9E9E')
              Text(log.msg)
                .fontSize(11)
                .fontColor(this.colorForLog(log.level))
            }
          }
        }, (log: LogEntry, index: number) => `${log.time}-${log.msg}`)
      }
      .width('100%')
      .layoutWeight(1)
      .scrollBar(BarState.Auto)
    }
    .width('100%')
    .layoutWeight(1)
  }
}
