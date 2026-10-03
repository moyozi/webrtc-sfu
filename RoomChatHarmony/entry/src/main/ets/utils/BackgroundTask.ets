import backgroundTaskManager from '@ohos.resourceschedule.backgroundTaskManager';
import wantAgent from '@ohos.app.ability.wantAgent';
import window from '@ohos.window';
import common from '@ohos.app.ability.common';
import { BusinessError } from '@kit.BasicServicesKit';

const TAG: string = '[RoomChat]';

/**
 * 通话长时任务 + 屏幕常亮。
 *
 * 通过 backgroundTaskManager 申请「音频播放/录音」长时任务:
 * 应用退到后台后系统不会回收进程, 摄像头/麦克风/WebRTC 连接继续工作,
 * 保证视频通话不被"杀掉"。
 */
export class CallBackgroundTask {
  private static running: boolean = false;

  static async start(context: common.UIAbilityContext): Promise<void> {
    if (CallBackgroundTask.running) {
      return;
    }
    try {
      const wantAgentInfo: wantAgent.WantAgentInfo = {
        wants: [
          {
            bundleName: context.abilityInfo.bundleName,
            abilityName: context.abilityInfo.name
          }
        ],
        actionType: wantAgent.OperationType.START_ABILITY,
        requestCode: 0,
        actionFlags: [wantAgent.WantAgentFlags.UPDATE_PRESENT_FLAG]
      };
      const agent = await wantAgent.getWantAgent(wantAgentInfo);
      const mode = backgroundTaskManager.BackgroundMode.AUDIO_PLAYBACK |
        backgroundTaskManager.BackgroundMode.AUDIO_RECORDING;
      await backgroundTaskManager.startBackgroundRunning(context, mode, agent);
      CallBackgroundTask.running = true;
      console.info(`${TAG} startBackgroundRunning ok`);
    } catch (e) {
      const err = e as BusinessError;
      console.error(`${TAG} startBackgroundRunning failed: ${err.code} ${err.message}`);
    }
  }

  static async stop(context: common.UIAbilityContext): Promise<void> {
    if (!CallBackgroundTask.running) {
      return;
    }
    try {
      await backgroundTaskManager.stopBackgroundRunning(context);
    } catch (e) {
      const err = e as BusinessError;
      console.error(`${TAG} stopBackgroundRunning failed: ${err.code} ${err.message}`);
    }
    CallBackgroundTask.running = false;
  }

  /** 通话中保持屏幕常亮 */
  static async keepScreenOn(context: common.UIAbilityContext, on: boolean): Promise<void> {
    try {
      const win = await window.getLastWindow(context);
      await win.setWindowKeepScreenOn(on);
    } catch (e) {
      const err = e as BusinessError;
      console.error(`${TAG} setWindowKeepScreenOn failed: ${err.code} ${err.message}`);
    }
  }
}
