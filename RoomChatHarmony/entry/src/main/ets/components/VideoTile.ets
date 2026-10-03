import { VideoRenderController } from '@ohos/webrtc';
import { Participant, SubStatus } from '../model/Models';

/**
 * 单个视频画面 — XComponent + 占位文案 + 底部名称标签。
 *
 * 用 @ObjectLink 接收 @Observed 的 Participant, 这样在 ForEach 复用时,
 * status / hasVideo 变化也能刷新 (普通 @Prop 在 key 不变时不会更新,
 * 会导致占位层一直盖住已经渲染出来的画面)。
 */
@Component
export struct VideoTile {
  // XComponent 控制器 (命令式绑定轨道, 不需要观察)
  controller: VideoRenderController = new VideoRenderController();
  @ObjectLink participant: Participant;
  @Prop xcomponentId: string = '';
  @Prop isLocal: boolean = false;
  // 本地画面关闭时的占位文案由外部决定
  @Prop localPlaceholder: string = '';

  private placeholder(): string {
    if (this.isLocal) {
      return this.localPlaceholder;
    }
    switch (this.participant.status) {
      case SubStatus.CONNECTING:
        return '连接中...';
      case SubStatus.ERROR:
        return '连接失败';
      case SubStatus.SUBSCRIBED:
        return '等待视频...';
      case SubStatus.IDLE:
      default:
        return '未订阅';
    }
  }

  private accent(): string {
    if (this.isLocal) {
      return '#4CAF50';
    }
    switch (this.participant.status) {
      case SubStatus.CONNECTING:
        return '#F9A825';
      case SubStatus.SUBSCRIBED:
        return '#2E7D32';
      case SubStatus.ERROR:
        return '#EF5350';
      case SubStatus.IDLE:
      default:
        return '#9E9E9E';
    }
  }

  build() {
    Stack({ alignContent: Alignment.BottomStart }) {
      XComponent({
        id: this.xcomponentId,
        type: XComponentType.SURFACE,
        controller: this.controller
      })
        .width('100%')
        .height('100%')
        .backgroundColor('#000000')

      if (!this.participant.hasVideo) {
        Column() {
          Text(this.placeholder())
            .fontSize(12)
            .fontColor('#FFFFFF99')
        }
        .width('100%')
        .height('100%')
        .justifyContent(FlexAlign.Center)
        .alignItems(HorizontalAlign.Center)
        .backgroundColor('#202124')
      }

      Row() {
        Text(this.participant.id)
          .fontSize(12)
          .fontColor('#FFFFFF')
          .maxLines(1)
          .textOverflow({ overflow: TextOverflow.Ellipsis })
          .layoutWeight(1)

        if (this.isLocal) {
          Text('本地')
            .fontSize(10)
            .fontColor(this.accent())
        }
      }
      .width('100%')
      .padding({ left: 8, right: 8, top: 4, bottom: 4 })
      .backgroundColor('#73000000')
    }
    .width('100%')
    .aspectRatio(16 / 9)
    .borderRadius(12)
    .clip(true)
    .backgroundColor('#000000')
  }
}
