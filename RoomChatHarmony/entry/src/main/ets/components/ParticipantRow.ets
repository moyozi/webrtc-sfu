import { Participant, SubStatus } from '../model/Models';

/** 参与者面板中的一行 (用 @ObjectLink 跟随 status 变化) */
@Component
export struct ParticipantRow {
  @ObjectLink participant: Participant;
  onSubscribe: (id: string) => void = () => {};
  onUnsubscribe: (id: string) => void = () => {};

  private color(): string {
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
    Row({ space: 8 }) {
      Text('●')
        .fontSize(10)
        .fontColor(this.color())
      Text(this.participant.id)
        .fontSize(14)
        .fontColor('#FFFFFF')
        .maxLines(1)
        .textOverflow({ overflow: TextOverflow.Ellipsis })
        .layoutWeight(1)

      if (this.participant.status === SubStatus.SUBSCRIBED) {
        Button('取消')
          .height(28)
          .fontSize(12)
          .onClick(() => {
            this.onUnsubscribe(this.participant.id);
          })
      } else if (this.participant.status === SubStatus.CONNECTING) {
        Text('连接中...')
          .fontSize(11)
          .fontColor('#F9A825')
      } else {
        Button('订阅')
          .height(28)
          .fontSize(12)
          .onClick(() => {
            this.onSubscribe(this.participant.id);
          })
      }
    }
    .width('100%')
  }
}
