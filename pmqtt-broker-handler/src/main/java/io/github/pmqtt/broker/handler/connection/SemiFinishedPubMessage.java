package io.github.pmqtt.broker.handler.connection;

import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
final class SemiFinishedPubMessage {
  private MqttMessageBuilders.PublishBuilder publishBuilder;
  private boolean noAck;
  private int entryIndex;
  private int batchIndex;
}
