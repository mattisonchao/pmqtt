package io.github.pmqtt.broker.handler.exchanger;

import io.netty.handler.codec.mqtt.MqttProperties;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public interface MessageExchanger {

  @NotNull CompletableFuture<Void> exchangeMqttMessage(
      @NotNull String mqttTopic,
      int mqttQos,
      @NotNull byte[] payload,
      boolean retained,
      @Nullable MqttProperties willProperties);
}
