package io.github.pmqtt.broker.handler.exchanger;

import io.github.pmqtt.broker.handler.MqttContext;
import io.github.pmqtt.broker.handler.converter.TopicNameConverter;
import io.github.pmqtt.broker.handler.exceptions.ExchangerException;
import io.netty.handler.codec.mqtt.MqttProperties;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
final class PulsarInternalMessageExchanger implements MessageExchanger {
  private final MqttContext mqttContext;

  PulsarInternalMessageExchanger(@NotNull MqttContext mqttContext) {
    this.mqttContext = mqttContext;
  }

  @Override
  public CompletableFuture<Void> exchangeMqttMessage(
      @NotNull String mqttTopic,
      int mqttQos,
      byte[] payload,
      boolean retained,
      @Nullable MqttProperties willProperties) {
    final PulsarClient client;
    try {
      client = mqttContext.getPulsarService().getClient();
    } catch (PulsarServerException ex) {
      return CompletableFuture.failedFuture(new ExchangerException(ex));
    }
    final TopicNameConverter converter = mqttContext.getConverter();
    final TopicName pulsarTopicName = converter.convert(mqttTopic);
    return client
        .newProducer()
        .topic(pulsarTopicName.toString())
        .createAsync()
        .thenCompose(
            producer -> producer.sendAsync(payload).thenCompose(__ -> producer.closeAsync()))
        .exceptionally(
            ex -> {
              final Throwable rc = FutureUtil.unwrapCompletionException(ex);
              log.error(
                  "Receive an error when exchanging mqtt message. mqtt_topic={} mqtt_qos={} retained={} properties={}",
                  mqttTopic,
                  mqttQos,
                  retained,
                  willProperties,
                  ex);
              throw new ExchangerException(rc);
            });
  }
}
