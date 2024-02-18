package io.github.pmqtt.broker.handler.converter;

import javax.validation.constraints.NotNull;
import org.apache.pulsar.common.naming.TopicName;

public interface TopicNameConverter {

  void init(@NotNull TopicNameConverterConf conf);

  @NotNull TopicName convert(@NotNull String mqttTopicName);

  @NotNull String convert(@NotNull TopicName pulsarTopicName);
}
