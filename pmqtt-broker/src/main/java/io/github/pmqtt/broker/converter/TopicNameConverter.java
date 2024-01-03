package io.github.pmqtt.broker.converter;

import org.apache.pulsar.common.naming.TopicName;

public interface TopicNameConverter {

  TopicName convert(String mqttTopicName);

  String convert(TopicName pulsarTopicName);
}
