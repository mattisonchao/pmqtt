package io.github.pmqtt.broker.handler.converter;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import javax.validation.constraints.NotNull;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

final class URLEncodeTopicNameConverter implements TopicNameConverter {
  private String defaultTenant;
  private String defaultNamespace;
  private String defaultTopicPrefix;

  @Override
  public void init(@NotNull TopicNameConverterConf conf) {
    this.defaultTenant = conf.defaultTenant();
    this.defaultNamespace = conf.defaultNamespace();
    this.defaultTopicPrefix = "persistent://" + defaultTenant + "/" + defaultNamespace + "/";
  }

  @Override
  public @NotNull TopicName convert(@NotNull String mqttTopicName) {
    Objects.requireNonNull(mqttTopicName);
    final boolean pulsarTopicFormat =
        Arrays.stream(TopicDomain.values())
            .anyMatch(domain -> mqttTopicName.startsWith(domain.value()));
    if (pulsarTopicFormat) {
      return TopicName.get(mqttTopicName);
    }
    return TopicName.get(
        TopicDomain.persistent.value(),
        defaultTenant,
        defaultNamespace,
        URLEncoder.encode(mqttTopicName, StandardCharsets.UTF_8));
  }

  @Override
  public @NotNull String convert(@NotNull TopicName pulsarTopicName) {
    Objects.requireNonNull(pulsarTopicName);
    final String topicNameStr = pulsarTopicName.toString();
    // if default
    if (topicNameStr.startsWith(defaultTopicPrefix)) {
      final String encodedTopicName = topicNameStr.replace(defaultTopicPrefix, "");
      return URLDecoder.decode(encodedTopicName, StandardCharsets.UTF_8);
    }
    return topicNameStr;
  }
}
