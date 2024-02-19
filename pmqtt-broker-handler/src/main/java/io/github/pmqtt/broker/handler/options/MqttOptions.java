package io.github.pmqtt.broker.handler.options;

import java.net.InetSocketAddress;
import java.util.Properties;
import javax.validation.constraints.NotNull;
import org.apache.pulsar.broker.ServiceConfiguration;

public record MqttOptions(
    @NotNull InetSocketAddress listenSocketAddress,
    boolean authenticationEnabled,
    boolean authorizationEnabled,
    @NotNull String defaultTenant,
    @NotNull String defaultNamespace,
    @NotNull String mqttTopicNameConverter) {

  // ----- converter
  private static final String MQTT_TOPIC_NAME_CONVERTER = "mqttTopicNameConverter";
  private static final String MQTT_DEFAULT_TENANT = "mqttDefaultTenant";
  private static final String MQTT_DEFAULT_NAMESPACE = "mqttDefaultNamespace";
  // ----- service
  private static final String MQTT_LISTEN_ADDRESS = "mqttListenAddress";
  private static final String MQTT_LISTEN_PORT = "mqttListenPort";
  // ----- authentication
  private static final String MQTT_AUTHENTICATION_ENABLED = "false";
  private static final String MQTT_AUTHORIZATION_ENABLED = "false";

  public static @NotNull MqttOptions parse(@NotNull ServiceConfiguration brokerConfiguration) {
    final Properties properties =
        brokerConfiguration.getProperties() == null
            ? new Properties()
            : brokerConfiguration.getProperties();
    final String listenAddress = (String) properties.getOrDefault(MQTT_LISTEN_ADDRESS, "127.0.0.1");
    final String listenPort = (String) properties.getOrDefault(MQTT_LISTEN_PORT, "3306");
    final boolean authenticationEnabled =
        Boolean.parseBoolean(
            (String) properties.getOrDefault(MQTT_AUTHENTICATION_ENABLED, "false"));
    final boolean authorizationEnabled =
        Boolean.parseBoolean((String) properties.getOrDefault(MQTT_AUTHORIZATION_ENABLED, "false"));
    final String defaultTenant = (String) properties.getOrDefault(MQTT_DEFAULT_TENANT, "mqtt");
    final String defaultNamespace =
        (String) properties.getOrDefault(MQTT_DEFAULT_NAMESPACE, "default");
    final String mqttTopicNameConverter =
        (String)
            properties.getOrDefault(
                MQTT_TOPIC_NAME_CONVERTER,
                "io.github.pmqtt.broker.handler.converter.URLEncodeTopicNameConverter");

    final InetSocketAddress socketAddress =
        new InetSocketAddress(listenAddress, Integer.parseInt(listenPort));
    return new MqttOptions(
        socketAddress,
        authenticationEnabled,
        authorizationEnabled,
        defaultTenant,
        defaultNamespace,
        mqttTopicNameConverter);
  }
}
