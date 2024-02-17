package io.github.pmqtt.broker.options;

import java.net.InetSocketAddress;
import java.util.Properties;
import javax.validation.constraints.NotNull;
import org.apache.pulsar.broker.ServiceConfiguration;

public record MqttOptions(
    @NotNull InetSocketAddress listenSocketAddress, boolean authenticationEnabled) {

  private static final String MQTT_LISTEN_ADDRESS = "mqttListenAddress";
  private static final String MQTT_LISTEN_PORT = "mqttListenPort";
  private static final String MQTT_AUTHENTICATION_ENABLED = "false";

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
    final InetSocketAddress socketAddress =
        new InetSocketAddress(listenAddress, Integer.parseInt(listenPort));
    return new MqttOptions(socketAddress, authenticationEnabled);
  }
}
