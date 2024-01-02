package io.github.pmqtt.broker.options;

import java.net.InetSocketAddress;
import javax.validation.constraints.NotNull;
import org.apache.pulsar.broker.ServiceConfiguration;

public record MqttOptions(@NotNull InetSocketAddress listenSocketAddress) {

  private static final String MQTT_LISTEN_ADDRESS = "mqttListenAddress";
  private static final String MQTT_LISTEN_PORT = "mqttListenPort";

  public static @NotNull MqttOptions parse(@NotNull ServiceConfiguration brokerConfiguration) {
    final String listenAddress =
        (String) brokerConfiguration.getProperties().getOrDefault(MQTT_LISTEN_ADDRESS, "127.0.0.1");
    final String listenPort =
        (String) brokerConfiguration.getProperties().getOrDefault(MQTT_LISTEN_PORT, "3306");
    final InetSocketAddress socketAddress =
        new InetSocketAddress(listenAddress, Integer.parseInt(listenPort));
    return new MqttOptions(socketAddress);
  }
}
