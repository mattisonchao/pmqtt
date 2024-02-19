package io.github.pmqtt.broker.packets;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import io.github.pmqtt.broker.base.AbstractPulsarCluster;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public final class V5ConnectTest extends AbstractPulsarCluster {

  @BeforeClass
  void setup() {
    start();
  }

  @AfterClass
  void cleanup() {
    close();
  }

  @Test
  public void testConnectSuccess() {
    final Pair<String, Integer> hostAndPort = getMqttHostAndPort();
    @Cleanup("disconnect")
    final Mqtt5BlockingClient bc =
        MqttClient.builder()
            .useMqttVersion5()
            .identifier(UUID.randomUUID().toString())
            .serverHost(hostAndPort.getLeft())
            .serverPort(hostAndPort.getRight())
            .build()
            .toBlocking();
    bc.connect();
  }

  @Test
  public void testConnectWithEmptyClientId() {
    final Pair<String, Integer> hostAndPort = getMqttHostAndPort();
    final Mqtt5BlockingClient bc =
        MqttClient.builder()
            .useMqttVersion5()
            .serverHost(hostAndPort.getLeft())
            .serverPort(hostAndPort.getRight())
            .build()
            .toBlocking();
    bc.connect(); // broker will give it a random client id
  }
}
