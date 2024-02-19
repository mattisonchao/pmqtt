package io.github.pmqtt.broker.packets;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import io.github.pmqtt.broker.base.AbstractPulsarCluster;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public final class V3ConnectTest extends AbstractPulsarCluster {

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
    final Mqtt3BlockingClient bc =
        MqttClient.builder()
            .useMqttVersion3()
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
    final Mqtt3BlockingClient bc =
        MqttClient.builder()
            .useMqttVersion3()
            .serverHost(hostAndPort.getLeft())
            .serverPort(hostAndPort.getRight())
            .build()
            .toBlocking();
    bc.connect();
  }
}
