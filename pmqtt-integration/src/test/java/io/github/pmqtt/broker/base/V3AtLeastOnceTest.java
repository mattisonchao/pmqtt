package io.github.pmqtt.broker.base;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public final class V3AtLeastOnceTest extends AbstractPulsarCluster {

  @BeforeClass
  void setup() {
    start();
  }

  @AfterClass
  void cleanup() {
    close();
  }

  @Test
  public void testE2EWithV5() {
    final Pair<String, Integer> hostAndPort = getMqttHostAndPort();
    final Mqtt3BlockingClient bc =
        MqttClient.builder()
            .useMqttVersion3()
            .identifier(UUID.randomUUID().toString())
            .serverHost(hostAndPort.getLeft())
            .serverPort(hostAndPort.getRight())
            .build()
            .toBlocking();
    bc.connect();
    System.out.println(1);
  }
}
