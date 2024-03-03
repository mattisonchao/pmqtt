package io.github.pmqtt.broker.features;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import io.github.pmqtt.broker.base.AbstractPulsarCluster;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public final class V3GlobalUniqueClientIdWithKickOutExistingPolicyTest
    extends AbstractPulsarCluster {
  @BeforeClass
  void setup() {
    enableMqttCoordinator();
    start();
  }

  @AfterClass
  void cleanup() {
    close();
  }

  @Override
  protected void configureMqttConfig(Properties properties) {
    properties.put("mqttDuplicatedClientIdPolicy", "kick_out_existing");
  }

  @Test
  public void testSingleBroker() {
    final Pair<String, Integer> hostAndPort = getBroker1MqttHostAndPort();
    final String clientId = UUID.randomUUID().toString();

    final Mqtt3BlockingClient bc =
        MqttClient.builder()
            .useMqttVersion3()
            .identifier(clientId)
            .serverHost(hostAndPort.getLeft())
            .serverPort(hostAndPort.getRight())
            .build()
            .toBlocking();
    bc.connect();

    final Mqtt3BlockingClient bc2 =
        MqttClient.builder()
            .useMqttVersion3()
            .identifier(clientId)
            .serverHost(hostAndPort.getLeft())
            .serverPort(hostAndPort.getRight())
            .build()
            .toBlocking();
    bc2.connect();

    Awaitility.await().untilAsserted(() -> Assert.assertFalse(bc.getState().isConnected()));

    bc.connect();

    Awaitility.await().untilAsserted(() -> Assert.assertFalse(bc2.getState().isConnected()));

    bc.disconnect();
  }

  @Test
  public void testMultipleBroker() {
    final Pair<String, Integer> hostAndPort = getBroker1MqttHostAndPort();
    final String clientId = UUID.randomUUID().toString();

    final Mqtt3BlockingClient bc =
        MqttClient.builder()
            .useMqttVersion3()
            .identifier(clientId)
            .serverHost(hostAndPort.getLeft())
            .serverPort(hostAndPort.getRight())
            .build()
            .toBlocking();
    bc.connect();

    final Pair<String, Integer> hostAndPort2 = getBroker2MqttHostAndPort();

    final Mqtt3BlockingClient bc2 =
        MqttClient.builder()
            .useMqttVersion3()
            .identifier(clientId)
            .serverHost(hostAndPort2.getLeft())
            .serverPort(hostAndPort2.getRight())
            .build()
            .toBlocking();
    bc2.connect();

    Awaitility.await().untilAsserted(() -> Assert.assertFalse(bc.getState().isConnected()));

    bc.connect();

    Awaitility.await().untilAsserted(() -> Assert.assertFalse(bc2.getState().isConnected()));

    bc.disconnect();
  }
}
