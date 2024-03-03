package io.github.pmqtt.broker.features;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.exceptions.Mqtt3ConnAckException;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAckReturnCode;
import io.github.pmqtt.broker.base.AbstractPulsarCluster;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public final class V3GlobalUniqueClientIdWithRejectPolicyTest extends AbstractPulsarCluster {
  @BeforeClass
  void setup() {
    enableMqttCoordinator();
    start();
  }

  @AfterClass
  void cleanup() {
    close();
  }

  @Test
  public void testSingleBroker() {
    // single broker reject
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
    try {
      bc2.connect();
      Assert.fail("unexpected behaviour");
    } catch (Mqtt3ConnAckException ex) {
      final Mqtt3ConnAck mqttMessage = ex.getMqttMessage();
      Assert.assertEquals(mqttMessage.getReturnCode(), Mqtt3ConnAckReturnCode.IDENTIFIER_REJECTED);
    }

    bc.disconnect();

    Awaitility.await().untilAsserted(() -> Assert.assertNotNull(bc2.connect()));

    bc2.disconnect();
  }

  @Test
  public void testMultipleBroker() {
    // single broker reject
    final Pair<String, Integer> hostAndPort1 = getBroker1MqttHostAndPort();
    final String clientId = UUID.randomUUID().toString();

    final Mqtt3BlockingClient bc =
        MqttClient.builder()
            .useMqttVersion3()
            .identifier(clientId)
            .serverHost(hostAndPort1.getLeft())
            .serverPort(hostAndPort1.getRight())
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
    try {
      bc2.connect();
      Assert.fail("unexpected behaviour");
    } catch (Mqtt3ConnAckException ex) {
      final Mqtt3ConnAck mqttMessage = ex.getMqttMessage();
      Assert.assertEquals(mqttMessage.getReturnCode(), Mqtt3ConnAckReturnCode.IDENTIFIER_REJECTED);
    }

    bc.disconnect();

    Awaitility.await()
        .untilAsserted(
            () -> {
              try {
                bc2.connect();
              } catch (Mqtt3ConnAckException ex) {
                Assert.fail("unexpected behaviour");
              }
            });

    bc2.disconnect();
  }
}
