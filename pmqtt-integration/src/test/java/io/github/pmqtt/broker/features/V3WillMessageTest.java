package io.github.pmqtt.broker.features;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import io.github.pmqtt.broker.base.AbstractPulsarCluster;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class V3WillMessageTest extends AbstractPulsarCluster {
  @BeforeClass
  void setup() {
    start();
  }

  @AfterClass
  void cleanup() {
    close();
  }

  @SneakyThrows
  @Test
  public void testWillMessageWillBeReceived() {
    final String mqttTopicName = "a/b/c" + UUID.randomUUID();
    final String willTopicName = "a/b/c" + UUID.randomUUID();

    final String willPayload = UUID.randomUUID().toString();
    final Mqtt3BlockingClient bc1 = createAutoLookupClient(mqttTopicName);
    final Mqtt3BlockingClient bc2 = createAutoLookupClient(willTopicName);
    bc2.connect();
    bc2.subscribeWith().topicFilter(willTopicName).qos(MqttQos.AT_LEAST_ONCE).send();
    final Mqtt3BlockingClient.Mqtt3Publishes bc2Publishes =
        bc2.publishes(MqttGlobalPublishFilter.ALL);

    final Mqtt3ConnAck connAck =
        bc1.connectWith()
            .cleanSession(true)
            .willPublish()
            .topic(willTopicName)
            .qos(MqttQos.AT_LEAST_ONCE)
            .payload(willPayload.getBytes(StandardCharsets.UTF_8))
            .applyWillPublish()
            .send();

    Assert.assertFalse(connAck.isSessionPresent());

    bc1.disconnect();

    final Optional<Mqtt3Publish> willMessage = bc2Publishes.receive(2, TimeUnit.SECONDS);
    Assert.assertTrue(willMessage.isPresent());
    Assert.assertEquals(
        willMessage.get().getPayloadAsBytes(), willPayload.getBytes(StandardCharsets.UTF_8));
    bc2.disconnect();
  }
}
