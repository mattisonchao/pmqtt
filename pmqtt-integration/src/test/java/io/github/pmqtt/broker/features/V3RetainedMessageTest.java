package io.github.pmqtt.broker.features;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAck;
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

public final class V3RetainedMessageTest extends AbstractPulsarCluster {
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
  public void testRetainedReceived() {
    final String mqttTopicName = "a/b/c" + UUID.randomUUID();
    final Mqtt3BlockingClient bc1 = createAutoLookupClient(mqttTopicName);
    final int messageNum = 5;

    final byte[] retainedPayload = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);

    bc1.connect();

    for (int i = 0; i < messageNum; i++) {
      boolean isSent = false;
      while (!isSent) {
        try {
          bc1.publishWith()
              .topic(mqttTopicName)
              .qos(MqttQos.AT_LEAST_ONCE)
              .payload((i + "").getBytes(StandardCharsets.UTF_8))
              .send();
          isSent = true;
        } catch (Throwable ex) {
          // ignore
        }
      }
    }

    // set retained
    boolean isSent = false;
    while (!isSent) {
      try {
        bc1.publishWith()
            .topic(mqttTopicName)
            .qos(MqttQos.AT_LEAST_ONCE)
            .retain(true)
            .payload(retainedPayload)
            .send();
        isSent = true;
      } catch (Throwable ex) {
        // ignore
      }
    }

    final Mqtt3BlockingClient bc2 = createAutoLookupClient(mqttTopicName);
    bc2.connect();

    final Mqtt3SubAck bc2SubAck =
        bc2.subscribeWith().topicFilter(mqttTopicName).qos(MqttQos.AT_LEAST_ONCE).send();

    Assert.assertEquals(bc2SubAck.getReturnCodes().size(), 1);
    Assert.assertEquals(bc2SubAck.getReturnCodes().get(0).getCode(), 1);

    final Mqtt3BlockingClient.Mqtt3Publishes bc2Publishes =
        bc2.publishes(MqttGlobalPublishFilter.ALL, false);

    final Optional<Mqtt3Publish> bc2Msg1 = bc2Publishes.receive(2, TimeUnit.SECONDS);
    Assert.assertTrue(bc2Msg1.isPresent());
    final Mqtt3Publish mqtt3Publish = bc2Msg1.get();
    Assert.assertTrue(mqtt3Publish.isRetain());
    Assert.assertEquals(mqtt3Publish.getPayloadAsBytes(), retainedPayload);

    final Optional<Mqtt3Publish> bc2Msg2 = bc2Publishes.receive(2, TimeUnit.SECONDS);
    Assert.assertFalse(bc2Msg2.isPresent());
  }
}
