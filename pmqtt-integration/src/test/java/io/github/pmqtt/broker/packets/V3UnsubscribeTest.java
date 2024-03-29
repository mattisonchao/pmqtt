package io.github.pmqtt.broker.packets;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAck;
import io.github.pmqtt.broker.base.AbstractPulsarCluster;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Producer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class V3UnsubscribeTest extends AbstractPulsarCluster {
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
  public void testUnsubscribe() {
    final String mqttTopicName = "a/b/c" + UUID.randomUUID();
    final String encodedTopicName =
        "persistent://mqtt/default/" + URLEncoder.encode(mqttTopicName, StandardCharsets.UTF_8);
    final Mqtt3BlockingClient mqtt3BlockingClient = createAutoLookupClient(mqttTopicName);
    mqtt3BlockingClient.connect();
    final Mqtt3SubAck ack =
        mqtt3BlockingClient
            .subscribeWith()
            .topicFilter(mqttTopicName)
            .qos(MqttQos.AT_MOST_ONCE)
            .send();
    Assert.assertEquals(ack.getReturnCodes().size(), 1);
    Assert.assertEquals(ack.getReturnCodes().get(0).getCode(), 0);
    @Cleanup
    final Producer<byte[]> producer =
        broker1.getClient().newProducer().topic(encodedTopicName).create();
    final byte[] payload = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    producer.send(payload);

    final Mqtt3BlockingClient.Mqtt3Publishes publishes =
        mqtt3BlockingClient.publishes(MqttGlobalPublishFilter.SUBSCRIBED, false);
    final Mqtt3Publish publishPacket = publishes.receive();
    final byte[] receivedPayload = publishPacket.getPayloadAsBytes();
    Assert.assertEquals(receivedPayload, payload);

    // unsub the topic
    mqtt3BlockingClient.unsubscribeWith().topicFilter(mqttTopicName).send();

    // send again
    producer.send(payload);

    final Optional<Mqtt3Publish> packet2 = publishes.receive(2, TimeUnit.SECONDS);
    Assert.assertTrue(packet2.isEmpty());

    mqtt3BlockingClient.disconnect();
  }

  @Test
  public void testUnsubThenSubWithCleanSession() {
    // todo: add test
  }
}
