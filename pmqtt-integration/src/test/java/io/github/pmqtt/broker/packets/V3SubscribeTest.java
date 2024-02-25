package io.github.pmqtt.broker.packets;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttClientIdentifier;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAck;
import io.github.pmqtt.broker.base.AbstractPulsarCluster;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public final class V3SubscribeTest extends AbstractPulsarCluster {

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
  public void testConsumeQos0WithRedirect() {
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

    mqtt3BlockingClient.disconnect();
  }

  @SneakyThrows
  @Test
  public void testConsumeQos1WithRedirect() {
    final String mqttTopicName = "a/b/c" + UUID.randomUUID();
    final String encodedTopicName =
        "persistent://mqtt/default/" + URLEncoder.encode(mqttTopicName, StandardCharsets.UTF_8);
    final Mqtt3BlockingClient mqtt3BlockingClient = createAutoLookupClient(mqttTopicName);
    mqtt3BlockingClient.connect();
    final Mqtt3SubAck ack =
        mqtt3BlockingClient
            .subscribeWith()
            .topicFilter(mqttTopicName)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();
    Assert.assertEquals(ack.getReturnCodes().size(), 1);
    Assert.assertEquals(ack.getReturnCodes().get(0).getCode(), 1);
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

    mqtt3BlockingClient.disconnect();
  }

  @SneakyThrows
  @Test
  public void testPacketIdExhausted() {
    final String mqttTopicName = "a/b/c" + UUID.randomUUID();
    final String encodedTopicName =
        "persistent://mqtt/default/" + URLEncoder.encode(mqttTopicName, StandardCharsets.UTF_8);
    final Mqtt3BlockingClient mqtt3BlockingClient = createAutoLookupClient(mqttTopicName);
    mqtt3BlockingClient.connect();
    final Mqtt3SubAck ack =
        mqtt3BlockingClient
            .subscribeWith()
            .topicFilter(mqttTopicName)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();
    Assert.assertEquals(ack.getReturnCodes().size(), 1);
    Assert.assertEquals(ack.getReturnCodes().get(0).getCode(), 1);

    final var publishes = mqtt3BlockingClient.publishes(MqttGlobalPublishFilter.SUBSCRIBED, true);
    @Cleanup
    final Producer<byte[]> producer =
        broker1.getClient().newProducer().topic(encodedTopicName).create();
    for (int i = 0; i < 65536; i++) {
      producer.sendAsync((i + "").getBytes(StandardCharsets.UTF_8));
    }
    producer.flush();

    final List<Mqtt3Publish> unAckMessages = new ArrayList<>();
    for (int i = 0; i < 65535; i++) {
      final Mqtt3Publish message = publishes.receive();
      unAckMessages.add(message);
      Assert.assertEquals(message.getPayloadAsBytes(), (i + "").getBytes(StandardCharsets.UTF_8));
    }
    final Optional<Mqtt3Publish> receivedMessage = publishes.receive(2, TimeUnit.SECONDS);
    // we should never receive the last message due to insufficient packet id.
    Assert.assertTrue(receivedMessage.isEmpty());
    // Ack messages
    unAckMessages.forEach(Mqtt3Publish::acknowledge);

    final Optional<Mqtt3Publish> receivedMessage1 = publishes.receive(2, TimeUnit.SECONDS);
    Assert.assertTrue(receivedMessage1.isPresent());
    mqtt3BlockingClient.disconnect();
  }

  @DataProvider(name = "batchMode")
  public Object[][] batchMode() {
    return new Object[][] {{true}, {false}};
  }

  @SneakyThrows
  @Test(dataProvider = "batchMode")
  public void testAcknowledgement(boolean withBatch) {
    final String mqttTopicName = "a/b/c" + UUID.randomUUID();
    final String encodedTopicName =
        "persistent://mqtt/default/" + URLEncoder.encode(mqttTopicName, StandardCharsets.UTF_8);
    final Mqtt3BlockingClient mqtt3BlockingClient = createAutoLookupClient(mqttTopicName);
    mqtt3BlockingClient.connect();
    final Mqtt3SubAck ack =
        mqtt3BlockingClient
            .subscribeWith()
            .topicFilter(mqttTopicName)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();
    Assert.assertEquals(ack.getReturnCodes().size(), 1);
    Assert.assertEquals(ack.getReturnCodes().get(0).getCode(), 1);

    final var publishes = mqtt3BlockingClient.publishes(MqttGlobalPublishFilter.SUBSCRIBED);
    @Cleanup
    final Producer<byte[]> producer =
        broker1
            .getClient()
            .newProducer()
            .enableBatching(withBatch)
            .topic(encodedTopicName)
            .create();
    for (int i = 0; i < 20000; i++) {
      producer.sendAsync((i + "").getBytes(StandardCharsets.UTF_8));
    }
    producer.flush();

    for (int i = 0; i < 20000; i++) {
      final Mqtt3Publish message = publishes.receive();
      Assert.assertEquals(message.getPayloadAsBytes(), (i + "").getBytes(StandardCharsets.UTF_8));
    }
    final PulsarAdmin adminClient = broker1.getAdminClient();
    Awaitility.await()
        .untilAsserted(
            () -> {
              final PersistentTopicInternalStats internalStats =
                  adminClient.topics().getInternalStats(encodedTopicName);
              final Optional<MqttClientIdentifier> clientIdentifier =
                  mqtt3BlockingClient.getConfig().getClientIdentifier();
              Assert.assertTrue(clientIdentifier.isPresent());
              final ManagedLedgerInternalStats.CursorStats cursorStats =
                  internalStats.cursors.get(clientIdentifier.get().toString());
              Assert.assertEquals(internalStats.lastConfirmedEntry, cursorStats.markDeletePosition);
            });
    mqtt3BlockingClient.disconnect();
  }
}
