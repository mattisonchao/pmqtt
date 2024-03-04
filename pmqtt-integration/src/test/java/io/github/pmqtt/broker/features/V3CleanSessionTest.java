package io.github.pmqtt.broker.features;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public final class V3CleanSessionTest extends AbstractPulsarCluster {
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
  public void testCleanSessionTrue() {
    final String mqttTopicName = "a/b/c" + UUID.randomUUID();
    final String encodedTopicName =
        "persistent://mqtt/default/" + URLEncoder.encode(mqttTopicName, StandardCharsets.UTF_8);
    final String clientId = UUID.randomUUID().toString();
    final Mqtt3BlockingClient bc1 = createAutoLookupClient(mqttTopicName, clientId);

    final Mqtt3ConnAck connAck = bc1.connectWith().cleanSession(true).send();
    Assert.assertFalse(connAck.isSessionPresent());

    final Mqtt3SubAck ack =
        bc1.subscribeWith().topicFilter(mqttTopicName).qos(MqttQos.AT_LEAST_ONCE).send();
    Assert.assertEquals(ack.getReturnCodes().size(), 1);
    Assert.assertEquals(ack.getReturnCodes().get(0).getCode(), 1);

    @Cleanup
    final Producer<byte[]> producer =
        broker1.getClient().newProducer().topic(encodedTopicName).create();
    final Mqtt3BlockingClient.Mqtt3Publishes publishes =
        bc1.publishes(MqttGlobalPublishFilter.SUBSCRIBED, true);

    final byte[] payload = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    producer.send(payload);

    final Mqtt3Publish publishPacket = publishes.receive();
    publishPacket.acknowledge();
    final byte[] receivedPayload = publishPacket.getPayloadAsBytes();
    Assert.assertEquals(receivedPayload, payload);

    final PulsarAdmin admin = broker1.getAdminClient();
    Awaitility.await()
        .untilAsserted(
            () -> {
              final PersistentTopicInternalStats internalStats =
                  admin.topics().getInternalStats(encodedTopicName);
              final ManagedLedgerInternalStats.CursorStats cursorStats =
                  internalStats.cursors.get(clientId);
              Assert.assertEquals(cursorStats.markDeletePosition, internalStats.lastConfirmedEntry);
            });
    bc1.disconnect();
    //  --- send message after client disconnect

    final byte[] payload2 = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    producer.send(payload2);

    final Mqtt3BlockingClient bc2 = createAutoLookupClient(mqttTopicName, clientId);
    Assert.assertFalse(bc2.connectWith().cleanSession(true).send().isSessionPresent());
    final Mqtt3SubAck bc2SubAck =
        bc2.subscribeWith().topicFilter(mqttTopicName).qos(MqttQos.AT_LEAST_ONCE).send();

    Assert.assertEquals(bc2SubAck.getReturnCodes().size(), 1);
    Assert.assertEquals(bc2SubAck.getReturnCodes().get(0).getCode(), 1);
    final Mqtt3BlockingClient.Mqtt3Publishes bc2Publishes =
        bc2.publishes(MqttGlobalPublishFilter.ALL, false);
    Optional<Mqtt3Publish> bc2Msg1 = bc2Publishes.receive(2, TimeUnit.SECONDS);
    Assert.assertFalse(bc2Msg1.isPresent());
  }

  @SneakyThrows
  @Test
  public void testCleanSessionFalse() {
    final String mqttTopicName = "a/b/c" + UUID.randomUUID();
    final String encodedTopicName =
        "persistent://mqtt/default/" + URLEncoder.encode(mqttTopicName, StandardCharsets.UTF_8);
    final String clientId = UUID.randomUUID().toString();
    final Mqtt3BlockingClient bc1 = createAutoLookupClient(mqttTopicName, clientId);

    final Mqtt3ConnAck connAck = bc1.connectWith().cleanSession(true).send();
    Assert.assertFalse(connAck.isSessionPresent());

    final Mqtt3SubAck ack =
        bc1.subscribeWith().topicFilter(mqttTopicName).qos(MqttQos.AT_LEAST_ONCE).send();
    Assert.assertEquals(ack.getReturnCodes().size(), 1);
    Assert.assertEquals(ack.getReturnCodes().get(0).getCode(), 1);

    @Cleanup
    final Producer<byte[]> producer =
        broker1.getClient().newProducer().topic(encodedTopicName).create();
    final Mqtt3BlockingClient.Mqtt3Publishes publishes =
        bc1.publishes(MqttGlobalPublishFilter.SUBSCRIBED, true);

    final byte[] payload = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    producer.send(payload);

    final Mqtt3Publish publishPacket = publishes.receive();
    publishPacket.acknowledge();
    final byte[] receivedPayload = publishPacket.getPayloadAsBytes();
    Assert.assertEquals(receivedPayload, payload);

    final PulsarAdmin admin = broker1.getAdminClient();
    Awaitility.await()
        .untilAsserted(
            () -> {
              final PersistentTopicInternalStats internalStats =
                  admin.topics().getInternalStats(encodedTopicName);
              final ManagedLedgerInternalStats.CursorStats cursorStats =
                  internalStats.cursors.get(clientId);
              Assert.assertEquals(cursorStats.markDeletePosition, internalStats.lastConfirmedEntry);
            });
    bc1.disconnect();
    //  --- send message after client disconnect

    final byte[] payload2 = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    producer.send(payload2);

    final Mqtt3BlockingClient bc2 = createAutoLookupClient(mqttTopicName, clientId);
    Assert.assertTrue(bc2.connectWith().cleanSession(false).send().isSessionPresent());
    final Mqtt3SubAck bc2SubAck =
        bc2.subscribeWith().topicFilter(mqttTopicName).qos(MqttQos.AT_LEAST_ONCE).send();

    Assert.assertEquals(bc2SubAck.getReturnCodes().size(), 1);
    Assert.assertEquals(bc2SubAck.getReturnCodes().get(0).getCode(), 1);
    final Mqtt3BlockingClient.Mqtt3Publishes bc2Publishes =
        bc2.publishes(MqttGlobalPublishFilter.ALL, false);
    Optional<Mqtt3Publish> bc2Msg1 = bc2Publishes.receive(2, TimeUnit.SECONDS);
    Assert.assertTrue(bc2Msg1.isPresent());
  }
}
