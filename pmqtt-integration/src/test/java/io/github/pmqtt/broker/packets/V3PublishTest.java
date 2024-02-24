package io.github.pmqtt.broker.packets;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.exceptions.MqttSessionExpiredException;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import io.github.pmqtt.broker.base.AbstractPulsarCluster;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public final class V3PublishTest extends AbstractPulsarCluster {

  @BeforeClass
  void setup() {
    start();
  }

  @AfterClass
  void cleanup() {
    close();
  }

  @DataProvider(name = "mqttQosProvider")
  public static Object[][] mqttQosProvider() {
    return new Object[][] {{MqttQos.AT_MOST_ONCE}, {MqttQos.AT_LEAST_ONCE}};
  }

  @SneakyThrows
  @Test(dataProvider = "mqttQosProvider")
  public void testPublishWithRedirect(MqttQos qos) {
    final String mqttTopicName = "a/b/c" + UUID.randomUUID();
    final String encodedTopicName =
        "persistent://mqtt/default/" + URLEncoder.encode(mqttTopicName, StandardCharsets.UTF_8);

    @Cleanup
    final Consumer<byte[]> consumer =
        broker1
            .getClient()
            .newConsumer()
            .topic(encodedTopicName)
            .subscriptionName("sub-1")
            .subscribe();
    final Mqtt3BlockingClient mqtt3BlockingClient = createAutoLookupClient(mqttTopicName);
    mqtt3BlockingClient.connect();
    final byte[] payload = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    boolean isSent = false;
    while (!isSent) {
      try {
        mqtt3BlockingClient.publishWith().topic(mqttTopicName).qos(qos).payload(payload).send();
        isSent = true;
      } catch (Throwable ignore) {

      }
    }
    final Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
    if (message == null) {
      if (qos == MqttQos.AT_MOST_ONCE) {
        // success
        return;
      } else {
        Assert.fail("At least once can't lost message");
      }
    }
    final byte[] data = message.getData();
    Assert.assertEquals(data, payload);
    mqtt3BlockingClient.disconnect();
  }

  // --- failed cases
  @SneakyThrows
  @Test
  public void testUnsupportedPubQos() {
    final String mqttTopicName = "a/b/c" + UUID.randomUUID();
    final Mqtt3BlockingClient mqtt3BlockingClient = createAutoLookupClient(mqttTopicName);
    mqtt3BlockingClient.connect();
    final byte[] payload = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    try {
      mqtt3BlockingClient
          .publishWith()
          .topic(mqttTopicName)
          .qos(MqttQos.EXACTLY_ONCE)
          .payload(payload)
          .send();
      Assert.fail("unexpected behaviour");
    } catch (MqttSessionExpiredException ex) {
      // ignore
    }
  }
}
