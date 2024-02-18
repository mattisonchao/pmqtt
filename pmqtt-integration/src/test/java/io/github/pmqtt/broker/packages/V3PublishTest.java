package io.github.pmqtt.broker.packages;

import com.fasterxml.jackson.databind.JsonNode;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import io.github.pmqtt.broker.base.AbstractPulsarCluster;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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

  @SneakyThrows
  @Test
  public void testPublishWithRedirect() {
    final Pair<String, Integer> hostAndPort = getMqttHostAndPort();
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

    @Cleanup("disconnect")
    final Mqtt3BlockingClient bc =
        MqttClient.builder()
            .useMqttVersion3()
            .identifier(UUID.randomUUID().toString())
            .serverHost(hostAndPort.getLeft())
            .serverPort(hostAndPort.getRight())
            .automaticReconnectWithDefaultConfig()
            .addDisconnectedListener(
                connector -> {
                  try {
                    final String webServiceAddress = broker1.getWebServiceAddress();
                    final HttpRequest request =
                        HttpRequest.newBuilder()
                            .uri(
                                URI.create(
                                    webServiceAddress
                                        + "/mqtt/v1/lookup?topic="
                                        + URLEncoder.encode(mqttTopicName, StandardCharsets.UTF_8)))
                            .GET()
                            .build();
                    final HttpClient httpClient =
                        HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build();
                    final HttpResponse<String> res =
                        httpClient.send(
                            request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                    final String result = res.body();
                    final JsonNode data =
                        ObjectMapperFactory.getMapper().getObjectMapper().readTree(result);
                    connector
                        .getReconnector()
                        .transportConfig()
                        .serverHost(data.get("host").asText())
                        .serverPort(data.get("port").asInt())
                        .applyTransportConfig();
                  } catch (Throwable e) {
                    throw new RuntimeException(e);
                  }
                })
            .build()
            .toBlocking();
    bc.connect();
    final byte[] payload = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    boolean isSent = false;
    while (!isSent) {
      try {
        bc.publishWith().topic(mqttTopicName).qos(MqttQos.AT_LEAST_ONCE).payload(payload).send();
        isSent = true;
      } catch (Throwable ignore) {

      }
    }
    final Message<byte[]> message = consumer.receive();
    final byte[] data = message.getData();
    Assert.assertEquals(data, payload);
  }
}
