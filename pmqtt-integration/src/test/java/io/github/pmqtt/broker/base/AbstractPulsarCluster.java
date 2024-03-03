package io.github.pmqtt.broker.base;

import com.fasterxml.jackson.databind.JsonNode;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import java.io.File;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;

public abstract class AbstractPulsarCluster implements AutoCloseable {
  private static final String CLUSTER_NAME = "pmqtt";
  private LocalBookkeeperEnsemble ensemble;
  protected ServiceConfiguration broker1Conf = new ServiceConfiguration();
  protected ServiceConfiguration broker2Conf = new ServiceConfiguration();
  protected PulsarService broker1;
  protected PulsarService broker2;

  private boolean enableMqttCoordinator = false;

  @SneakyThrows
  protected void start() {
    ensemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
    ensemble.start();
    loadBrokerConfiguration(broker1Conf);
    loadBrokerConfiguration(broker2Conf);
    // deep copy
    broker1 = new PulsarService(broker1Conf);
    broker2 = new PulsarService(broker2Conf);
    broker1.start();
    broker2.start();
  }

  protected void enableMqttCoordinator() {
    enableMqttCoordinator = true;
  }

  private void loadBrokerConfiguration(ServiceConfiguration brokerConf) {
    final String projectRootPath =
        System.getProperty("user.dir").replace(File.separator + "pmqtt-integration", "");
    final String narDir =
        projectRootPath
            + File.separator
            + "pmqtt-broker-handler"
            + File.separator
            + "build"
            + File.separator
            + "libs";
    // init broker configuration
    brokerConf.setClusterName(CLUSTER_NAME);
    brokerConf.setAdvertisedAddress("localhost");
    brokerConf.setWebServicePort(Optional.of(0));
    brokerConf.setBrokerServicePort(Optional.of(0));
    brokerConf.setMetadataStoreUrl("zk:127.0.0.1:" + ensemble.getZookeeperPort());
    brokerConf.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + ensemble.getZookeeperPort());
    brokerConf.setMaxUnackedMessagesPerConsumer(0);
    // set pmqtt protocol handler
    brokerConf.setProtocolHandlerDirectory(narDir);
    brokerConf.setMessagingProtocols(Set.of("mqtt"));
    final Properties properties = new Properties();
    properties.put("mqttListenPort", String.valueOf(SocketUtils.findAvailableTcpPort()));
    if (enableMqttCoordinator) {
      properties.put("mqttCoordinatorEnabled", "true");
    }
    brokerConf.setProperties(properties);
  }

  protected Pair<String, Integer> getMqttHostAndPort() {
    return Pair.of(
        "localhost", Integer.parseInt((String) broker1Conf.getProperty("mqttListenPort")));
  }

  protected Pair<String, Integer> getBroker1MqttHostAndPort() {
    return Pair.of(
        "localhost", Integer.parseInt((String) broker1Conf.getProperty("mqttListenPort")));
  }

  protected Pair<String, Integer> getBroker2MqttHostAndPort() {
    return Pair.of(
        "localhost", Integer.parseInt((String) broker2Conf.getProperty("mqttListenPort")));
  }

  @SneakyThrows
  @Override
  public void close() {
    broker1.close();
    broker2.close();
    ensemble.stop();
  }

  protected Mqtt3BlockingClient createAutoLookupClient(String mqttTopicName) {
    final Pair<String, Integer> hostAndPort = getMqttHostAndPort();
    return MqttClient.builder()
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
  }
}
