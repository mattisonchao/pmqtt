package io.github.pmqtt.broker.base;

import java.io.File;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;

public abstract class AbstractPulsarCluster implements AutoCloseable {
  private static final String CLUSTER_NAME = "pmqtt";
  private LocalBookkeeperEnsemble ensemble;
  protected ServiceConfiguration broker1Conf = new ServiceConfiguration();
  protected ServiceConfiguration broker2Conf = new ServiceConfiguration();
  protected PulsarService broker1;
  protected PulsarService broker2;

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

  protected void loadBrokerConfiguration(ServiceConfiguration brokerConf) {
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
    // set pmqtt protocol handler
    brokerConf.setProtocolHandlerDirectory(narDir);
    brokerConf.setMessagingProtocols(Set.of("mqtt"));
    final Properties properties = new Properties();
    properties.put("mqttListenPort", String.valueOf(SocketUtils.findAvailableTcpPort()));
    brokerConf.setProperties(properties);
  }

  protected Pair<String, Integer> getMqttHostAndPort() {
    return Pair.of(
        "localhost", Integer.parseInt((String) broker1Conf.getProperty("mqttListenPort")));
  }

  @SneakyThrows
  @Override
  public void close() {
    broker1.close();
    broker2.close();
    ensemble.stop();
  }
}
