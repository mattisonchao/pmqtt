package io.github.pmqtt.broker.handler;

import io.github.pmqtt.broker.handler.env.Constants;
import io.github.pmqtt.broker.handler.options.MqttOptions;
import io.github.pmqtt.broker.handler.web.base.AbstractEndpoint;
import io.github.pmqtt.broker.handler.web.endpoints.v1.MqttTopicLookup;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.web.WebService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;

@Slf4j
public final class MqttProtocolHandler implements ProtocolHandler {
  private MqttOptions options;
  private MqttContext mqttContext;

  @Override
  public String protocolName() {
    return Constants.PROTOCOL_NAME;
  }

  @Override
  public boolean accept(@NotNull String protocol) {
    return Constants.PROTOCOL_NAME.equalsIgnoreCase(protocol);
  }

  @Override
  public void initialize(@NotNull ServiceConfiguration conf) {
    this.options = MqttOptions.parse(conf);
  }

  @Override
  public @NotNull String getProtocolDataToAdvertise() {
    final InetSocketAddress address = options.listenSocketAddress();
    return String.format("%s:%s", address.getHostName(), address.getPort());
  }

  @Override
  public void start(@NotNull BrokerService service) {
    final PulsarService pulsar = service.getPulsar();
    try {
      this.mqttContext = new MqttContext(pulsar, options);
      initializeDefaultNamespace(service, options);
      initializeWebService(service);
      final String[] lines =
          """
              ,------. ,--.   ,--. ,-----.,--------.,--------.
              |  .--. '|   `.'   |'  .-.  '--.  .--''--.  .--'
              |  '--' ||  |'.'|  ||  | |  |  |  |      |  |
              |  | --' |  |   |  |'  '-'  '-.|  |      |  |
              `--'     `--'   `--' `-----'--'`--'      `--'
              """
              .split("\n");
      for (String line : lines) {
        log.info(line);
      }
      log.info("MQTT Protocol handler has been loaded. the options={}", options);
    } catch (Exception ex) {
      log.error("Start mqtt protocol handler failed. options={}", options, ex);
      throw new IllegalStateException(ex);
    }
  }

  @SuppressWarnings("unchecked")
  private void initializeWebService(@NotNull BrokerService service) throws Exception {
    final String handlerPath = "/mqtt";
    // ----- initialize the static field
    AbstractEndpoint.pulsarService = service.getPulsar();
    AbstractEndpoint.mqttContext = mqttContext;
    final WebService webService = service.getPulsar().getWebService();
    webService.addRestResource(handlerPath, true, new HashMap<>(), true, MqttTopicLookup.class);
    // ----- dynamic load handlers
    final Class<? extends WebService> webServiceClass = webService.getClass();
    final Field serverField = webServiceClass.getDeclaredField("server");
    serverField.setAccessible(true);
    final Server server = (Server) serverField.get(webService);
    final StatisticsHandler handler = (StatisticsHandler) server.getHandler();
    final HandlerCollection collection = (HandlerCollection) handler.getHandler();
    final Field handlersField = webServiceClass.getDeclaredField("handlers");
    handlersField.setAccessible(true);
    final List<Handler> handlers = (List<Handler>) handlersField.get(webService);
    final Optional<Handler> mqttHandler =
        handlers.stream()
            .filter(
                h ->
                    h instanceof ServletContextHandler
                        && ((ServletContextHandler) h).getContextPath().equals(handlerPath))
            .findAny();

    assert mqttHandler.isPresent();
    final Handler mqttManagementHandler = mqttHandler.get();
    final Optional<Handler> contextHandler =
        Arrays.stream(collection.getHandlers())
            .filter(h -> h instanceof ContextHandlerCollection)
            .findFirst();
    assert contextHandler.isPresent();
    final ContextHandlerCollection contextHandlerCollection =
        (ContextHandlerCollection) contextHandler.get();
    contextHandlerCollection.addHandler(mqttManagementHandler);
    mqttManagementHandler.start();
  }

  private void initializeDefaultNamespace(
      @NotNull BrokerService service, @NotNull MqttOptions options) {
    final String defaultTenant = options.defaultTenant();
    final String defaultNamespace = options.defaultNamespace();
    final PulsarResources pulsarResources = service.getPulsar().getPulsarResources();
    final ServiceConfiguration serviceConfiguration = service.getPulsar().getConfiguration();
    final TenantInfo tenantInfo =
        TenantInfo.builder().allowedClusters(Set.of(serviceConfiguration.getClusterName())).build();
    try {
      pulsarResources.getTenantResources().createTenant(defaultTenant, tenantInfo);
    } catch (MetadataStoreException ex) {
      if (ex instanceof MetadataStoreException.AlreadyExistsException) {
        log.info(
            "Jump to creating the default tenant due to already existing. default_tenant={}",
            defaultTenant);
      } else {
        throw new IllegalStateException(ex);
      }
    }
    final NamespaceName ns = NamespaceName.get(defaultTenant, defaultNamespace);
    try {
      final Policies policies = new Policies();
      policies.replication_clusters = Set.of(serviceConfiguration.getClusterName());
      pulsarResources.getNamespaceResources().createPolicies(ns, policies);
    } catch (MetadataStoreException ex) {
      if (ex instanceof MetadataStoreException.AlreadyExistsException) {
        log.info(
            "Jump to creating the default namespace due to already existing. default_namespace={}",
            defaultNamespace);
      } else {
        throw new IllegalStateException(ex);
      }
    }
  }

  @Override
  public @NotNull Map<InetSocketAddress, ChannelInitializer<SocketChannel>>
      newChannelInitializers() {
    return Map.of(options.listenSocketAddress(), mqttContext);
  }

  @Override
  public void close() {}
}
