package io.github.pmqtt.broker;

import io.github.pmqtt.broker.env.Constants;
import io.github.pmqtt.broker.options.MqttOptions;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.Map;
import javax.validation.constraints.NotNull;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;

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
    this.mqttContext = new MqttContext(pulsar, options);
  }

  @Override
  public @NotNull Map<InetSocketAddress, ChannelInitializer<SocketChannel>>
      newChannelInitializers() {
    return Map.of(options.listenSocketAddress(), mqttContext);
  }

  @Override
  public void close() {}
}
