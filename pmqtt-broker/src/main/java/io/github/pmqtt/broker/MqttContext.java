package io.github.pmqtt.broker;

import io.github.pmqtt.broker.connection.Connection;
import io.github.pmqtt.broker.options.MqttOptions;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import javax.validation.constraints.NotNull;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;

public final class MqttContext extends ChannelInitializer<SocketChannel> {
  public static final String CONNECT_IDLE_NAME = "connectIdle";
  public static final String CONNECT_TIMEOUT_NAME = "connectTimeout";

  @Getter private final PulsarService pulsarService;
  @Getter private final MqttOptions mqttOptions;

  public MqttContext(@NotNull PulsarService pulsarService, @NotNull MqttOptions mqttOptions) {
    this.pulsarService = pulsarService;
    this.mqttOptions = mqttOptions;
  }

  @Override
  protected void initChannel(SocketChannel ch) {
    final ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast("mqttEncoder", MqttEncoder.INSTANCE);
    pipeline.addLast("mqttDecoder", new MqttDecoder());
    pipeline.addLast(CONNECT_IDLE_NAME, new IdleStateHandler(90, 0, 0));
    pipeline.addLast(
        CONNECT_TIMEOUT_NAME,
        new ChannelDuplexHandler() {
          @Override
          public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
            if (event instanceof IdleStateEvent) {
              IdleStateEvent e = (IdleStateEvent) event;
              if (e.state() == IdleState.READER_IDLE) {
                ctx.channel().close();
              }
            }
            super.userEventTriggered(ctx, event);
          }
        });
    pipeline.addLast("mqtt", new Connection(this));
  }
}
