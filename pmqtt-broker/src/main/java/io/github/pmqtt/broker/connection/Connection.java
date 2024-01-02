package io.github.pmqtt.broker.connection;

import static io.github.pmqtt.common.futures.CompletableFutures.wrap;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.github.pmqtt.broker.MqttContext;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.broker.service.TransportCnx;

@ToString
@Slf4j
public class Connection extends ChannelInboundHandlerAdapter implements TransportCnx {

  private final MqttContext mqttContext;

  public Connection(@Nonnull MqttContext mqttContext) {
    this.mqttContext = mqttContext;
  }

  @Override
  public String getClientVersion() {
    return "mqtt-client:";
  }

  @Override
  public String getProxyVersion() {
    return "none";
  }

  @Override
  public SocketAddress clientAddress() {
    return ctx.channel().remoteAddress();
  }

  @Override
  public BrokerService getBrokerService() {
    return null;
  }

  @Override
  public PulsarCommandSender getCommandSender() {
    return null;
  }

  @Override
  public boolean isBatchMessageCompatibleVersion() {
    return false;
  }

  @Override
  public String getAuthRole() {
    return null;
  }

  @Override
  public AuthenticationDataSource getAuthenticationData() {
    return null;
  }

  @Override
  public boolean isActive() {
    return false;
  }

  @Override
  public boolean isWritable() {
    return false;
  }

  @Override
  public void completedSendOperation(boolean isNonPersistentTopic, int msgSize) {}

  @Override
  public void removedProducer(Producer producer) {}

  @Override
  public void closeProducer(Producer producer) {}

  @Override
  public void cancelPublishRateLimiting() {}

  @Override
  public void cancelPublishBufferLimiting() {}

  @Override
  public void disableCnxAutoRead() {}

  @Override
  public void enableCnxAutoRead() {}

  @Override
  public void execute(Runnable runnable) {}

  @Override
  public void removedConsumer(Consumer consumer) {}

  @Override
  public void closeConsumer(Consumer consumer) {}

  @Override
  public boolean isPreciseDispatcherFlowControl() {
    return false;
  }

  @Override
  public Promise<Void> newPromise() {
    return null;
  }

  @Override
  public boolean hasHAProxyMessage() {
    return false;
  }

  @Override
  public HAProxyMessage getHAProxyMessage() {
    return null;
  }

  @Override
  public String clientSourceAddress() {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> checkConnectionLiveness() {
    return null;
  }

  @Override
  public void channelRead(@NotNull ChannelHandlerContext ctx, @NotNull Object msg) {
    this.ctx = ctx;
    if (!(msg instanceof MqttMessage)) {
      ctx.fireExceptionCaught(new UnsupportedMessageTypeException(msg, MqttMessage.class));
      return;
    }
    // --- Check codec
    final DecoderResult result = ((MqttMessage) msg).decoderResult();
    if (result.isFailure()) {
      final var cause = result.cause();
      if (cause instanceof MqttUnacceptableProtocolVersionException) {
        rejectAsync(CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, MqttProperties.NO_PROPERTIES);
      } else {
        ctx.fireExceptionCaught(result.cause());
      }
      return;
    }
    final MqttFixedHeader fixed = ((MqttMessage) msg).fixedHeader();
    if (fixed.messageType() != MqttMessageType.CONNECT
        && STATUS_UPDATER.get(this) != STATUS_ACCEPTED) {
      // After a Network Connection is established by a Client to a Server,
      // the first Packet sent from the Client to the Server MUST be a CONNECT Packet
      rejectAsync(CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, MqttProperties.NO_PROPERTIES);
      return;
    }
    switch (fixed.messageType()) {
      case CONNECT -> handleConnect((MqttConnectMessage) msg);
      default -> throw new UnsupportedOperationException();
    }
  }

  // ------- mqtt message handlers
  private void rejectAsync(
      @Nonnull MqttConnectReturnCode code, @Nonnull MqttProperties properties) {
    if (code == CONNECTION_ACCEPTED) {
      throw new IllegalArgumentException("Reject not support accept return code.");
    }
    if (!STATUS_UPDATER.compareAndSet(this, STATUS_INIT, STATUS_REJECTED)) {
      log.warn(
          "Connection is not expected status while rejecting.  expect: {}, received:{}(non-atomic)",
          STATUS_INIT,
          STATUS_UPDATER.get(this));
      throw new IllegalStateException("Connection is not expected status while rejecting.");
    }
    final var message =
        MqttMessageBuilders.connAck()
            .returnCode(code)
            .properties(properties)
            .sessionPresent(false)
            .build();
    wrap(ctx.writeAndFlush(message))
        .thenCompose(__ -> wrap(ctx.close()))
        .exceptionally(
            ex -> {
              // it shouldn't be happens
              log.error(
                  "Receive an error while rejecting connection.  connection={}",
                  this,
                  Throwables.getRootCause(ex));
              return null;
            });
  }

  private void handleConnect(@Nonnull MqttConnectMessage connectMessage) {
    final MqttConnectVariableHeader var = connectMessage.variableHeader();
    final MqttConnectPayload payload = connectMessage.payload();
    final boolean assignedIdentifier = Strings.isNullOrEmpty(payload.clientIdentifier());
    final String identifier;
    if (assignedIdentifier) {
      identifier = UUID.randomUUID().toString();
    } else {
      identifier = payload.clientIdentifier();
    }
    this.cleanSession = var.isCleanSession();
    this.id = identifier;
    this.assignedId = assignedIdentifier;
    this.version = MqttVersion.fromProtocolNameAndLevel(var.name(), (byte) var.version());
    // We don't support mqtt 5 yet.
    if (connectMessage.variableHeader().version() > MqttVersion.MQTT_3_1_1.protocolLevel()) {
      // The Server MUST respond to the CONNECT Packet with a CONNACK return code 0x01
      // (unacceptable protocol level) and then disconnect the Client if the Protocol
      // Level is not supported by the Server [MQTT-3.1.2-2].
      rejectAsync(CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION, MqttProperties.NO_PROPERTIES);
      return;
    }
    // See https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html
    // In cases where the ClientID is assigned by the Server, return the assigned ClientID.
    // This also lifts the restriction that Server assigned ClientIDs can only be used with Clean
    // Session=1.
    if (cleanSession && assignedId) {
      final MqttConnectReturnCode code =
          version.protocolLevel() >= MqttVersion.MQTT_5.protocolLevel()
              ? CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID
              : CONNECTION_REFUSED_IDENTIFIER_REJECTED;
      rejectAsync(code, var.properties());
      return;
    }
    this.keepAliveTimeSeconds = var.keepAliveTimeSeconds();
    this.connectTime = System.currentTimeMillis();
    // remove the idle state handler for timeout on CONNECT
    ctx.pipeline().remove("idle");
    ctx.pipeline().remove("timeoutOnConnect");

    // keep alive == 0 means NO keep alive, no timeout to handle
    if (connectMessage.variableHeader().keepAliveTimeSeconds() != 0) {

      final var keepAliveTimeout =
          (int) Math.ceil(connectMessage.variableHeader().keepAliveTimeSeconds() * 1.5D);
      ctx.pipeline().addBefore("mqttEncoder", "idle", new IdleStateHandler(keepAliveTimeout, 0, 0));
      ctx.pipeline()
          .addBefore(
              "mqttEncoder",
              "keepAliveHandler",
              new ChannelDuplexHandler() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                  if (evt instanceof IdleStateEvent event) {
                    if (event.state() == IdleState.READER_IDLE) {
                      log.info(
                          "Prepare close the connection by idle. client_id={} client_address={}",
                          id,
                          clientAddress());
                      closeAsync();
                    }
                  }
                }
              });
    }
    final MqttConnAckMessage message =
        MqttMessageBuilders.connAck()
            .returnCode(CONNECTION_ACCEPTED)
            .properties(var.properties())
            .sessionPresent(
                !cleanSession) // todo session present, it should be subscription in pulsar
            .build();
    if (!STATUS_UPDATER.compareAndSet(this, STATUS_INIT, STATUS_ACCEPTED)) {
      // unexpected behaviour
      log.warn(
          "Received an unexpected connect message with accepted status, triggering reconnect.");
      rejectAsync(CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION, MqttProperties.NO_PROPERTIES);
      return;
    }
    wrap(ctx.writeAndFlush(message))
        .thenAccept(
            __ ->
                log.info(
                    "Accepted the connection. client_id={} client_address={}", id, clientAddress()))
        .exceptionally(
            ex -> {
              log.error(
                  "Receive an error while accepting connection.  connection={}",
                  this,
                  Throwables.getRootCause(ex));
              closeAsync();
              return null;
            });
  }

  // ------ mqtt socket properties
  private ChannelHandlerContext ctx;

  // ------ mqtt properties
  private boolean cleanSession;
  private String id;
  private boolean assignedId;
  private MqttVersion version;
  private int keepAliveTimeSeconds;

  // ------ lifecycle
  private long connectTime;
  private volatile int status = STATUS_INIT;
  private static final AtomicIntegerFieldUpdater<Connection> STATUS_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(Connection.class, "status");
  private static final int STATUS_INIT = 0;
  private static final int STATUS_ACCEPTED = 1;
  private static final int STATUS_REJECTED = 2;
  private static final int STATUS_CLOSED = 3;

  private void closeAsync() {
    log.info("Closing the connection. client_id={} client_address={}", id, clientAddress());
    wrap(ctx.close())
        .thenAccept(
            __ -> {
              STATUS_UPDATER.set(this, STATUS_CLOSED);
              log.info(
                  "Closed the connection. client_id={} client_address={}", id, clientAddress());
            })
        .exceptionally(
            ex -> {
              // it shouldn't be happens
              log.error(
                  "Receive an error while close connection.  connection={}",
                  this,
                  Throwables.getRootCause(ex));
              return null;
            });
  }
}
