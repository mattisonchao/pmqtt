package io.github.pmqtt.broker.handler.connection;

import static io.github.pmqtt.broker.handler.utils.future.CompletableFutures.wrap;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED_5;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_MOVED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_TOPIC_NAME_INVALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNSPECIFIED_ERROR;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.github.pmqtt.broker.handler.MqttContext;
import io.github.pmqtt.broker.handler.converter.TopicNameConverter;
import io.github.pmqtt.broker.handler.exceptions.UnConnectedException;
import io.github.pmqtt.broker.handler.exceptions.UnauthorizedException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
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
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.SubscriptionOption;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.CommandTopicMigrated;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProducerAccessMode;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.roaringbitmap.RoaringBitmap;

@ToString
@Slf4j
public class Connection extends ChannelInboundHandlerAdapter
    implements TransportCnx, PulsarCommandSender {

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
    return mqttContext.getPulsarService().getBrokerService();
  }

  @Override
  public PulsarCommandSender getCommandSender() {
    return this;
  }

  @Override
  public boolean isBatchMessageCompatibleVersion() {
    return true;
  }

  @Override
  public String getAuthRole() {
    return "";
  }

  @Override
  public AuthenticationDataSource getAuthenticationData() {
    return new AuthenticationDataCommand("");
  }

  @Override
  public boolean isActive() {
    return ctx.channel().isWritable();
  }

  @Override
  public boolean isWritable() {
    return ctx.channel().isWritable();
  }

  @Override
  public void completedSendOperation(boolean isNonPersistentTopic, int msgSize) {
    // todo
  }

  @Override
  public void removedProducer(Producer producer) {
    // todo
  }

  @Override
  public void closeProducer(Producer producer) {
    // todo
  }

  @Override
  public void cancelPublishRateLimiting() {}

  @Override
  public void cancelPublishBufferLimiting() {}

  @Override
  public void disableCnxAutoRead() {
    ctx.channel().config().setAutoRead(false);
  }

  @Override
  public void enableCnxAutoRead() {
    ctx.channel().config().setAutoRead(true);
  }

  @Override
  public void execute(Runnable runnable) {
    ctx.channel().eventLoop().execute(runnable);
  }

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
    return ctx.newPromise();
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
    if (ctx.channel().remoteAddress() instanceof InetSocketAddress remoteAddress) {
      return remoteAddress.getAddress().getHostAddress();
    } else {
      return "";
    }
  }

  @Override
  public CompletableFuture<Boolean> checkConnectionLiveness() {
    // we don't need support this
    return CompletableFuture.completedFuture(true);
  }

  //  -------- channel section
  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    this.ctx = ctx;
    super.channelRegistered(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    closeAsync(CONNECTION_REFUSED_UNSPECIFIED_ERROR.byteValue());
    super.channelInactive(ctx);
  }

  @Override
  public void channelRead(@NotNull ChannelHandlerContext ctx, @NotNull Object msg) {
    if (!(msg instanceof MqttMessage)) {
      ctx.fireExceptionCaught(new UnsupportedMessageTypeException(msg, MqttMessage.class));
      return;
    }
    // --- Check codec
    final DecoderResult result = ((MqttMessage) msg).decoderResult();
    if (result.isFailure()) {
      final var cause = result.cause();
      if (cause instanceof MqttUnacceptableProtocolVersionException) {
        connectRejectAsync(
            CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, MqttProperties.NO_PROPERTIES);
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
      connectRejectAsync(
          CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, MqttProperties.NO_PROPERTIES);
      return;
    }
    try {
      switch (fixed.messageType()) {
        case CONNECT -> handleConnect((MqttConnectMessage) msg);
        case PUBLISH -> {
          checkConnectionEstablish();
          handlePublish((MqttPublishMessage) msg);
        }
        case SUBSCRIBE -> {
          checkConnectionEstablish();
          handleSubscribe((MqttSubscribeMessage) msg);
        }
        case PUBACK -> {
          checkConnectionEstablish();
          handlePubAck((MqttPubAckMessage) msg);
        }
        case UNSUBSCRIBE -> {
          checkConnectionEstablish();
        }
        case DISCONNECT -> {
          checkConnectionEstablish();
          closeAsync(CLOSE_NO_REASON);
        }
        case PINGREQ -> handlePing();
        default -> {
          log.warn("received unsupported message auth type.");
          closeAsync(CONNECTION_REFUSED_UNSPECIFIED_ERROR.byteValue());
        }
      }
    } catch (Throwable ex) {
      if (ex instanceof UnConnectedException) {
        log.info("{} connection={}", ex.getMessage(), this);
      } else {
        log.error("Receive an exception while process message.");
      }
      closeAsync(CONNECTION_REFUSED_UNSPECIFIED_ERROR.byteValue());
    }
  }

  private void checkConnectionEstablish() {
    if (STATUS_UPDATER.get(this) != STATUS_ACCEPTED) {
      throw new UnConnectedException();
    }
  }

  // ------- mqtt message handlers

  private static final String KEEP_ALIVE_HANDLER_NAME = "keepAliveHandler";
  private static final String MQTT_ENCODER_NAME = "mqttEncoder";
  private static final String AUTH_METHOD_USERNAME_PREFIX = "method:";

  private String subject;

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
    this.clientId = identifier;
    this.assignedId = assignedIdentifier;
    this.version = MqttVersion.fromProtocolNameAndLevel(var.name(), (byte) var.version());
    // check protocol version
    if (connectMessage.variableHeader().version() > MqttVersion.MQTT_5.protocolLevel()) {
      // The Server MUST respond to the CONNECT Packet with a CONNACK return code 0x01
      // (unacceptable protocol level) and then disconnect the Client if the Protocol
      // Level is not supported by the Server [MQTT-3.1.2-2].
      connectRejectAsync(
          CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION, MqttProperties.NO_PROPERTIES);
      return;
    }
    // See https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html
    // In cases where the ClientID is assigned by the Server, return the assigned ClientID.
    // This also lifts the restriction that Server assigned ClientIDs can only be used with Clean
    // Session=1.
    if (assignedId && !cleanSession) {
      final MqttConnectReturnCode code =
          version.protocolLevel() >= MqttVersion.MQTT_5.protocolLevel()
              ? CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID
              : CONNECTION_REFUSED_IDENTIFIER_REJECTED;
      connectRejectAsync(code, var.properties());
      return;
    }
    this.keepAliveTimeSeconds = var.keepAliveTimeSeconds();
    this.connectTime = System.currentTimeMillis();
    // remove the idle state handler for timeout on CONNECT
    ctx.pipeline().remove(MqttContext.CONNECT_IDLE_NAME);
    ctx.pipeline().remove(MqttContext.CONNECT_TIMEOUT_NAME);

    // keep alive == 0 means NO keep alive, no timeout to handle
    if (connectMessage.variableHeader().keepAliveTimeSeconds() != 0) {
      final var keepAliveTimeout =
          (int) Math.ceil(connectMessage.variableHeader().keepAliveTimeSeconds() * 1.5D);
      ctx.pipeline()
          .addBefore(
              MQTT_ENCODER_NAME,
              MqttContext.CONNECT_IDLE_NAME,
              new IdleStateHandler(keepAliveTimeout, 0, 0));
      ctx.pipeline()
          .addBefore(
              MQTT_ENCODER_NAME,
              KEEP_ALIVE_HANDLER_NAME,
              new ChannelDuplexHandler() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                  if (evt instanceof IdleStateEvent event) {
                    if (event.state() == IdleState.READER_IDLE) {
                      log.info(
                          "Prepare close the connection by idle. client_id={} client_address={}",
                          clientId,
                          clientAddress());
                      closeAsync(CONNECTION_REFUSED_UNSPECIFIED_ERROR.byteValue());
                    }
                  }
                }
              });
    }
    final MqttConnAckMessage message =
        MqttMessageBuilders.connAck()
            .returnCode(CONNECTION_ACCEPTED)
            .properties(
                properties -> {
                  if (assignedIdentifier) {
                    properties.assignedClientId(clientId);
                  }
                  properties
                      .maximumQos((byte) MqttQoS.AT_LEAST_ONCE.value())
                      .wildcardSubscriptionAvailable(false)
                      .sharedSubscriptionAvailable(false)
                      .retainAvailable(false)
                      .subscriptionIdentifiersAvailable(false);
                })
            .sessionPresent(
                !cleanSession) // todo session present, it should be subscription in pulsar
            .build();
    if (!STATUS_UPDATER.compareAndSet(this, STATUS_INIT, STATUS_ACCEPTED)) {
      // unexpected behaviour
      log.warn(
          "Received an unexpected connect message with accepted status, triggering reconnect.");
      connectRejectAsync(
          CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION, MqttProperties.NO_PROPERTIES);
      return;
    }

    this.willMessage = connectMessage.payload().willMessageInBytes();
    this.willTopic = connectMessage.payload().willTopic();
    this.willProperties = connectMessage.payload().willProperties();
    this.willQos = connectMessage.variableHeader().willQos();
    this.willFlag = connectMessage.variableHeader().isWillFlag();

    final CompletableFuture<String> authFuture;
    if (mqttContext.getMqttOptions().authenticationEnabled()) {
      authFuture = doAuthenticate(connectMessage);
    } else {
      authFuture = CompletableFuture.completedFuture(null);
    }
    authFuture
        .thenCompose(
            subject -> {
              this.subject = subject;
              return wrap(ctx.writeAndFlush(message));
            })
        .thenAccept(
            __ -> {
              log.info(
                  "Accepted the connection. client_id={} client_address={}",
                  clientId,
                  clientAddress());
            })
        .exceptionally(
            ex -> {
              log.error(
                  "Receive an error while accepting connection.  connection={}",
                  this,
                  Throwables.getRootCause(ex));
              closeAsync(CONNECTION_REFUSED_UNSPECIFIED_ERROR.byteValue());
              return null;
            });
  }

  private @NotNull CompletableFuture<String> doAuthenticate(
      @NotNull MqttConnectMessage connectMessage) {
    // todo adding integration test
    final AuthenticationService authenticationService =
        mqttContext.getPulsarService().getBrokerService().getAuthenticationService();
    final boolean hasUserName = connectMessage.variableHeader().hasUserName();
    final String userName = connectMessage.payload().userName();
    final boolean hasAuthMethod = hasUserName && userName.startsWith(AUTH_METHOD_USERNAME_PREFIX);
    final String authMethod = userName.replace(AUTH_METHOD_USERNAME_PREFIX, "");
    final boolean hasPassword = connectMessage.variableHeader().hasPassword();
    final byte[] ps = connectMessage.payload().passwordInBytes();
    final AuthenticationDataSource authenticationDataSource;
    if (!hasAuthMethod) { // simple username and password
      // only username here
      authenticationDataSource =
          hasPassword
              ? new AuthenticationDataCommand(
                  userName + ":" + new String(ps, StandardCharsets.UTF_8))
              : new AuthenticationDataCommand(userName);

    } else {
      final String source = hasPassword ? new String(ps, StandardCharsets.UTF_8) : null;
      authenticationDataSource = new AuthenticationDataCommand(source);
    }
    final AuthenticationProvider provider =
        authenticationService.getAuthenticationProvider(authMethod);
    return provider.authenticateAsync(authenticationDataSource);
  }

  private void connectRejectAsync(
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

  private void handlePing() {
    wrap(ctx.writeAndFlush(MqttMessage.PINGRESP))
        .exceptionally(
            ex -> {
              log.warn("Receive an error while send ping response. ctx={}", ctx);
              return null;
            });
  }

  private CompletableFuture<Producer> producerFuture;
  private static final int PRODUCER_ID = 0; // only support single producer here

  private void handlePublish(@Nonnull MqttPublishMessage message) {
    final var var = message.variableHeader();
    final var fix = message.fixedHeader();
    if (fix.qosLevel() == MqttQoS.EXACTLY_ONCE) {
      closeAsync(CONNECTION_REFUSED_UNSPECIFIED_ERROR.byteValue());
      return;
    }
    final var mqttTopicName = var.topicName();
    final var pulsarTopicName = mqttContext.getConverter().convert(mqttTopicName);
    if (producerFuture == null) {
      // producer future will be running in the single netty io thread.
      final CompletableFuture<Boolean> authFuture;
      if (mqttContext.getMqttOptions().authorizationEnabled()) {
        authFuture =
            mqttContext
                .getPulsarService()
                .getBrokerService()
                .getAuthorizationService()
                .allowTopicOperationAsync(
                    pulsarTopicName, TopicOperation.PRODUCE, null, subject, null);
      } else {
        authFuture = CompletableFuture.completedFuture(true);
      }
      producerFuture =
          authFuture
              .thenCompose(
                  authorized -> {
                    if (!authorized) {
                      throw new UnauthorizedException(
                          subject, pulsarTopicName.toString(), TopicOperation.PRODUCE.name());
                    }
                    return getBrokerService().getOrCreateTopic(pulsarTopicName.toString());
                  })
              .thenCompose(
                  topic -> {
                    final var producer =
                        new Producer(
                            topic,
                            this,
                            PRODUCER_ID,
                            clientId,
                            "",
                            false,
                            Collections.emptyMap(),
                            SchemaVersion.Empty,
                            0,
                            true,
                            ProducerAccessMode.Shared,
                            Optional.empty(),
                            false);
                    return topic
                        .addProducer(producer, new CompletableFuture<>())
                        .thenApply(topicEpoch -> producer);
                  });
    }
    producerFuture // todo: we should give a pending limit here.
        .thenAccept(
            producer -> {
              if (!Objects.equals(pulsarTopicName.toString(), producer.getTopic().getName())) {
                final MqttConnectReturnCode code =
                    version.protocolLevel() >= MqttVersion.MQTT_5.protocolLevel()
                        ? CONNECTION_REFUSED_TOPIC_NAME_INVALID
                        : CONNECTION_REFUSED_UNSPECIFIED_ERROR;
                publishAckAsync(var.packetId(), code, MqttProperties.NO_PROPERTIES);
                return;
              }
              publishAsync(producer, message);
            })
        .exceptionally(
            ex -> {
              final Throwable rc = FutureUtil.unwrapCompletionException(ex);
              if (rc instanceof BrokerServiceException.ServiceUnitNotReadyException) {
                log.warn(
                    "The topic is not owned by the current broker. mqtt_topic_name={}  pulsar_topic_name={}",
                    mqttTopicName,
                    pulsarTopicName);
                handleTopicDoesNotOwnedByCurrentBroker();
                // todo: mqtt 5 disconnect property support
                return null;
              }
              if (rc instanceof UnauthorizedException) {
                log.warn(rc.getMessage());
                closeAsync(CONNECTION_REFUSED_NOT_AUTHORIZED_5.byteValue());
                return null;
              }
              log.error(
                  "Received an exception while publish message. mqtt_topic_name={} producer={}",
                  mqttTopicName,
                  producerFuture.getNow(null),
                  ex);
              closeAsync(CONNECTION_REFUSED_UNSPECIFIED_ERROR.byteValue());
              return null;
            });
  }

  private static final FastThreadLocal<MessageMetadata> LOCAL_MESSAGE_METADATA =
      new FastThreadLocal<>() {
        @Override
        protected MessageMetadata initialValue() {
          return new MessageMetadata();
        }
      };

  private final BlockingQueue<Integer> inflightPublishPackages = new ArrayBlockingQueue<>(5000);

  private static final int NO_ACK_PACKET_ID = -1;

  private void publishAsync(
      @Nonnull Producer producer, @Nonnull MqttPublishMessage publishMessage) {
    final var producerId = producer.getProducerId();
    final var metadata = LOCAL_MESSAGE_METADATA.get();
    final int packetId = publishMessage.variableHeader().packetId();
    metadata.clear();
    metadata.setEventTime(System.currentTimeMillis());
    metadata.setSequenceId(-1);
    metadata.setPublishTime(System.currentTimeMillis());
    metadata.setProducerName(clientId);
    metadata.setCompression(CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
    final var payload = publishMessage.payload();
    metadata.setUncompressedSize(payload.readableBytes());
    final var buf =
        Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
    payload.release();
    inflightPublishPackages.add(
        publishMessage.fixedHeader().qosLevel().value() >= MqttQoS.AT_LEAST_ONCE.value()
            ? packetId
            : NO_ACK_PACKET_ID);
    try {
      producer.publishMessage(producerId, -1, buf, 1, false, false, null);
    } catch (Throwable ex) {
      sendSendError(producerId, -1, ServerError.UnknownError, ex.getMessage());
    }
  }

  @Override
  public void sendSendReceiptResponse(
      long producerId, long sequenceId, long highestId, long ledgerId, long entryId) {
    final Integer packetId = inflightPublishPackages.poll();
    if (packetId == null) {
      log.warn(
          "Received a send receipt without packet id. producer={}, ledger_id={}, entry_id={}",
          producerFuture.getNow(null),
          ledgerId,
          entryId);
      return;
    }
    if (packetId == NO_ACK_PACKET_ID) {
      // qos 0 message do not need receipt
      return;
    }
    publishAckAsync(packetId, CONNECTION_ACCEPTED, MqttProperties.NO_PROPERTIES);
  }

  @Override
  public void sendSendError(long producerId, long sequenceId, ServerError error, String errorMsg) {
    final Integer packetId = inflightPublishPackages.poll();
    if (packetId == null) {
      log.warn(
          "Received a send error without packet id. producer={} error_code={} error_message={}",
          producerFuture.getNow(null),
          error,
          errorMsg);
      // ignore the empty packet id
      return;
    }
    log.error(
        "Received an error while publishing message. packet_id={} producer={} error_code={} error_message={}",
        packetId,
        producerFuture.getNow(null),
        error,
        errorMsg);
    publishAckAsync(packetId, CONNECTION_REFUSED_UNSPECIFIED_ERROR, MqttProperties.NO_PROPERTIES);
  }

  private void publishAckAsync(
      int packetId, @NotNull MqttConnectReturnCode code, @NotNull MqttProperties properties) {
    final MqttMessage message =
        MqttMessageBuilders.pubAck()
            .packetId(packetId)
            .properties(properties)
            .reasonCode(code.byteValue())
            .build();
    wrap(ctx.writeAndFlush(message))
        .exceptionally(
            ex -> {
              log.error(
                  "Receive an error while ack publishes the package.  connection={}",
                  this,
                  Throwables.getRootCause(ex));
              closeAsync(CONNECTION_REFUSED_UNSPECIFIED_ERROR.byteValue());
              return null;
            });
  }

  private CompletableFuture<Consumer> consumerFuture;
  private static final long CONSUMER_ID = 0;
  private static final int PRIORITY_LEVEL = 0;
  private static final int CONSUMER_EPOCH = 0;
  private MqttQoS consumerQos;
  private static final int PERMITS_TOTAL = 1000;
  private final AtomicInteger pendingFlowPermits = new AtomicInteger(0);

  private static final KeySharedMeta EMPTY_KEY_SHARED_METADATA =
      new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT);

  private void handleSubscribe(@Nonnull MqttSubscribeMessage subscribeMessage) {
    final var var = subscribeMessage.idAndPropertiesVariableHeader();
    final var payload = subscribeMessage.payload();
    final int packetId = var.messageId();
    final List<MqttTopicSubscription> subscriptions = payload.topicSubscriptions();
    final var subscription = subscriptions.get(0); // only support single topic

    this.consumerQos = subscription.qualityOfService();
    final String mqttTopicName = subscription.topicName();
    final TopicName pulsarTopicName = mqttContext.getConverter().convert(mqttTopicName);
    final String subscriptionName = clientId;
    final String consumerName = clientId;
    // producer future will be running in the single netty io thread.
    final CompletableFuture<Boolean> authFuture;
    if (mqttContext.getMqttOptions().authorizationEnabled()) {
      authFuture =
          mqttContext
              .getPulsarService()
              .getBrokerService()
              .getAuthorizationService()
              .allowTopicOperationAsync(
                  pulsarTopicName, TopicOperation.SUBSCRIBE, null, subject, null);
    } else {
      authFuture = CompletableFuture.completedFuture(true);
    }
    if (consumerFuture != null) {
      log.warn(
          "Receive a duplicated subscribe request. the current version of plugin only support single subscribe."
              + " client_address={} client_id={}",
          clientSourceAddress(),
          clientId);
      return;
    }
    consumerFuture =
        authFuture
            .thenCompose(
                authorized -> {
                  if (!authorized) {
                    throw new UnauthorizedException(
                        subject, pulsarTopicName.toString(), TopicOperation.CONSUME.name());
                  }
                  return getBrokerService().getOrCreateTopic(pulsarTopicName.toString());
                })
            .thenCompose(
                topic ->
                    getBrokerService()
                        .isAllowAutoSubscriptionCreationAsync(pulsarTopicName)
                        .thenAccept(
                            isAllowedAutoSubscriptionCreation -> {
                              if (!isAllowedAutoSubscriptionCreation
                                  && !topic.getSubscriptions().containsKey(subscriptionName)
                                  && topic.isPersistent()) {
                                // todo finish the authorization check
                                throw new IllegalStateException("");
                              }
                            })
                        .thenCompose(
                            __ -> {
                              final SubscriptionOption option =
                                  SubscriptionOption.builder()
                                      .cnx(this)
                                      .subscriptionName(subscriptionName)
                                      .consumerId(CONSUMER_ID)
                                      .subType(CommandSubscribe.SubType.Shared)
                                      .priorityLevel(PRIORITY_LEVEL)
                                      .consumerName(consumerName)
                                      .isDurable(true)
                                      .startMessageId(MessageId.latest)
                                      .metadata(Collections.emptyMap())
                                      .readCompacted(false)
                                      .initialPosition(CommandSubscribe.InitialPosition.Latest)
                                      .startMessageRollbackDurationSec(-1)
                                      .replicatedSubscriptionStateArg(false)
                                      .keySharedMeta(EMPTY_KEY_SHARED_METADATA)
                                      .subscriptionProperties(Optional.empty())
                                      .consumerEpoch(CONSUMER_EPOCH)
                                      .build();
                              return topic.subscribe(option);
                            }));
    consumerFuture
        .thenCompose(
            consumer -> {
              final MqttSubAckMessage message =
                  MqttMessageBuilders.subAck()
                      .packetId(packetId)
                      .addGrantedQos(consumerQos)
                      .build();
              return wrap(ctx.writeAndFlush(message))
                  .thenAccept(__ -> consumer.flowPermits(PERMITS_TOTAL));
            })
        .thenAccept(
            __ -> {
              log.info(
                  "Subscribed on topic. consumer={} subscription={} mqtt_topic_name={}"
                      + " pulsar_topic_name={} client_address={}",
                  consumerName,
                  subscriptionName,
                  mqttTopicName,
                  pulsarTopicName,
                  clientAddress());
            })
        .exceptionally(
            ex -> {
              final Throwable rc = FutureUtil.unwrapCompletionException(ex);
              if (rc instanceof BrokerServiceException.ServiceUnitNotReadyException) {
                log.warn(
                    "The topic is not owned by the current broker. mqtt_topic_name={}  pulsar_topic_name={}",
                    mqttTopicName,
                    pulsarTopicName);
                handleTopicDoesNotOwnedByCurrentBroker();
                return null;
              }
              log.error(
                  "Received an error while subscribe on topic. consumer={} subscription={} mqtt_topic_name={}"
                      + " pulsar_topic_name={} client_address={}",
                  consumerName,
                  subscriptionName,
                  mqttTopicName,
                  pulsarTopicName,
                  clientAddress(),
                  rc);
              closeAsync(CONNECTION_REFUSED_UNSPECIFIED_ERROR.byteValue());
              return null;
            });
  }

  private final RoaringBitmap packetIdBitMap = new RoaringBitmap();
  private final Map<Integer, PacketIdContext> packetContexts = new TreeMap<>();
  private static final int MAX_PACKET_ID = 65535;

  private void handlePubAck(MqttPubAckMessage msg) {
    final var var = msg.variableHeader();
    final int packetId = var.messageId();
    final PacketIdContext context = packetContexts.remove(packetId);
    packetIdBitMap.remove(packetId);
    final Consumer consumer = consumerFuture.getNow(null);
    if (consumer == null) {
      // it shouldn't happen
      log.error(
          "Get empty consumer when ack qos 1 messages, it should be a bug here."
              + " mqtt_packet_id={} message_context={}",
          packetId,
          context);
      return;
    }
    // todo batch ack request
    final CommandAck commandAck = new CommandAck();
    final CommandAck ack =
        commandAck
            .setAckType(CommandAck.AckType.Individual)
            .setConsumerId(CONSUMER_ID)
            .setRequestId(-1);
    final MessageIdData ackMessageId =
        ack.addMessageId().setLedgerId(context.getLedgerId()).setEntryId(context.getEntryId());
    if (context.isBatch()) {
      for (int i = 0; i < context.getBatchSize(); i++) {
        ackMessageId.addAckSet(context.getBatchIndex() == i ? 0 : 1);
      }
    }
    consumer
        .messageAcked(ack)
        .exceptionally(
            ex -> {
              log.error(
                  "Receive an error while acknowledge message. packet_id={} packet_context={}",
                  packetId,
                  context);
              return null;
            });
  }

  // ------ mqtt socket properties
  private ChannelHandlerContext ctx;

  // ------ mqtt properties
  private boolean cleanSession;
  private String clientId;
  private boolean assignedId;
  private MqttVersion version;
  private int keepAliveTimeSeconds;

  // ------ mqtt will message  todo support internal client send will message
  private byte[] willMessage;
  private String willTopic;
  private MqttProperties willProperties;
  private int willQos;
  private boolean willRetained;
  private boolean willFlag;

  // ------ lifecycle
  private long connectTime;
  private volatile int status = STATUS_INIT;
  private static final AtomicIntegerFieldUpdater<Connection> STATUS_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(Connection.class, "status");
  private static final int STATUS_INIT = 0;
  private static final int STATUS_ACCEPTED = 1;
  private static final int STATUS_REJECTED = 2;
  private static final int STATUS_CLOSED = 3;

  private static final byte CLOSE_NO_REASON = -1;

  private void closeAsync(byte reasonCode) {
    // DCL start
    if (STATUS_UPDATER.get(this) == STATUS_CLOSED) {
      return;
    }
    synchronized (this) {
      if (STATUS_UPDATER.get(this) == STATUS_CLOSED) {
        return;
      }
      STATUS_UPDATER.set(this, STATUS_CLOSED);
    }
    // DCL end
    log.info("Closing the connection. client_id={} client_address={}", clientId, clientAddress());
    final CompletableFuture<Void> disconnectMessageFuture;
    if (reasonCode != CLOSE_NO_REASON
        && version.protocolLevel() > MqttVersion.MQTT_5.protocolLevel()) {
      final MqttMessage disconnectMessage =
          MqttMessageBuilders.disconnect().reasonCode(reasonCode).build();
      disconnectMessageFuture = wrap(ctx.writeAndFlush(disconnectMessage));
    } else {
      disconnectMessageFuture = CompletableFuture.completedFuture(null);
    }
    disconnectMessageFuture
        .thenCompose(__ -> wrap(ctx.close()))
        .thenAccept(
            __ -> {
              log.info(
                  "Closed the connection. client_id={} client_address={}",
                  clientId,
                  clientAddress());
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

  // ---------- command sender
  private Map<Long, CompletableFuture<?>> requestFutures = new TreeMap<>();

  @Override
  public void sendSuccessResponse(long requestId) {}

  @Override
  public void sendErrorResponse(long requestId, ServerError error, String message) {}

  private static final FastThreadLocal<SingleMessageMetadata> LOCAL_SINGLE_MESSAGE_METADATA =
      new FastThreadLocal<>() {
        @Override
        protected SingleMessageMetadata initialValue() {
          return new SingleMessageMetadata();
        }
      };

  @Override
  public Future<Void> sendMessagesToConsumer(
      long consumerId,
      String pulsarTopicNameStr,
      Subscription subscription,
      int partitionIdx,
      List<? extends Entry> entries,
      EntryBatchSizes batchSizes,
      EntryBatchIndexesAcks batchIndexesAcks,
      RedeliveryTracker redeliveryTracker,
      long epoch) {
    final TopicName pulsarTopicName = TopicName.get(pulsarTopicNameStr);
    final TopicNameConverter converter = mqttContext.getConverter();
    final ChannelPromise writePromise = ctx.newPromise();
    // single thread to avoid concurrent problem
    // protect `packetIdBitMap`,
    execute(
        () -> {
          final Set<Integer> insufficientPacketIdEntryIndexes = new HashSet<>();
          final List<Entry> entriesToRelease = new ArrayList<>(entries.size());
          final List<SemiFinishedPubMessage> mqttPublishMessages =
              makeSemiFinishedPublishMessages(entries, entriesToRelease, pulsarTopicNameStr);
          for (final SemiFinishedPubMessage semiFinishedPubMessage : mqttPublishMessages) {
            final var pubBuilder = semiFinishedPubMessage.getPublishBuilder();
            final int entryIndex = semiFinishedPubMessage.getEntryIndex();
            final Entry originalEntry = entries.get(entryIndex);
            final int batchSize = batchSizes.getBatchSize(entryIndex);
            // packet id generation
            final int packetId =
                consumerQos == MqttQoS.AT_MOST_ONCE
                    ? NO_ACK_PACKET_ID
                    : (int) packetIdBitMap.nextAbsentValue(1);
            if (packetId > MAX_PACKET_ID) {
              insufficientPacketIdEntryIndexes.add(entryIndex);
              continue;
            }
            packetIdBitMap.add(packetId);
            final var packetIdContextBuilder =
                PacketIdContext.builder()
                    .ledgerId(originalEntry.getLedgerId())
                    .entryId(originalEntry.getEntryId())
                    .batchSize(batchSize)
                    .batchIndex(semiFinishedPubMessage.getBatchIndex());
            packetContexts.put(packetId, packetIdContextBuilder.build());

            final MqttPublishMessage message =
                pubBuilder
                    .messageId(packetId)
                    .qos(consumerQos)
                    .topicName(converter.convert(pulsarTopicName))
                    .build();
            ctx.write(message, ctx.voidPromise());
          }

          // Use an empty write here so that we can just tie the flush with the write promise for
          // the last entry.
          ctx.writeAndFlush(Unpooled.EMPTY_BUFFER, writePromise);
          writePromise.addListener(
              (future) -> {
                final Consumer consumer = consumerFuture.getNow(null);
                if (future.isSuccess()) {
                  if (consumer != null) {
                    // acknowledge qos0 messages because qos0 don't need ack
                    if (consumerQos == MqttQoS.AT_MOST_ONCE) {
                      final List<Position> needAckEntries =
                          entries.stream().map(Entry::getPosition).toList();
                      consumer
                          .getSubscription()
                          .acknowledgeMessage(
                              needAckEntries,
                              CommandAck.AckType.Individual,
                              Collections.emptyMap());
                    }

                    // todo the loop redeliver may drain the resources, we need find out a good way
                    // to avoid loop redelivery
                    // redeliver messages due to insufficient packet id
                    int totalRedeliveredMessageNum = 0;
                    final List<MessageIdData> redeliverMessages = new ArrayList<>();
                    for (int insufficientPacketIdEntryIndex : insufficientPacketIdEntryIndexes) {
                      totalRedeliveredMessageNum +=
                          batchSizes.getBatchSize(insufficientPacketIdEntryIndex);
                      final Entry entry = entries.get(insufficientPacketIdEntryIndex);
                      final MessageIdData messageIdData = new MessageIdData();
                      messageIdData.setLedgerId(entry.getLedgerId());
                      messageIdData.setEntryId(entry.getEntryId());
                      redeliverMessages.add(messageIdData);
                    }
                    consumer.redeliverUnacknowledgedMessages(redeliverMessages);

                    // Do not flow permits while packet id has been exhausted.
                    if (!insufficientPacketIdEntryIndexes.isEmpty()) {
                      consumer.flowPermits(totalRedeliveredMessageNum);
                    } else {
                      int pendingFlowTotal =
                          pendingFlowPermits.addAndGet(mqttPublishMessages.size());
                      if (pendingFlowTotal >= PERMITS_TOTAL / 2) {
                        if (pendingFlowPermits.compareAndSet(pendingFlowTotal, 0)) {
                          consumer.flowPermits(pendingFlowTotal);
                        }
                      }
                    }
                  } else {
                    // it shouldn't happen
                    log.error(
                        "Get empty consumer when ack qos 0 messages, it should be a bug here.");
                  }
                } else {
                  // redeliver all the messages
                  entries.forEach(
                      entry ->
                          redeliveryTracker.incrementAndGetRedeliveryCount(entry.getPosition()));

                  log.error(
                      "Receive an error while writing the message to client. client_address={} topic_name={}",
                      clientSourceAddress(),
                      pulsarTopicNameStr,
                      future.cause());
                }

                // release the entries only after flushing the channel
                //
                // InflightReadsLimiter tracks the amount of memory retained by in-flight data to
                // the
                // consumer. It counts the memory as being released when the entry is deallocated
                // that is that it reaches refcnt=0.
                // so we need to call release only when we are sure that Netty released the internal
                // ByteBuf
                entriesToRelease.forEach(Entry::release);
              });
          batchSizes.recyle();
          if (batchIndexesAcks != null) {
            batchIndexesAcks.recycle();
          }
        });
    return writePromise;
  }

  private static List<SemiFinishedPubMessage> makeSemiFinishedPublishMessages(
      List<? extends Entry> entries, List<Entry> entriesToRelease, String pulsarTopicName) {
    final List<SemiFinishedPubMessage> mqttPublishMessages = new ArrayList<>();
    for (int entryIndex = 0; entryIndex < entries.size(); entryIndex++) {
      final Entry entry = entries.get(entryIndex);
      if (entry == null) {
        // Entry was filtered out
        continue;
      }
      final ByteBuf payload = entry.getDataBuffer();
      final MessageMetadata metadata = Commands.parseMessageMetadata(payload);
      if (metadata.hasNumMessagesInBatch()) {
        int batchSize = metadata.getNumMessagesInBatch();
        try {
          for (int batchIndex = 0; batchIndex < batchSize; batchIndex++) {
            final SingleMessageMetadata metadata1 = LOCAL_SINGLE_MESSAGE_METADATA.get();
            metadata1.clear();
            final ByteBuf payload1 =
                Commands.deSerializeSingleMessageInBatch(payload, metadata1, batchIndex, batchSize);
            final var builder = makeMqttPubMessageByMetadataAndPayload(metadata1, payload1);
            mqttPublishMessages.add(
                SemiFinishedPubMessage.builder()
                    .entryIndex(entryIndex)
                    .batchIndex(batchIndex)
                    .publishBuilder(builder)
                    .build());
          }
        } catch (IOException ex) {
          log.warn(
              "Receive an error while decoding the batch pulsar format message,"
                  + " skip delivering entry but not ack it.  topic_name={} ledger_id={} entry_id={}",
              pulsarTopicName,
              entry.getLedgerId(),
              entry.getEntryId());
        }
      } else {
        final var builder = makeMqttPubMessageByMetadataAndPayload(metadata, payload);
        mqttPublishMessages.add(
            SemiFinishedPubMessage.builder()
                .entryIndex(entryIndex)
                .publishBuilder(builder)
                .build());
      }
      entriesToRelease.add(entry);
    }
    return mqttPublishMessages;
  }

  private static MqttMessageBuilders.PublishBuilder makeMqttPubMessageByMetadataAndPayload(
      Object metadata, ByteBuf payload) {
    // todo support mqtt5
    return MqttMessageBuilders.publish().payload(payload);
  }

  private void handleTopicDoesNotOwnedByCurrentBroker() {
    closeAsync(CONNECTION_REFUSED_SERVER_MOVED.byteValue());
  }

  // ---------- useless methods

  @Override
  public void sendActiveConsumerChange(long consumerId, boolean isActive) {}

  @Override
  public void sendProducerSuccessResponse(
      long requestId, String producerName, SchemaVersion schemaVersion) {}

  @Override
  public void sendReachedEndOfTopic(long consumerId) {}

  @Override
  public void sendProducerSuccessResponse(
      long requestId,
      String producerName,
      long lastSequenceId,
      SchemaVersion schemaVersion,
      Optional<Long> topicEpoch,
      boolean isProducerReady) {}

  @Override
  public void sendPartitionMetadataResponse(ServerError error, String errorMsg, long requestId) {}

  @Override
  public void sendPartitionMetadataResponse(int partitions, long requestId) {}

  @Override
  public void sendGetTopicsOfNamespaceResponse(
      List<String> topics, String topicsHash, boolean filtered, boolean changed, long requestId) {}

  @Override
  public void sendGetSchemaResponse(long requestId, SchemaInfo schema, SchemaVersion version) {}

  @Override
  public void sendGetSchemaErrorResponse(long requestId, ServerError error, String errorMessage) {}

  @Override
  public void sendGetOrCreateSchemaResponse(long requestId, SchemaVersion schemaVersion) {}

  @Override
  public void sendGetOrCreateSchemaErrorResponse(
      long requestId, ServerError error, String errorMessage) {}

  @Override
  public void sendConnectedResponse(
      int clientProtocolVersion, int maxMessageSize, boolean supportsTopicWatchers) {}

  @Override
  public void sendLookupResponse(
      String brokerServiceUrl,
      String brokerServiceUrlTls,
      boolean authoritative,
      CommandLookupTopicResponse.LookupType response,
      long requestId,
      boolean proxyThroughServiceUrl) {}

  @Override
  public void sendLookupResponse(ServerError error, String errorMsg, long requestId) {}

  @Override
  public boolean sendTopicMigrated(
      CommandTopicMigrated.ResourceType type,
      long resourceId,
      String brokerUrl,
      String brokerUrlTls) {
    return false;
  }

  @Override
  public void sendTcClientConnectResponse(
      long requestId, org.apache.pulsar.common.api.proto.ServerError error, String message) {}

  @Override
  public void sendTcClientConnectResponse(long requestId) {}

  @Override
  public void sendNewTxnResponse(
      long requestId, org.apache.pulsar.client.api.transaction.TxnID txnID, long tcID) {}

  @Override
  public void sendNewTxnErrorResponse(
      long requestId, long tcID, ServerError error, String message) {}

  @Override
  public void sendEndTxnResponse(long requestId, TxnID txnID, int txnAction) {}

  @Override
  public void sendEndTxnErrorResponse(
      long requestId, TxnID txnID, ServerError error, String message) {}

  @Override
  public void sendWatchTopicListSuccess(
      long requestId, long watcherId, String topicsHash, List<String> topics) {}

  @Override
  public void sendWatchTopicListUpdate(
      long watcherId, List<String> newTopics, List<String> deletedTopics, String topicsHash) {}
}
