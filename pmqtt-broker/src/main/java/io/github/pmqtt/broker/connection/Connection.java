package io.github.pmqtt.broker.connection;

import static io.github.pmqtt.broker.utils.future.CompletableFutures.wrap;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_QOS_NOT_SUPPORTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_TOPIC_NAME_INVALID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_UNSPECIFIED_ERROR;
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
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandTopicMigrated;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProducerAccessMode;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;

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
    closeAsync();
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
    switch (fixed.messageType()) {
      case CONNECT -> handleConnect((MqttConnectMessage) msg);
      case PUBLISH -> handlePublish((MqttPublishMessage) msg);
      case AUTH -> {
        return;
      }
      case SUBSCRIBE -> {
        return;
      }
      case UNSUBSCRIBE -> {
        return;
      }
      case PINGREQ -> {
        return;
      }
      case PINGRESP -> {
        return;
      }
      case DISCONNECT -> {
        return;
      }
      default -> throw new UnsupportedOperationException();
    }
  }

  // ------- mqtt message handlers
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
    if (cleanSession && assignedId) {
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
                          clientId,
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
      connectRejectAsync(
          CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION, MqttProperties.NO_PROPERTIES);
      return;
    }
    wrap(ctx.writeAndFlush(message))
        .thenAccept(
            __ ->
                log.info(
                    "Accepted the connection. client_id={} client_address={}",
                    clientId,
                    clientAddress()))
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

  private Producer producer;
  private static final int PRODUCER_ID = 0; // only support single producer here

  private void handlePublish(@Nonnull MqttPublishMessage message) {
    final var var = message.variableHeader();
    final var fix = message.fixedHeader();
    if (fix.qosLevel() == MqttQoS.EXACTLY_ONCE) {
      final MqttConnectReturnCode code =
          version.protocolLevel() >= MqttVersion.MQTT_5.protocolLevel()
              ? CONNECTION_REFUSED_QOS_NOT_SUPPORTED
              : CONNECTION_REFUSED_UNSPECIFIED_ERROR;
      publishAckAsync(var.packetId(), code, MqttProperties.NO_PROPERTIES);
      return;
    }
    final var mqttTopicName = var.topicName();
    final var pulsarTopicName = mqttContext.getConverter().convert(mqttTopicName);
    if (producer != null) {
      // in some cases the client can publish message with different topic
      if (!Objects.equals(pulsarTopicName.toString(), producer.getTopic().getName())) {
        final MqttConnectReturnCode code =
            version.protocolLevel() >= MqttVersion.MQTT_5.protocolLevel()
                ? CONNECTION_REFUSED_TOPIC_NAME_INVALID
                : CONNECTION_REFUSED_UNSPECIFIED_ERROR;
        publishAckAsync(var.packetId(), code, MqttProperties.NO_PROPERTIES);
        return;
      }
      publishAsync(message);
      return;
    }
    try {
      producer =
          getBrokerService()
              .getOrCreateTopic(pulsarTopicName.toString())
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
                  })
              .get(30, TimeUnit.SECONDS); // back pressure here
      publishAsync(message);
    } catch (InterruptedException | ExecutionException | TimeoutException ex) {
      final Throwable rc = FutureUtil.unwrapCompletionException(ex);
      log.error("Received an exception.", ex);
      if (rc instanceof BrokerServiceException.ServiceUnitNotReadyException) {
        // todo redirect
        return;
      }
    }
  }

  private static final FastThreadLocal<MessageMetadata> LOCAL_MESSAGE_METADATA =
      new FastThreadLocal<>() {
        @Override
        protected MessageMetadata initialValue() {
          return new MessageMetadata();
        }
      };

  private final BlockingQueue<Integer> inflightPublishPackages = new ArrayBlockingQueue<>(5000);

  private void publishAsync(@Nonnull MqttPublishMessage publishMessage) {
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
    inflightPublishPackages.add(packetId);
    try {
      producer.publishMessage(producerId, -1, buf, 1, false, false, null);
    } catch (Throwable ex) {
      log.warn(""); // todo exception
      sendSendError(producerId, -1, ServerError.UnknownError, ex.getMessage());
    }
  }

  @Override
  public void sendSendReceiptResponse(
      long producerId, long sequenceId, long highestId, long ledgerId, long entryId) {
    final Integer packetId = inflightPublishPackages.poll();
    if (packetId == null) {
      log.warn(""); // todo handle exception
      return;
    }
    publishAckAsync(packetId, CONNECTION_ACCEPTED, MqttProperties.NO_PROPERTIES);
  }

  @Override
  public void sendSendError(long producerId, long sequenceId, ServerError error, String errorMsg) {
    final Integer packetId = inflightPublishPackages.poll();
    if (packetId == null) {
      log.warn(""); // todo handle exception
      return;
    }
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
              closeAsync();
              return null;
            });
  }

  private Consumer consumer;

  private void handleSubscribe(@Nonnull MqttSubscribeMessage subscribeMessage) {}

  // ------ mqtt socket properties
  private ChannelHandlerContext ctx;

  // ------ mqtt properties
  private boolean cleanSession;
  private String clientId;
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
    log.info("Closing the connection. client_id={} client_address={}", clientId, clientAddress());
    wrap(ctx.close())
        .thenAccept(
            __ -> {
              STATUS_UPDATER.set(this, STATUS_CLOSED);
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

  @Override
  public Future<Void> sendMessagesToConsumer(
      long consumerId,
      String topicName,
      Subscription subscription,
      int partitionIdx,
      List<? extends Entry> entries,
      EntryBatchSizes batchSizes,
      EntryBatchIndexesAcks batchIndexesAcks,
      RedeliveryTracker redeliveryTracker,
      long epoch) {
    return null;
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
