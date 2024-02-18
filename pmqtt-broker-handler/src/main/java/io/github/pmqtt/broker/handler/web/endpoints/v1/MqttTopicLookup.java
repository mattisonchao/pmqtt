package io.github.pmqtt.broker.handler.web.endpoints.v1;

import io.github.pmqtt.broker.handler.env.Constants;
import io.github.pmqtt.broker.handler.web.base.AbstractEndpoint;
import io.github.pmqtt.broker.handler.web.entity.URI;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import javax.inject.Singleton;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.protocol.ProtocolHandlers;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.FutureUtil;

@Path("/v1/lookup")
@Api
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Singleton
@Slf4j
public final class MqttTopicLookup extends AbstractEndpoint {

  @GET
  @ApiOperation(value = "Get topic owner broker mqtt address", response = URI.class)
  @ApiResponses({
    @ApiResponse(code = 200, message = "success"),
    @ApiResponse(code = 401, message = "authentication failed"),
    @ApiResponse(code = 403, message = "authorization failed"),
    @ApiResponse(code = 404, message = "topic does not found"),
    @ApiResponse(code = 500, message = "server internal error")
  })
  public void getTopicOwner(
      @Suspended final AsyncResponse asyncResponse,
      @Context ServletContext servletContext,
      @Context HttpServletRequest httpRequest,
      @Context UriInfo uriInfo,
      @ApiParam(value = "topic", required = true) @QueryParam("topic") String topic,
      @ApiParam(
              value =
                  "Whether leader broker redirected this call to this broker. For internal use.")
          @QueryParam("authoritative")
          @DefaultValue("false")
          boolean authoritative) {
    final TopicName pulsarTopicName = mqttContext.getConverter().convert(topic);
    validateTopicOperationAsync(httpRequest, pulsarTopicName, TopicOperation.LOOKUP)
        .thenCompose(
            __ -> validateTopicOwnershipAsync(httpRequest, uriInfo, pulsarTopicName, authoritative))
        .thenAccept(
            __ -> {
              final ProtocolHandlers protocolHandlers = pulsarService.getProtocolHandlers();
              final Optional<SocketAddress> mqttHandlerEndpoint =
                  protocolHandlers.getEndpoints().entrySet().stream()
                      .filter(entry -> entry.getValue().equals(Constants.PROTOCOL_NAME))
                      .map(Map.Entry::getKey)
                      .findAny();
              if (mqttHandlerEndpoint.isEmpty()) {
                log.warn(
                    "Mqtt protocol handler can't found in pulsar service, reject mqtt topic lookup."
                        + " mqtt_topic_name={}",
                    topic);
                throw new WebApplicationException(Response.Status.NOT_IMPLEMENTED);
              }
              final InetSocketAddress inetSocketAddress =
                  (InetSocketAddress) mqttHandlerEndpoint.get();
              final URI uri =
                  URI.builder()
                      .schema("mqtt")
                      .host(inetSocketAddress.getHostName())
                      .port(inetSocketAddress.getPort())
                      .build();
              asyncResponse.resume(uri);
            })
        .exceptionally(
            ex -> {
              final Throwable rc = FutureUtil.unwrapCompletionException(ex);
              if (rc instanceof WebApplicationException) {
                asyncResponse.resume(rc);
                return null;
              }
              log.error("Lookup topic failed. mqtt_topic_name={}", topic, rc);
              asyncResponse.resume(new WebApplicationException(rc));
              return null;
            });
  }
}
