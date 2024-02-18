package io.github.pmqtt.broker.handler.web.base;

import static org.apache.pulsar.functions.worker.rest.FunctionApiResource.ORIGINAL_PRINCIPAL_HEADER;

import io.github.pmqtt.broker.handler.MqttContext;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.CompletableFuture;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Those logics are copied from Apache Pulsar #{@link
 * org.apache.pulsar.broker.web.PulsarWebResource} for singleton web application endpoint to reduce
 * the Object creation.
 */
@Slf4j
public abstract class AbstractEndpoint {
  public static PulsarService pulsarService;
  public static MqttContext mqttContext;

  protected @NotNull CompletableFuture<Void> validateTopicOperationAsync(
      @NotNull HttpServletRequest request,
      @NotNull TopicName topicName,
      @NotNull TopicOperation operation) {
    final String subject =
        (String) request.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName);
    final String originalPrincipal = request.getHeader(ORIGINAL_PRINCIPAL_HEADER);
    final AuthenticationDataSource authData =
        (AuthenticationDataSource)
            request.getAttribute(AuthenticationFilter.AuthenticatedDataAttributeName);
    if (pulsarService.getConfiguration().isAuthenticationEnabled()
        && pulsarService.getBrokerService().isAuthorizationEnabled()) {
      if (subject == null) {
        return FutureUtil.failedFuture(
            new RestException(
                Response.Status.UNAUTHORIZED, "Need to authenticate to perform the request"));
      }
      return pulsarService
          .getBrokerService()
          .getAuthorizationService()
          .allowTopicOperationAsync(topicName, operation, originalPrincipal, subject, authData)
          .thenAccept(
              isAuthorized -> {
                if (!isAuthorized) {
                  throw new RestException(
                      Response.Status.UNAUTHORIZED,
                      String.format(
                          "Unauthorized to validateTopicOperation for operation [%s] on topic [%s]",
                          operation.toString(), topicName));
                }
              });
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  protected static boolean isLeaderBroker() {
    if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsarService.getConfig())) {
      return true;
    }
    return pulsarService.getLeaderElectionService().isLeader();
  }

  protected @NotNull CompletableFuture<Void> validateTopicOwnershipAsync(
      @NotNull HttpServletRequest request,
      @NotNull UriInfo uriInfo,
      @NotNull TopicName topicName,
      boolean authoritative) {
    final boolean isHttps = "https".equalsIgnoreCase(request.getScheme());
    final NamespaceService nsService = pulsarService.getNamespaceService();

    final LookupOptions options =
        LookupOptions.builder()
            .authoritative(authoritative)
            .requestHttps(isHttps)
            .readOnly(false)
            .loadTopicsInBundle(false)
            .build();

    return nsService
        .getWebServiceUrlAsync(topicName, options)
        .thenApply(
            webUrl -> {
              if (webUrl == null || !webUrl.isPresent()) {
                log.info("Unable to get web service url");
                throw new RestException(
                    Response.Status.PRECONDITION_FAILED,
                    "Failed to find ownership for topic:" + topicName);
              }
              return webUrl.get();
            })
        .thenCompose(
            webUrl ->
                nsService
                    .isServiceUnitOwnedAsync(topicName)
                    .thenApply(isTopicOwned -> Pair.of(webUrl, isTopicOwned)))
        .thenAccept(
            pair -> {
              final URL webUrl = pair.getLeft();
              boolean isTopicOwned = pair.getRight();

              if (!isTopicOwned) {
                boolean newAuthoritative = isLeaderBroker();
                // Replace the host and port of the current request and redirect
                URI redirect =
                    UriBuilder.fromUri(uriInfo.getRequestUri())
                        .host(webUrl.getHost())
                        .port(webUrl.getPort())
                        .replaceQueryParam("authoritative", newAuthoritative)
                        .build();
                // Redirect
                if (log.isDebugEnabled()) {
                  log.debug("Redirecting the rest call to {}", redirect);
                }
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
              }
            })
        .exceptionally(
            ex -> {
              if (ex.getCause() instanceof IllegalArgumentException
                  || ex.getCause() instanceof IllegalStateException) {
                if (log.isDebugEnabled()) {
                  log.debug("Failed to find owner for topic: {}", topicName, ex);
                }
                throw new RestException(
                    Response.Status.PRECONDITION_FAILED, "Can't find owner for topic " + topicName);
              } else if (ex.getCause() instanceof WebApplicationException) {
                throw (WebApplicationException) ex.getCause();
              } else {
                throw new RestException(ex.getCause());
              }
            });
  }
}
