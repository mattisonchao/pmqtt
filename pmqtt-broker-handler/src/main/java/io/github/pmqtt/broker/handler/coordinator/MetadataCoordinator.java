package io.github.pmqtt.broker.handler.coordinator;

import io.github.pmqtt.broker.handler.exceptions.CoordinatorException;
import io.github.pmqtt.broker.handler.options.DuplicatedClientIdPolicy;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.jetbrains.annotations.Nullable;

@Slf4j
final class MetadataCoordinator implements DistributedResourcesCoordinator {
  private static final String METADATA_COORDINATOR_PATH_PREDIX = "/pmqtt/coordinator/";
  private final LockManager<String> lockManager;
  private final MetadataStore lockManagerMeta;
  private final Map<String, CompletableFuture<ResourceLock<String>>> locks;
  private final DuplicatedClientIdPolicy duplicatedClientIdPolicy;

  MetadataCoordinator(
      @NotNull PulsarService pulsarService,
      @NotNull DuplicatedClientIdPolicy duplicatedClientIdPolicy) {
    this.lockManager = pulsarService.getCoordinationService().getLockManager(String.class);
    this.lockManagerMeta = pulsarService.getLocalMetadataStore();
    this.locks = new ConcurrentHashMap<>();
    this.duplicatedClientIdPolicy = duplicatedClientIdPolicy;
  }

  @SuppressWarnings("UnstableApiUsage")
  @Override
  public CompletableFuture<Boolean> tryAcquireAsync(
      @NotNull String applicantId,
      @NotNull String resourceKey,
      @Nullable BiConsumer<Void, ? super Throwable> whenLockExpired,
      @Nullable Consumer<Supplier<CompletableFuture<Void>>> releaseFunc) {
    Objects.requireNonNull(applicantId);
    Objects.requireNonNull(resourceKey);

    // This value is used for avoid steal ds lock in pulsar implementation.
    final String content = applicantId + ":" + resourceKey;
    final MutableBoolean updated = new MutableBoolean(false);
    final var lockFuture =
        locks.compute(
            resourceKey,
            (__, existingValue) ->
                switch (duplicatedClientIdPolicy) {
                  case REJECT -> {
                    // because the resource lock behind lock manager auto support steal lock.
                    // we should use concurrent map in the local broker to avoid lock stealing.
                    if (existingValue == null) {
                      updated.setValue(true);
                      yield lockManager.acquireLock(
                          METADATA_COORDINATOR_PATH_PREDIX + resourceKey, content);
                    }
                    yield existingValue;
                  }
                  case KICK_OUT_EXISTING -> {
                    // rely on the resource lock stealing feature. we don't need care about the
                    // previous lock here,
                    // because the notification mechanism will call lock expired callback.
                    updated.setValue(true);

                    // for same session to release the current lock first. because the same session
                    // will not call lock expired callback
                    final CompletableFuture<Void> stepFuture;
                    if (existingValue == null) {
                      stepFuture = CompletableFuture.completedFuture(null);
                    } else {
                      stepFuture =
                          existingValue
                              .exceptionally(ex -> null) // ignore exception
                              .thenCompose(
                                  resourceLock -> {
                                    if (resourceLock != null) {
                                      // If this lock release got an exception, we should block the
                                      // new lock from stealing
                                      // todo: do some test with release failed, we should let admin
                                      // has an approach to fix it
                                      return resourceLock.release();
                                    }
                                    return CompletableFuture.completedFuture(null);
                                  });
                    }
                    // todo: maybe we should do self-spin here, because the other broker may try
                    // lock quicker than us
                    yield stepFuture
                        .thenCompose(
                            ignore ->
                                // force delete exist node
                                lockManagerMeta.delete(
                                    METADATA_COORDINATOR_PATH_PREDIX + resourceKey,
                                    Optional.empty()))
                        .exceptionally(
                            ex -> {
                              final Throwable rc = FutureUtil.unwrapCompletionException(ex);
                              if (rc instanceof MetadataStoreException.NotFoundException) {
                                return null;
                              }
                              throw new CompletionException(rc);
                            })
                        .thenCompose(
                            unused ->
                                lockManager.acquireLock(
                                    METADATA_COORDINATOR_PATH_PREDIX + resourceKey, content));
                  }
                });
    if (!updated.booleanValue()) {
      return CompletableFuture.completedFuture(false);
    }
    return lockFuture
        .thenApply(
            resourceLock -> {
              // register listener
              final CompletableFuture<Void> lockExpiredFuture = resourceLock.getLockExpiredFuture();
              // because the future has a stack inside, the remove operation will be the last step.
              lockExpiredFuture.whenComplete(
                  (result, error) -> {
                    locks.remove(resourceKey, lockFuture); // remove self
                    if (error != null) {
                      // it shouldn't be happened
                      log.warn(
                          "Receive an unexpected error when lock expire. resource_key={}",
                          resourceKey,
                          error);
                    }
                  });
              // register the user's expired future
              if (whenLockExpired != null) {
                lockExpiredFuture.whenComplete(whenLockExpired);
              }
              if (releaseFunc != null) {
                final Supplier<CompletableFuture<Void>> a =
                    () ->
                        resourceLock
                            .release()
                            .exceptionally(
                                ex -> {
                                  final Throwable rc = FutureUtil.unwrapCompletionException(ex);
                                  log.error(
                                      "Receive an error when release the resource lock. resource_key={}",
                                      resourceKey,
                                      rc);
                                  throw new CoordinatorException(rc);
                                });
                releaseFunc.accept(a);
              }
              return true;
            })
        .exceptionally(
            ex -> {
              final Throwable rc = FutureUtil.unwrapCompletionException(ex);
              if (rc instanceof MetadataStoreException.LockBusyException) {
                locks.remove(resourceKey, lockFuture);
                return false;
              }
              log.error(
                  "Receive an error when acquire the resource lock. resource_key={}",
                  resourceKey,
                  rc);
              locks.remove(resourceKey, lockFuture);
              throw new CoordinatorException(rc);
            });
  }
}
