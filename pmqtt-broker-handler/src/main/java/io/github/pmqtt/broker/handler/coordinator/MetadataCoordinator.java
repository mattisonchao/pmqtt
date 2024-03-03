package io.github.pmqtt.broker.handler.coordinator;

import io.github.pmqtt.broker.handler.exceptions.CoordinatorException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.jetbrains.annotations.Nullable;

@Slf4j
final class MetadataCoordinator implements DistributedResourcesCoordinator {
  private static final String METADATA_COORDINATOR_PATH_PREDIX = "/pmqtt/coordinator/";
  private final LockManager<String> lockManager;
  private final Map<String, CompletableFuture<ResourceLock<String>>> locks;

  MetadataCoordinator(@NotNull LockManager<String> lockManager) {
    this.lockManager = lockManager;
    this.locks = new ConcurrentHashMap<>();
  }

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
        locks.computeIfAbsent(
            resourceKey,
            __ -> {
              updated.setValue(true);
              return lockManager.acquireLock(
                  METADATA_COORDINATOR_PATH_PREDIX + resourceKey, content);
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
