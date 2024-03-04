package io.github.pmqtt.broker.handler.coordinator;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.validation.constraints.NotNull;
import org.jetbrains.annotations.Nullable;

public final class CoordinatorDisable implements DistributedResourcesCoordinator {
  public static final DistributedResourcesCoordinator INSTANCE = new CoordinatorDisable();

  @Override
  public CompletableFuture<Boolean> tryAcquireAsync(
      @NotNull String applicantId,
      String resourceKey,
      @Nullable BiConsumer<Void, ? super Throwable> whenLockExpired,
      @Nullable Consumer<Supplier<CompletableFuture<Void>>> releaseFunc) {
    if (releaseFunc != null) {
      releaseFunc.accept(() -> CompletableFuture.completedFuture(null));
    }
    return CompletableFuture.completedFuture(true);
  }
}
