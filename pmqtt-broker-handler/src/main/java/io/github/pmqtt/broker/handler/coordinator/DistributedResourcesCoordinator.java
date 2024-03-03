package io.github.pmqtt.broker.handler.coordinator;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.validation.constraints.NotNull;
import org.jetbrains.annotations.Nullable;

public interface DistributedResourcesCoordinator {

  @NotNull CompletableFuture<Boolean> tryAcquireAsync(
      @NotNull String applicantId,
      @NotNull String resourceKey,
      @Nullable BiConsumer<Void, ? super Throwable> whenLockExpired,
      @Nullable Consumer<Supplier<CompletableFuture<Void>>> releaseFunc);
}
