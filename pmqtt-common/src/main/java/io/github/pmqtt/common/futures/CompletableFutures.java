package io.github.pmqtt.common.futures;

import io.netty.util.concurrent.Future;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.experimental.UtilityClass;

@SuppressWarnings("unchecked")
@UtilityClass
public final class CompletableFutures {

  public static <T> CompletableFuture<T> wrap(Future<T> future) {
    Objects.requireNonNull(future);
    final CompletableFuture<T> f = new CompletableFuture<>();
    future.addListener(
        result -> {
          if (result.isSuccess()) {
            f.complete((T) result.get());
          } else {
            f.completeExceptionally(result.cause());
          }
        });
    return f;
  }
}
