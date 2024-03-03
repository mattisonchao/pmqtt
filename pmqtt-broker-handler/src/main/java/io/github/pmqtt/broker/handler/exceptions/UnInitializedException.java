package io.github.pmqtt.broker.handler.exceptions;

import javax.validation.constraints.NotNull;
import lombok.Getter;

@Getter
public final class UnInitializedException extends CoordinatorException {
  private final String resourceKey;

  public UnInitializedException(@NotNull String resourceKey) {
    super("Resource lock is not initialized. resource_key=" + resourceKey);
    this.resourceKey = resourceKey;
  }
}
