package io.github.pmqtt.broker.handler.coordinator;

import javax.validation.constraints.NotNull;
import lombok.experimental.UtilityClass;
import org.apache.pulsar.metadata.api.coordination.LockManager;

@UtilityClass
public final class CoordinatorFactory {
  public static DistributedResourcesCoordinator createMetadata(
      @NotNull LockManager<String> lockManager) {
    return new MetadataCoordinator(lockManager);
  }
}
