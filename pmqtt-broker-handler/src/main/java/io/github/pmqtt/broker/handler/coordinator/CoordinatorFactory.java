package io.github.pmqtt.broker.handler.coordinator;

import io.github.pmqtt.broker.handler.options.DuplicatedClientIdPolicy;
import javax.validation.constraints.NotNull;
import lombok.experimental.UtilityClass;
import org.apache.pulsar.broker.PulsarService;

@UtilityClass
public final class CoordinatorFactory {
  public static DistributedResourcesCoordinator createMetadata(
      @NotNull PulsarService pulsarService, DuplicatedClientIdPolicy duplicatedClientIdPolicy) {
    return new MetadataCoordinator(pulsarService, duplicatedClientIdPolicy);
  }
}
