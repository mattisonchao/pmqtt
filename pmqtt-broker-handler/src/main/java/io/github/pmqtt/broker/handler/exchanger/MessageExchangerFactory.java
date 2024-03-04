package io.github.pmqtt.broker.handler.exchanger;

import io.github.pmqtt.broker.handler.MqttContext;
import java.util.Objects;
import javax.validation.constraints.NotNull;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class MessageExchangerFactory {

  public static MessageExchanger createPulsarInternal(@NotNull MqttContext context) {
    Objects.requireNonNull(context);
    return new PulsarInternalMessageExchanger(context);
  }
}
