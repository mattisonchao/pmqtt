package io.github.pmqtt.broker.handler.connection;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;

public record RawMessage(@JsonProperty("payload") @NotNull byte[] payload) {}
