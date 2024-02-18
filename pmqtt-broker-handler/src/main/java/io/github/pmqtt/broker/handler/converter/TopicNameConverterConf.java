package io.github.pmqtt.broker.handler.converter;

import javax.validation.constraints.NotNull;

public record TopicNameConverterConf(
    @NotNull String defaultTenant, @NotNull String defaultNamespace) {}
