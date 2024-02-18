package io.github.pmqtt.broker.handler.converter;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.InvocationTargetException;
import javax.validation.constraints.NotNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public final class TopicNameConverterFactory {

  public static @NotNull TopicNameConverter create(
      @NotNull String converterClassName,
      @NotNull String defaultTenant,
      @NotNull String defaultNamespace)
      throws ClassNotFoundException {
    requireNonNull(converterClassName);
    requireNonNull(defaultTenant);
    requireNonNull(defaultNamespace);
    final Class<?> klass = Class.forName(converterClassName);
    try {
      final TopicNameConverter converter =
          (TopicNameConverter) klass.getDeclaredConstructor().newInstance();
      final TopicNameConverterConf conf =
          new TopicNameConverterConf(defaultTenant, defaultNamespace);
      converter.init(conf);
      return converter;
    } catch (InvocationTargetException
        | InstantiationException
        | IllegalAccessException
        | NoSuchMethodException ex) {
      log.error("Load topic name converter failed. ", ex);
      throw new IllegalStateException("Load topic name converter failed", ex);
    }
  }
}
