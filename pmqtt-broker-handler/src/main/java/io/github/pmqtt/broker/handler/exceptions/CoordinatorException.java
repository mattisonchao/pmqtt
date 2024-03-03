package io.github.pmqtt.broker.handler.exceptions;

public class CoordinatorException extends MqttHandlerException {
  public CoordinatorException(String message) {
    super(message);
  }

  public CoordinatorException(Throwable cause) {
    super(cause);
  }
}
