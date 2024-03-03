package io.github.pmqtt.broker.handler.exceptions;

public class MqttHandlerException extends RuntimeException {

  public MqttHandlerException(String message) {
    super(message);
  }

  public MqttHandlerException(Throwable cause) {
    super(cause);
  }
}
