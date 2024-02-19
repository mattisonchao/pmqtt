package io.github.pmqtt.broker.handler.exceptions;

public sealed class MqttHandlerException extends RuntimeException permits UnauthorizedException {

  public MqttHandlerException(String message) {
    super(message);
  }
}
