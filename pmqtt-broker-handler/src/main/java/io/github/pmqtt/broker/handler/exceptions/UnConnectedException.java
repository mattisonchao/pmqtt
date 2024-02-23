package io.github.pmqtt.broker.handler.exceptions;

public final class UnConnectedException extends MqttHandlerException {
  public UnConnectedException() {
    super("Receive message on unconnected connection.");
  }
}
