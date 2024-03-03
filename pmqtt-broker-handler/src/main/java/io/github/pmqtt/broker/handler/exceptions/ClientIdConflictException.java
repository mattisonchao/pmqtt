package io.github.pmqtt.broker.handler.exceptions;

import lombok.Getter;

@Getter
public final class ClientIdConflictException extends MqttHandlerException {
  private final String clientId;

  public ClientIdConflictException(String clientId) {
    super(String.format("Client id is conflict. client_id=%s", clientId));
    this.clientId = clientId;
  }
}
