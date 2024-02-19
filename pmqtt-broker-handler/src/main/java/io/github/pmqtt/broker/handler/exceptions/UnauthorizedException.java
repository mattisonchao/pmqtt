package io.github.pmqtt.broker.handler.exceptions;

import lombok.Getter;

@Getter
public final class UnauthorizedException extends MqttHandlerException {
  private final String subject;
  private final String resource;
  private final String action;

  public UnauthorizedException(String subject, String resource, String action) {
    super(
        String.format(
            "Authorization verification failed. subject=%s resource=%s action=%s",
            subject, resource, action));
    this.subject = subject;
    this.resource = resource;
    this.action = action;
  }
}
