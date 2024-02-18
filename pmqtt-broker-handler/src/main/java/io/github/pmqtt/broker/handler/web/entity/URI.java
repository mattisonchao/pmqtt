package io.github.pmqtt.broker.handler.web.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class URI {
  private String schema;
  private String host;
  private int port;
}
