package io.github.pmqtt.broker.handler.connection;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public final class PacketIdContext {
  private long ledgerId;
  private long entryId;
  private boolean batch;
  private int batchSize;
  private int batchIndex;
}
