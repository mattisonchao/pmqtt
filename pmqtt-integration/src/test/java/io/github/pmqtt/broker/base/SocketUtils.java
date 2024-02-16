package io.github.pmqtt.broker.base;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Random;
import javax.net.ServerSocketFactory;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class SocketUtils {
  public static int findAvailableTcpPort() {
    int candidatePort;
    int searchCounter = 0;
    do {
      if (searchCounter > MAX_ATTEMPTS) {
        throw new IllegalStateException(
            String.format(
                "Could not find an available TCP port in the range [%d, %d] after %d attempts",
                PORT_RANGE_MIN, PORT_RANGE_MAX, MAX_ATTEMPTS));
      }
      candidatePort = PORT_RANGE_MIN + random.nextInt(PORT_RANGE + 1);
      searchCounter++;
    } while (!isPortAvailable(candidatePort));

    return candidatePort;
  }

  private static boolean isPortAvailable(int port) {
    try {
      ServerSocket serverSocket =
          ServerSocketFactory.getDefault()
              .createServerSocket(port, 1, InetAddress.getByName("localhost"));
      serverSocket.close();
      return true;
    } catch (Exception ex) {
      return false;
    }
  }

  private static final int PORT_RANGE_MIN = 1024;
  private static final int PORT_RANGE_MAX = 65535;
  private static final int PORT_RANGE = PORT_RANGE_MAX - PORT_RANGE_MIN;
  private static final int MAX_ATTEMPTS = 1_000;
  private static final Random random = new Random(System.nanoTime());
}
