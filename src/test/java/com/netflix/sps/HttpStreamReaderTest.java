package com.netflix.sps;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class HttpStreamReaderTest {
  private int localPort = 7654;
  private int lineCount = 10;
  private HttpServer server;

  @Before
  public void setup()
      throws IOException {
    server = HttpServer.create(new InetSocketAddress(localPort), 0);
    server.createContext("/sps", httpExchange -> {
      httpExchange.sendResponseHeaders(200, 0);
      OutputStream os = httpExchange.getResponseBody();
      for (int i = 0; i < lineCount; i++) {
        byte[] line = "testline\n".getBytes();
        os.write(line);
      }

      os.close();
    });
    server.setExecutor(null);
    server.start();
  }

  @After
  public void cleanup() {
    server.stop(0);
  }

  @Test
  public void testReadStream()
      throws IOException {
    String url = "http://localhost:" + localPort + "/sps";
    final int[] count = new int[1];
    HttpStreamReader reader = new HttpStreamReader(url, line -> count[0]++);
    try {
      reader.connect();
      reader.readStream();
      Assert.assertEquals("The number of lines read from the stream is wrong.", count[0], lineCount);
    } catch (IOException e) {
      Assert.fail("Error on reading the stream." + e.getMessage());
    } finally {
      reader.close();
    }
  }
}
