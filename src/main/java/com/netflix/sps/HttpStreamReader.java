package com.netflix.sps;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;


/**
 * Read event stream from the given http url.
 */
public class HttpStreamReader implements Closeable {
  private static Log LOGGER = LogFactory.getLog(HttpStreamReader.class);
  private CloseableHttpClient httpclient;
  private String url;
  private InputStream eventInputStream;
  private volatile boolean isRunning = false;
  private StreamEventProcessor processer;

  public HttpStreamReader(String url, StreamEventProcessor processer) {
    this.url = url;
    this.processer = processer;
  }

  public void connect()
      throws IOException {
    LOGGER.info("Sending request to: " + url);
    httpclient = HttpClients.createDefault();
    HttpGet httpget = new HttpGet(url);
    CloseableHttpResponse response = null;
    response = httpclient.execute(httpget);
    if (response.getStatusLine().getStatusCode() != 200) {
      String errorMsg =
          "Could not get correct response from url: " + url + ". ERROR:" + response.getStatusLine().getStatusCode()
              + " Reason: " + response.getStatusLine().getReasonPhrase();
      LOGGER.error(errorMsg);
      throw new IOException(errorMsg);
    }
    LOGGER.info("Connected to url: " + url);

    eventInputStream = response.getEntity().getContent();
    isRunning = true;
  }

  public void readStream()
      throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(eventInputStream));
    LOGGER.info("Start to read the stream.");
    while (isRunning) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      processer.process(line);
    }
    LOGGER.info("The stream ends or the reader is interrupted.");
  }

  @Override
  public void close()
      throws IOException {
    isRunning = false;
    if (httpclient != null) {
      httpclient.close();
    }
  }

  public interface StreamEventProcessor {
    void process(String line);
  }
}
