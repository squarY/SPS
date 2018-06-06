package com.netflix.sps;

import java.io.IOException;


public class SpsApplication {
  public static void main(String[] args) {
    String url = "https://tweet-service.herokuapp.com/sps";
    long timeWindow = 1000l;
    try (HttpStreamReader reader = new HttpStreamReader(url, new SpsAnalyzer(timeWindow))) {
      reader.connect();
      reader.readStream();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
