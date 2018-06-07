package com.netflix.sps.bruteforce;

import com.netflix.sps.http.HttpStreamReader;
import java.io.IOException;


public class SpsApplication {
  public static void main(String[] args) {
    System.out.println("The brute force solution for SPS.");
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
