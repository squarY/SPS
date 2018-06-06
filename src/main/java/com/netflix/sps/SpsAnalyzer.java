package com.netflix.sps;

import com.google.gson.Gson;
import com.netflix.sps.data.StartEvent;
import com.netflix.sps.data.StartEventResult;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Analyze the given start per second even. Count the successful event grouped by title, country and device for every
 * second.
 */
public class SpsAnalyzer implements HttpStreamReader.StreamEventProcessor {
  private static final Log LOGGER = LogFactory.getLog(SpsAnalyzer.class);
  private Gson gson;
  private volatile Map<StartEventResult, Integer> eventCountMap;
  private OutputStream output;

  public SpsAnalyzer(long timeWindow) {
    this.gson = new Gson();
    output = System.out;
    eventCountMap = new HashMap<>();
    if (timeWindow > 0) {
      Timer timer = new Timer();
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          onTimeWindow(output);
        }
      }, timeWindow, timeWindow);
    }
  }

  @Override
  public void process(String line) {
    count(filter(line));
  }

  private StartEvent filter(String line) {
    try {
      if (StringUtils.isNotBlank(line)) {
        int start = line.indexOf(':') + 1;
        StartEvent result = gson.fromJson(line.substring(start), StartEvent.class);
        if (result.isSuccess() && StringUtils.isNotBlank(result.getDevice()) && StringUtils
            .isNotBlank(result.getTitle()) && StringUtils.isNotBlank(result.getCountry())) {
          return result;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Error on filtering com.netflix.sps.data: " + line, e);
    }
    return null;
  }

  private void count(StartEvent event) {
    if (event == null) {
      return;
    }
    synchronized (eventCountMap) {
      StartEventResult result = new StartEventResult(event);
      if (eventCountMap.containsKey(result)) {
        eventCountMap.put(result, eventCountMap.get(result) + 1);
      } else {
        eventCountMap.put(result, 1);
      }
    }
  }

  public void onTimeWindow(OutputStream outputStream) {
    Map<StartEventResult, Integer> oldEventCountMap = null;
    synchronized (eventCountMap) {
      oldEventCountMap = this.eventCountMap;
      this.eventCountMap = new HashMap<>();
    }
    for (Map.Entry<StartEventResult, Integer> entry : oldEventCountMap.entrySet()) {
      StartEventResult result = entry.getKey();
      result.setSps(entry.getValue());
      String line = gson.toJson(result, StartEventResult.class) + "\n";
      try {
        outputStream.write(line.getBytes());
      } catch (IOException e) {
        LOGGER.warn("Could not write result: " + line + " to output stream.", e);
      }
    }
  }
}
