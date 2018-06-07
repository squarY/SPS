package com.netflix.sps.stage.impl;

import com.netflix.sps.data.StartEvent;
import com.netflix.sps.data.StartEventResult;
import java.util.HashMap;
import java.util.Map;
import com.netflix.sps.stage.DataStream;
import com.netflix.sps.stage.Stage;
import java.util.Set;


public class WindowCountStage extends Stage<StartEvent, StartEventResult> {
  private volatile Map<StartEventResult, Integer> eventCountMap = new HashMap<>();

  public WindowCountStage(DataStream<StartEvent> input, DataStream<StartEventResult> output, Set<Integer> partitionIds,
      long timeWindow) {
    super(input, output, partitionIds, timeWindow);
  }

  @Override
  public void process(StartEvent data, DataStream<StartEventResult> outputStream) {
    synchronized (eventCountMap) {
      StartEventResult result = new StartEventResult(data);
      if (eventCountMap.containsKey(result)) {
        eventCountMap.put(result, eventCountMap.get(result) + 1);
      } else {
        eventCountMap.put(result, 1);
      }
    }
  }

  @Override
  public void onTimeWindow(DataStream<StartEventResult> outputStream) {
    Map<StartEventResult, Integer> oldEventCountMap = null;
    synchronized (eventCountMap) {
      oldEventCountMap = this.eventCountMap;
      this.eventCountMap = new HashMap<>();
    }
    for (Map.Entry<StartEventResult, Integer> entry : oldEventCountMap.entrySet()) {
      StartEventResult result = entry.getKey();
      result.setSps(entry.getValue());
      outputStream.write(result);
    }
  }
}
