package com.netflix.sps.stage.impl;

import com.google.gson.Gson;
import com.netflix.sps.data.StartEventResult;
import com.netflix.sps.stage.DataStream;
import com.netflix.sps.stage.Stage;
import java.util.Set;


/**
 * The termination stage. Just print all results it received.
 */
public class SinkStage extends Stage<StartEventResult, Void> {
  private Gson gson = new Gson();

  public SinkStage(DataStream<StartEventResult> input, DataStream<Void> output, Set<Integer> inputPatitionSet) {
    super(input, output, inputPatitionSet);
  }

  @Override
  public void process(StartEventResult data, DataStream<Void> outputStream) {
    String result = gson.toJson(data, StartEventResult.class);
    System.out.println(result);
  }
}
