package com.netflix.sps.stage.impl;

import com.google.gson.Gson;
import com.netflix.sps.data.StartEvent;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import com.netflix.sps.stage.DataStream;
import com.netflix.sps.stage.Stage;


public class FilterStage extends Stage<String, StartEvent> {
  private Gson gson = new Gson();

  public FilterStage(DataStream<String> input, DataStream<StartEvent> output, Set<Integer> partitionIds) {
    super(input, output, partitionIds);
  }

  @Override
  public void process(String data, DataStream<StartEvent> outputStream) {
    int start = data.indexOf(':') + 1;
    StartEvent result = gson.fromJson(data.substring(start), StartEvent.class);
    if (result != null && result.isSuccess() && StringUtils.isNotBlank(result.getDevice()) && StringUtils
        .isNotBlank(result.getTitle()) && StringUtils.isNotBlank(result.getCountry())) {
      outputStream.write(result);
    }
  }
}
