package com.netflix.sps.stage.impl;

import com.netflix.sps.data.StartEvent;
import com.netflix.sps.stage.DataStream;
import com.netflix.sps.stage.singlemachine.InMemoryDataStream;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;


public class FilterStageTest {
  @Test
  public void testFilter()
      throws InterruptedException {
    int partitionCount = 2;
    DataStream<String> inputStream = new InMemoryDataStream<>(partitionCount, data -> Math.abs(data.hashCode() % partitionCount));
    int errorCount = 100;
    int successCount = 100;
    for (int i = 0; i < errorCount; i++) {
      inputStream.write("data: {\"device\":\"xbox_one\",\"sev\":\"error\",\"title\":\"test_error" + i
          + "\",\"country\":\"IND\",\"time\":1515445354624}");
    }
    for (int i = 0; i < successCount; i++) {
      inputStream.write("data: {\"device\":\"xbox_one_s\",\"sev\":\"success\",\"title\":\"test_success" + i
          + "\",\"country\":\"IND\",\"time\":1515445354624}");
    }
    DataStream<StartEvent> outputStream = new InMemoryDataStream<>(1, data -> 0);
    for (int partition = 0; partition < partitionCount; partition++) {

      Set<Integer> partitionSet = new HashSet<>();
      partitionSet.add(partition);
      FilterStage stage = new FilterStage(inputStream, outputStream, partitionSet);
      new Thread(stage).start();
    }
    Thread.sleep(1000l);
    Assert.assertEquals(successCount, outputStream.remain(0));
    StartEvent startEvent = outputStream.read(0);
    Assert.assertEquals(startEvent.getDevice(), "xbox_one_s");
  }
}
