package com.netflix.sps.stage.impl;

import com.netflix.sps.data.StartEvent;
import com.netflix.sps.data.StartEventResult;
import com.netflix.sps.stage.DataStream;
import com.netflix.sps.stage.singlemachine.InMemoryDataStream;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;


public class WindowCountStageTest {
  @Test
  public void testCount()
      throws InterruptedException {
    int inputPartitionCount = 2;
    DataStream<StartEvent> inputStream = new InMemoryDataStream<>(inputPartitionCount,
        event -> Math.abs(new StartEventResult(event).hashCode()) % inputPartitionCount);

    int eventCount = 100;
    for (int i = 0; i < eventCount; i++) {
      StartEvent event = new StartEvent();
      int testId = i % 2;
      event.setCountry("test" + testId);
      event.setDevice("test" + testId);
      event.setTitle("test" + testId);
      inputStream.write(event);
    }
    DataStream<StartEventResult> outputStream = new InMemoryDataStream<>(1, data -> 0);
    for (int partition = 0; partition < inputPartitionCount; partition++) {

      Set<Integer> partitionSet = new HashSet<>();
      partitionSet.add(partition);
      WindowCountStage stage = new WindowCountStage(inputStream, outputStream, partitionSet, 500);
      new Thread(stage).start();
    }
    Thread.sleep(1000);
    Assert.assertEquals(2, outputStream.remain(0));
    Assert.assertEquals(50, outputStream.read(0).getSps());
    Assert.assertEquals(50, outputStream.read(0).getSps());
  }
}
