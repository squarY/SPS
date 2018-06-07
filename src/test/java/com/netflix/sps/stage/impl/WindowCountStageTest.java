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
    // Create an input stream with 2 partitions.
    DataStream<StartEvent> inputStream = new InMemoryDataStream<>(inputPartitionCount,
        event -> Math.abs(new StartEventResult(event).hashCode()) % inputPartitionCount);
    // Create 100 StartEvent instances which could be grouped by two categories.
    int eventCount = 100;
    for (int i = 0; i < eventCount; i++) {
      StartEvent event = new StartEvent();
      int testId = i % 2;
      event.setCountry("test" + testId);
      event.setDevice("test" + testId);
      event.setTitle("test" + testId);
      inputStream.write(event);
    }
    // Create an output stream with 1 partition
    DataStream<StartEventResult> outputStream = new InMemoryDataStream<>(1, data -> 0);
    // Create 2 window count stage threads.
    for (int partition = 0; partition < inputPartitionCount; partition++) {
      Set<Integer> partitionSet = new HashSet<>();
      partitionSet.add(partition);
      WindowCountStage stage = new WindowCountStage(inputStream, outputStream, partitionSet, 500);
      new Thread(stage).start();
    }
    Thread.sleep(1000);
    // We should have 2 results for 2 group.
    Assert.assertEquals(2, outputStream.remain(0));
    // And each of the group should be counted 50 times.
    Assert.assertEquals(50, outputStream.read(0).getSps());
    Assert.assertEquals(50, outputStream.read(0).getSps());
  }
}
