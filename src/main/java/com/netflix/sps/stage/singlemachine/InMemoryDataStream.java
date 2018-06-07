package com.netflix.sps.stage.singlemachine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import com.netflix.sps.stage.DataStream;
import java.util.function.Function;


public class InMemoryDataStream<T> implements DataStream<T> {
  private Map<Integer, BlockingQueue<T>> partitionedQueues = new HashMap<>();
  private Function<T, Integer> partitionFunc;

  public InMemoryDataStream(int partitionCount, Function<T, Integer> partitionFunc) {
    this.partitionFunc = partitionFunc;
    partitionedQueues = new HashMap<>();
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      partitionedQueues.put(partitionId, new LinkedBlockingQueue<T>());
    }
  }

  @Override
  public void write(T data) {
    int partitionId = partitionFunc.apply(data);
    // TODO set the timeout value to prevent it from waiting for ever. And also the retry logic.
    getQueue(partitionId).offer(data);
  }

  @Override
  public T read(int partitionId) {
    // TODO set the timeout value to prevent it from waiting for ever. And also the retry logic.
    return getQueue(partitionId).poll();
  }

  @Override
  public int remain(int partitionId) {
    return getQueue(partitionId).size();
  }

  @Override
  public int getPartitionCount() {
    return partitionedQueues.size();
  }

  private BlockingQueue<T> getQueue(int partitionId) {
    BlockingQueue<T> queue = partitionedQueues.get(partitionId);
    if (queue == null) {
      throw new IllegalStateException(
          "Illegal partition: " + partitionId + ", could not found the related queue for this partition id.");
    }
    return queue;
  }
}
