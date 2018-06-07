package com.netflix.sps.stage;

/**
 * Data stream that play as the bridge among the stages. Data instance should be transferred from one stage to another
 * stage through one or multiple streams.
 */
public interface DataStream<T> {
  void write(T data);

  T read(int partitionId);

  /**
   * How many data instances is remaining in the given partition in this stream.
   */
  int remain(int partitionId);

  int getPartitionCount();
}
