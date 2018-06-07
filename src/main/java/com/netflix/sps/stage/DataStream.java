package com.netflix.sps.stage;

import java.util.Set;


public interface DataStream<T> {
  void write(T data);

  T read(int partitionId);

  int remain(int partitionId);

  int getPartitionCount();
}
