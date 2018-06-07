package com.netflix.sps.stage;

import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public abstract class Stage<I, O> implements Runnable {
  private static final Log LOGGER = LogFactory.getLog(Stage.class);
  private DataStream<I> input;
  private DataStream<O> output;
  private long timeWindow;
  private Set<Integer> intputPartitionSet;
  private Iterator<Integer> inputPartitionIter;

  public Stage(DataStream<I> input, DataStream<O> output, Set<Integer> inputPatitionSet) {
    this(input, output, inputPatitionSet, 0);
  }

  public Stage(DataStream<I> input, DataStream<O> output, Set<Integer> inputPartitionSet, long timeWindow) {
    this.input = input;
    this.output = output;
    this.timeWindow = timeWindow;
    this.intputPartitionSet = inputPartitionSet;
    this.inputPartitionIter = this.intputPartitionSet.iterator();
  }

  @Override
  public void run() {
    if (timeWindow > 0) {
      Timer timer = new Timer();
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          onTimeWindow(output);
        }
      }, timeWindow, timeWindow);
    }

    while (true) {
      I data = null;
      try {
        data = readData();
        if (data != null) {
          process(data, output);
        }
      } catch (Exception e) {
        LOGGER.warn("Error on processing input:" + data, e);
      }
    }
  }

  public I readData() {
    if (!inputPartitionIter.hasNext()) {
      // Go back the start.
      inputPartitionIter = intputPartitionSet.iterator();
    }
    if (inputPartitionIter.hasNext()) {
      return input.read(inputPartitionIter.next());
    } else {
      throw new IllegalStateException("No partition id is available.");
    }
  }

  public abstract void process(I data, DataStream<O> outputStream);

  public void onTimeWindow(DataStream<O> outputStream) {
    // By default do nothing, sub-class could override this method to implement the time window feature.
  }
}
