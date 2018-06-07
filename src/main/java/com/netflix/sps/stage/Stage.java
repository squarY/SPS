package com.netflix.sps.stage;

import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Stage is a kind of worker which will do the real computation work. Each stage has an input stream and an output
 * stream so that the data could be flow into and flow out of this stage.
 *
 * Stage only read data from the interested partitions in the input stream and we use round robin to iterate from the
 * different input partitions. Regarding to the write, output stream will decide which partition the given data will be
 * written to.
 *
 * If it's a time window stage, a timer will be initialized to execute the regular task on time.
 */
public abstract class Stage<I, O> implements Runnable {
  private static final Log LOGGER = LogFactory.getLog(Stage.class);
  private DataStream<I> input;
  private DataStream<O> output;
  private long timeWindow;
  private Set<Integer> inputPartitionSet;
  private Iterator<Integer> inputPartitionIter;

  /**
   * Create a stage.
   * @param input input stream.
   * @param output output stream
   * @param inputPartitionSet The partitions this stage are interested in the input stream.
   */
  public Stage(DataStream<I> input, DataStream<O> output, Set<Integer> inputPartitionSet) {
    this(input, output, inputPartitionSet, 0);
  }

  public Stage(DataStream<I> input, DataStream<O> output, Set<Integer> inputPartitionSet, long timeWindow) {
    this.input = input;
    this.output = output;
    this.timeWindow = timeWindow;
    this.inputPartitionSet = inputPartitionSet;
    this.inputPartitionIter = this.inputPartitionSet.iterator();
  }

  @Override
  public void run() {
    initTimer();
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
    // Choose the partition by the round robin method.
    if (!inputPartitionIter.hasNext()) {
      // Go back the start.
      inputPartitionIter = inputPartitionSet.iterator();
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

  private void initTimer() {
    if (timeWindow > 0) {
      Timer timer = new Timer();
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          onTimeWindow(output);
        }
      }, timeWindow, timeWindow);
    }
  }
}
