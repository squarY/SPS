package com.netflix.sps.stage.singlemachine;

import com.netflix.sps.http.HttpStreamReader;
import com.netflix.sps.data.StartEvent;
import com.netflix.sps.data.StartEventResult;
import com.netflix.sps.stage.DataStream;
import com.netflix.sps.stage.Stage;
import com.netflix.sps.stage.impl.FilterStage;
import com.netflix.sps.stage.impl.SinkStage;
import com.netflix.sps.stage.impl.WindowCountStage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * The pipeline of all stages and streams. It's a kind of a topology of the streaming job.
 */
public class Pipeline {
  //TODO use a graph to store all stage and their relationship.
  List<Stage> stageList = new ArrayList<>();

  public void start() {
    // Start each stage in the thread.
    for (Stage stage : stageList) {
      new Thread(stage).start();
    }
  }

  public void preparePipeline(DataStream<String> firstStream) {
    // create 2 filter stages, 2 count stages and 1 sink stage.
    // TODO  the partition count and stage count should come from some config files
    DataStream<StartEvent> filteredStream = filter(firstStream, 2, 2);
    DataStream<StartEventResult> aggregatedStream = count(filteredStream, 2);
    print(aggregatedStream);
  }

  public DataStream<StartEvent> filter(DataStream<String> inputStream, int outputPartitionCount, int workerNum) {
    // Create an output stream with multiple partitions. Will partition the event based on device,title and
    // country(implemented by the hashcode() method of StartEventResult.
    DataStream<StartEvent> outputStream = new InMemoryDataStream<>(outputPartitionCount,
        event -> Math.abs(new StartEventResult(event).hashCode() % outputPartitionCount));
    List<Set<Integer>> workerPartitionSets = getInterestedPartitionSet(inputStream.getPartitionCount(), workerNum);
    for (int i = 0; i < workerNum; i++) {
      FilterStage filterStage = new FilterStage(inputStream, outputStream, workerPartitionSets.get(i));
      stageList.add(filterStage);
    }
    return outputStream;
  }

  public DataStream<StartEventResult> count(DataStream<StartEvent> inputStream, int workerNum) {
    // Create an output stream with 1 partition for all results.
    DataStream<StartEventResult> outputStream = new InMemoryDataStream<>(1, result -> 0);
    List<Set<Integer>> workerPartitionSets = getInterestedPartitionSet(inputStream.getPartitionCount(), workerNum);
    for (int i = 0; i < workerNum; i++) {
      WindowCountStage countStage = new WindowCountStage(inputStream, outputStream, workerPartitionSets.get(i), 1000l);
      stageList.add(countStage);
    }
    return outputStream;
  }

  public void print(DataStream<StartEventResult> inputStream) {
    Set<Integer> partitionSet = new HashSet<>();
    partitionSet.add(0);
    SinkStage sinkStage = new SinkStage(inputStream, null, partitionSet);
    stageList.add(sinkStage);
  }

  private List<Set<Integer>> getInterestedPartitionSet(int partitionCount, int workerNum) {
    // Use the round robin method to assign partitions for each stages.
    // E.g we have 10 partitions and 3 stage. So the stage0 should get partition 0,3,6,9; stage1 gets 1,4,7
    // And stage2 gets 2,4,8
    List<Set<Integer>> workerPartitionSets = new ArrayList<>(workerNum);
    for (int i = 0; i < workerNum; i++) {
      workerPartitionSets.add(new HashSet<>());
    }
    int worker = 0;
    for (int partition = 0; partition < partitionCount; partition++) {
      if (worker == workerNum) {
        worker = 0;
      }
      workerPartitionSets.get(worker++).add(partition);
    }
    return workerPartitionSets;
  }

  public static void main(String[] args)
      throws IOException {
    System.out.println("The SEDA solution for the SPS.");
    Pipeline pipeline = new Pipeline();
    // Create a http read to read the stream from the remote and output each line to the in memory stream with 2
    // partitions.
    final DataStream<String> httpEventStream = new InMemoryDataStream<>(2, event -> Math.abs(event.hashCode() % 2));
    HttpStreamReader reader =
        new HttpStreamReader("https://tweet-service.herokuapp.com/sps", line -> httpEventStream.write(line));
    // Create job topology
    pipeline.preparePipeline(httpEventStream);
    // Start the job.
    pipeline.start();
    // Connect to the target url.
    reader.connect();
    // Start to read stream.
    reader.readStream();
  }
}
