# SPS
## Brute force solution
The idea of the brute force solution(com.netflix.sps.bruteforce) is very simple: Read one stream->filter->count, then
output the result in every sec. The entire processing is run in one thread.

## SEDA solution
[SEDA](https://en.wikipedia.org/wiki/Staged_event-driven_architecture) is model which could make use of multipe cpu in
the event processing. I am familiar with this model and apply it in my previous work, so I choose SEDA in this submission.

The basic idea is splitting the entire process to couple of stages. Each stage could be run in multiple threads. The bridge
among the stages is the data stream. So the process now looks like a unix pipeline, so it's a unblock model, each stage
could process events as fast as they can, the data stream will buffer the input and output.
### Single machine SEDA solution
Each stages run in the same JVM, so I choose the in memory message queue as the implementation of the data stream. Finally
a pipeline is used to connect all stages to complete the analyze work.
### Distributed SEDA solution
I don't have enough time to implement the distribution solution.(When I complete the single machine SEDA, it already took 4 hours)
So I try to describe what I want to do here.

The basic idea is same, we could run the stages in multiple machines to scale out. There are two points are different from the
single machine solution:
- Instead of using in memory queue, we need to introduce the remote message queue like Kafka.
- We need a manager to manage the pipeline. We will give a description of the pipeline(code or config file) to the manager.
Manager will create the stage in proper machine and also create topic in kafka as the data stream. Also it need to track
the health and load of each stage to do the failover and load re-balance.

## Time window
The requirement of this task is group and count the start events in every sec. That means we need to maintain a time window with length 1 sec.
I noticed there is a field called "time" in each event, so the question is do we use the time that event was generated to decide the time window,
or use the client's time for the time window.

Finally I choose client's time. As it's a streaming processing application, it dose not make sense to wait the late even in any case. And in
the normal case there should be not too much difference between the event time and the client time as we process event very fast.


## How to run
```
# Clone from the github.
> git clone https://github.com/squarY/SPS.git
> cd SPS

# Build, it will download all dependencies.
> ./gradlew fatJar

# Run
> chmod +x bin/start.sh
# Brute force solution
> bin/start.sh simple
# SEDA solution
> bin/start.sh
```

