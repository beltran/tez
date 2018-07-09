package org.apache.tez.perfclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.tez.client.CallerContext;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.DagContainerLauncher;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class PerformanceClient {
  static String ROOT = "Tokenizer";
  static String LEAVES = "leaves";
  private static final Logger LOG = LoggerFactory.getLogger(PerformanceClient.class);
  private static final int numLeaves = 100;
  private static final int numRunsPerWaitingTime = 20;
  private static int[] pollWaitingTime = {0, 10, 100, 1000, 10000};
  TezClient client;
  volatile boolean completed = false;


  private DAG createDAG(TezConfiguration tezConf, String inputPath, int numPartitions) throws IOException {
    DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(tezConf),
        TextInputFormat.class, inputPath).groupSplits(!false)
        .generateSplitsInAM(!false).build();

    Vertex rootVertex = Vertex.create(ROOT, ProcessorDescriptor.create(
        Root.class.getName()));
    rootVertex.addDataSource("Input", dataSource);

    Vertex endVertex = Vertex.create(LEAVES + "END",
        ProcessorDescriptor.create(Leaves.class.getName()), numPartitions);

    Vertex endVertexSecond = Vertex.create(LEAVES + "END_SECOND",
        ProcessorDescriptor.create(Leaves.class.getName()), numPartitions);

    OrderedPartitionedKVEdgeConfig edgeConf1 = OrderedPartitionedKVEdgeConfig
        .newBuilder(Text.class.getName(), IntWritable.class.getName(),
            HashPartitioner.class.getName())
        .setFromConfiguration(tezConf)
        .build();

    List<Vertex> leaves = new ArrayList<>(numLeaves);

    for (int i = 0; i < numLeaves; i++) {
      leaves.add(Vertex.create(LEAVES + i,
          ProcessorDescriptor.create(Leaves.class.getName()), numPartitions));

    }

    List<Vertex> secondLeaves = new ArrayList<>(numLeaves);
    for (int i = 0; i < numLeaves; i++) {
      secondLeaves.add(Vertex.create(LEAVES + i + "second",
          ProcessorDescriptor.create(Leaves.class.getName()), numPartitions));

    }

    DAG dag = DAG.create("LongMany");
    dag.addVertex(rootVertex);
    dag.addVertex(endVertex);

    for (Vertex v: leaves) {
      dag.addVertex(v);
      dag.addEdge(Edge.create(rootVertex, v,
          edgeConf1.createDefaultEdgeProperty()));
      dag.addEdge(Edge.create(v, endVertex,
          edgeConf1.createDefaultEdgeProperty()));
    }

    return dag;
  }

  public static void main(String[] args) throws Exception{

    for (int i = 0; i < pollWaitingTime.length; i++) {
      for (int j = 0; j < numRunsPerWaitingTime; j++) {
        LOG.info("Running: " + pollWaitingTime[i] + " for the " + (j + 1) + "th time");
        double timeTaken = new PerformanceClient().run(args[0], pollWaitingTime[i]);
        LOG.info("Took to run " + timeTaken + " " + pollWaitingTime[i]);
      }
    }
  }

  double run(String inputPath, int timeToPollInMillis) throws Exception{
    LOG.debug("Beginning client long many");
    TezConfiguration tezConfiguration = new TezConfiguration();
    tezConfiguration.setInt(
        TezConfiguration.TEZ_TASK_MAX_ALLOWED_OUTPUT_FAILURES,
        Integer.MAX_VALUE);
    client = createTezClient(tezConfiguration);

    DAG dag = createDAG(tezConfiguration, inputPath, 1);
    DAGClient dagClient = submitDAG(dag);
    double startTime = System.nanoTime() / 1000000000.0;

    while (true) {
      try {
        DAGStatus dagStatus = dagClient.getDAGStatus(EnumSet.of(StatusGetOpts.GET_COUNTERS));
        if (dagStatus.isCompleted()) {
          completed = true;
          break;
        }
        for (int j = 0; j < numLeaves; j++) {
          dagClient.getVertexStatus(LEAVES + j, EnumSet.of(StatusGetOpts.GET_COUNTERS));
        }
        Thread.sleep((long)(timeToPollInMillis *
            ThreadLocalRandom.current().nextDouble(0.7, 1)));
      } catch (Exception e) {
        LOG.error(e.toString(), e);
        e.printStackTrace();

      }
    }

    double stopTime = System.nanoTime() / 1000000000.0;
    double timeTaken =  (stopTime - startTime);

    LOG.debug("Ending performance client. All completed");
    return timeTaken;
  }

  DAGClient submitDAG(DAG dag) throws Exception{
    CallerContext callerContext = CallerContext.create("TezExamples",
        "Performance client DAG: " + dag.getName());
    dag.setCallerContext(callerContext);
    return client.submitDAG(dag);
  }

  TezClient createTezClient(TezConfiguration tezConfiguration) throws IOException, TezException {
    TezClient tezClient = TezClient.create("TezExampleApplication", tezConfiguration, false);
    tezClient.start();
    return tezClient;
  }

  void stopClient(TezConfiguration tezConfiguration) throws Exception {
    client.stop();
  }
}
