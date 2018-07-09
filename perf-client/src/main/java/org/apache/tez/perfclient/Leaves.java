package org.apache.tez.perfclient;

import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;

import java.util.concurrent.ThreadLocalRandom;

public class Leaves extends SimpleMRProcessor {
  double errorRate = 0.8;
  public Leaves(ProcessorContext context) {
    super(context);
  }

  @Override
  public void run() throws Exception {
    if(ThreadLocalRandom.current().nextDouble(0.7, 1) < errorRate) {
      throw new Exception("Aborting to produce more tasks");
    }
  }
}
