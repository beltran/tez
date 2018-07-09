package org.apache.tez.perfclient;

import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.examples.TezExampleBase;

public class PerformanceExample extends TezExampleBase {
  @Override protected void printUsage() {

  }

  @Override protected int validateArgs(String[] otherArgs) {
    return 0;
  }

  @Override protected int runJob(String[] args, TezConfiguration tezConf, TezClient tezClient) throws Exception {
    return 0;
  }
}
