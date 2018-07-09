package org.apache.tez.perfclient;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

public class Root extends SimpleProcessor {
  IntWritable one = new IntWritable(1);
  Text word = new Text();

  public Root(ProcessorContext context) {
    super(context);
  }

  @Override
  public void run() throws Exception {
    try
    {
      Thread.sleep(1 * 1000);
    }
    catch(InterruptedException ex)
    {
      Thread.currentThread().interrupt();
    }
  }

}