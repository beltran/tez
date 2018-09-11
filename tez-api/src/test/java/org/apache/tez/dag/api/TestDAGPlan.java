/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.api;

import java.nio.ByteBuffer;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.JavaOptsChecker;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.Vertex.VertexExecutionContext;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexType;
import org.apache.tez.dag.api.records.DAGProtos.VertexExecutionContextProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.serviceplugins.api.ContainerLauncherDescriptor;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicatorDescriptor;
import org.apache.tez.serviceplugins.api.TaskSchedulerDescriptor;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// based on TestDAGLocationHint
public class TestDAGPlan {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder(); //TODO: doesn't seem to be deleting this folder automatically as expected.

  @Test(timeout = 5000)
  public void testBasicJobPlanSerde() throws IOException {

    DAGPlan job = DAGPlan.newBuilder()
       .setName("test")
       .addVertex(
           VertexPlan.newBuilder()
             .setName("vertex1")
             .setType(PlanVertexType.NORMAL)
             .addTaskLocationHint(PlanTaskLocationHint.newBuilder().addHost("machineName").addRack("rack1").build())
             .setTaskConfig(
                 PlanTaskConfiguration.newBuilder()
                   .setNumTasks(2)
                   .setVirtualCores(4)
                   .setMemoryMb(1024)
                   .setJavaOpts("")
                   .setTaskModule("x.y")
                   .build())
             .build())
        .build();
   File file = tempFolder.newFile("jobPlan");
   FileOutputStream outStream = null;
   try {
     outStream = new FileOutputStream(file);
     job.writeTo(outStream);
   }
   finally {
     if(outStream != null){
       outStream.close();
     }
   }

   DAGPlan inJob;
   FileInputStream inputStream;
   try {
     inputStream = new FileInputStream(file);
     inJob = DAGPlan.newBuilder().mergeFrom(inputStream).build();
   }
   finally {
     outStream.close();
   }

   Assert.assertEquals(job, inJob);
  }

  @Test(timeout = 5000)
  public void testEdgeManagerSerde() {
    DAG dag = DAG.create("testDag");
    Vertex v1 = createStubbedVertex(1, 10);
    Vertex v2 = createStubbedVertex(2);

    InputDescriptor inputDescriptor = InputDescriptor.create("input")
        .setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    OutputDescriptor outputDescriptor = OutputDescriptor.create("output")
        .setUserPayload(UserPayload.create(ByteBuffer.wrap("outputBytes".getBytes())));
    Edge edge = Edge.create(v1, v2, EdgeProperty.create(
        EdgeManagerPluginDescriptor.create("emClass").setUserPayload(
            UserPayload.create(ByteBuffer.wrap("emPayload".getBytes()))),
        DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge);

    DAGPlan dagProto = dag.createDag(new TezConfiguration(), null, null, null, true);

    EdgeProperty edgeProperty = DagTypeConverters.createEdgePropertyMapFromDAGPlan(dagProto
        .getEdgeList().get(0));

    EdgeManagerPluginDescriptor emDesc = edgeProperty.getEdgeManagerDescriptor();
    Assert.assertNotNull(emDesc);
    Assert.assertEquals("emClass", emDesc.getClassName());
    Assert.assertTrue(
        Arrays.equals("emPayload".getBytes(), emDesc.getUserPayload().deepCopyAsArray()));
  }

  @Test(timeout = 5000)
  public void testUserPayloadSerde() {
    DAG dag = DAG.create("testDag");
    Vertex v1 = createStubbedVertex(1, 10);
    Vertex v2 = createStubbedVertex(2);

    InputDescriptor inputDescriptor = InputDescriptor.create("input").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    OutputDescriptor outputDescriptor = OutputDescriptor.create("output").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("outputBytes".getBytes())));
    Edge edge = Edge.create(v1, v2, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge);

    DAGPlan dagProto = dag.createDag(new TezConfiguration(), null, null, null, true);

    assertEquals(2, dagProto.getVertexCount());
    assertEquals(1, dagProto.getEdgeCount());

    VertexPlan v1Proto = dagProto.getVertex(0);
    VertexPlan v2Proto = dagProto.getVertex(1);
    EdgePlan edgeProto = dagProto.getEdge(0);

    assertEquals("processor1Bytes", new String(v1Proto.getProcessorDescriptor().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("processor1", v1Proto.getProcessorDescriptor().getClassName());

    assertEquals("processor2Bytes", new String(v2Proto.getProcessorDescriptor().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("processor2", v2Proto.getProcessorDescriptor().getClassName());

    assertEquals("inputBytes", new String(edgeProto.getEdgeDestination().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("input", edgeProto.getEdgeDestination().getClassName());

    assertEquals("outputBytes", new String(edgeProto.getEdgeSource().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("output", edgeProto.getEdgeSource().getClassName());

    EdgeProperty edgeProperty = DagTypeConverters
        .createEdgePropertyMapFromDAGPlan(dagProto.getEdgeList().get(0));

    byte[] ib = edgeProperty.getEdgeDestination().getUserPayload().deepCopyAsArray();
    assertEquals("inputBytes", new String(ib));
    assertEquals("input", edgeProperty.getEdgeDestination().getClassName());

    byte[] ob = edgeProperty.getEdgeSource().getUserPayload().deepCopyAsArray();
    assertEquals("outputBytes", new String(ob));
    assertEquals("output", edgeProperty.getEdgeSource().getClassName());
  }

  @Test(timeout = 5000)
  public void userVertexOrderingIsMaintained() {
    DAG dag = DAG.create("testDag");
    Vertex v1 = createStubbedVertex(1, 10);
    Vertex v2 = createStubbedVertex(2);
    Vertex v3 = createStubbedVertex(3);

    InputDescriptor inputDescriptor = InputDescriptor.create("input").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    OutputDescriptor outputDescriptor = OutputDescriptor.create("output").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("outputBytes".getBytes())));
    Edge edge = Edge.create(v1, v2, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge).addVertex(v3);

    DAGPlan dagProto = dag.createDag(new TezConfiguration(), null, null, null, true);

    assertEquals(3, dagProto.getVertexCount());
    assertEquals(1, dagProto.getEdgeCount());

    VertexPlan v1Proto = dagProto.getVertex(0);
    VertexPlan v2Proto = dagProto.getVertex(1);
    VertexPlan v3Proto = dagProto.getVertex(2);
    EdgePlan edgeProto = dagProto.getEdge(0);

    // either v1 or v2 will be on top based on topological order 
    String v1ProtoPayload = new String(v1Proto.getProcessorDescriptor().getTezUserPayload().getUserPayload().toByteArray());
    String v2ProtoPayload = new String(v2Proto.getProcessorDescriptor().getTezUserPayload().getUserPayload().toByteArray());
    assertTrue(v1ProtoPayload.equals("processor1Bytes") || v1ProtoPayload.equals("processor3Bytes"));
    assertTrue(v2ProtoPayload.equals("processor1Bytes") || v2ProtoPayload.equals("processor3Bytes"));
    assertTrue(v1Proto.getProcessorDescriptor().getClassName().equals("processor1") ||
        v1Proto.getProcessorDescriptor().getClassName().equals("processor3"));
    assertTrue(v2Proto.getProcessorDescriptor().getClassName().equals("processor1") ||
        v2Proto.getProcessorDescriptor().getClassName().equals("processor3"));

    assertEquals("processor2Bytes", new String(v3Proto.getProcessorDescriptor().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("processor2", v3Proto.getProcessorDescriptor().getClassName());

    assertEquals("inputBytes", new String(edgeProto.getEdgeDestination().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("input", edgeProto.getEdgeDestination().getClassName());

    assertEquals("outputBytes", new String(edgeProto.getEdgeSource().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("output", edgeProto.getEdgeSource().getClassName());

    EdgeProperty edgeProperty = DagTypeConverters
        .createEdgePropertyMapFromDAGPlan(dagProto.getEdgeList().get(0));

    byte[] ib = edgeProperty.getEdgeDestination().getUserPayload().deepCopyAsArray();
    assertEquals("inputBytes", new String(ib));
    assertEquals("input", edgeProperty.getEdgeDestination().getClassName());

    byte[] ob = edgeProperty.getEdgeSource().getUserPayload().deepCopyAsArray();
    assertEquals("outputBytes", new String(ob));
    assertEquals("output", edgeProperty.getEdgeSource().getClassName());
  }

  @Test
  public void testCriticalPathOrdering() {
    DAG dag = DAG.create("testDag");
    Vertex v1 = createStubbedVertex(1, 10);
    Vertex v2 = createStubbedVertex(2, 10);
    Vertex v3 = createStubbedVertex(3);
    Vertex v4 = createStubbedVertex(4, 10);
    Vertex v5 = createStubbedVertex(5);

    Vertex v6 = createStubbedVertex(6, 10);
    Vertex v7 = createStubbedVertex(7);

    InputDescriptor inputDescriptor = InputDescriptor.create("input").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    OutputDescriptor outputDescriptor = OutputDescriptor.create("output").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("outputBytes".getBytes())));
    EdgeProperty edgeProp = EdgeProperty.create(DataMovementType.SCATTER_GATHER,
        DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
        outputDescriptor, inputDescriptor);
    Edge v1v3 = Edge.create(v1, v3, edgeProp);
    Edge v2v5 = Edge.create(v2, v5, edgeProp);
    Edge v3v4 = Edge.create(v3, v4, edgeProp);
    Edge v4v5 = Edge.create(v4, v5, edgeProp);
    Edge v6v7 = Edge.create(v6, v7, edgeProp);

    dag.addVertex(v1).addVertex(v2).addVertex(v3).addVertex(v4).addVertex(v5)
        .addEdge(v1v3).addEdge(v2v5).addEdge(v3v4).addEdge(v4v5);
    dag.addVertex(v6).addVertex(v7).addEdge(v6v7);

    DAGPlan dagProto = dag.createDag(new TezConfiguration(), null, null, null, true);

    assertEquals(7, dagProto.getVertexCount());
    assertEquals(5, dagProto.getEdgeCount());

    // v1 or v6 should be on top because they have children with maxdepth of 1
    VertexPlan vproto = dagProto.getVertex(0);
    assertTrue(vproto.getProcessorDescriptor().getClassName().equals("processor1") ||
        vproto.getProcessorDescriptor().getClassName().equals("processor6"));
    String payload = new String(vproto.getProcessorDescriptor().getTezUserPayload().getUserPayload().toByteArray());
    assertTrue(payload.equals("processor1Bytes") || payload.equals("processor6Bytes"));
    vproto = dagProto.getVertex(1);
    assertTrue(vproto.getProcessorDescriptor().getClassName().equals("processor1") ||
        vproto.getProcessorDescriptor().getClassName().equals("processor6"));
    payload = new String(vproto.getProcessorDescriptor().getTezUserPayload().getUserPayload().toByteArray());
    assertTrue(payload.equals("processor1Bytes") || payload.equals("processor6Bytes"));

    // v3 and v7 will be the next vertices since they have children with maxdepth of 2
    vproto = dagProto.getVertex(2);
    assertTrue(vproto.getProcessorDescriptor().getClassName().equals("processor3") ||
        vproto.getProcessorDescriptor().getClassName().equals("processor7"));
    payload = new String(vproto.getProcessorDescriptor().getTezUserPayload().getUserPayload().toByteArray());
    assertTrue(payload.equals("processor3Bytes") || payload.equals("processor7Bytes"));
    vproto = dagProto.getVertex(3);
    assertTrue(vproto.getProcessorDescriptor().getClassName().equals("processor3") ||
        vproto.getProcessorDescriptor().getClassName().equals("processor7"));
    payload = new String(vproto.getProcessorDescriptor().getTezUserPayload().getUserPayload().toByteArray());
    assertTrue(payload.equals("processor3Bytes") || payload.equals("processor7Bytes"));

    // v2 and v4 both have children with maxdepth 3 but v2 should always be next since it has no inputs
    vproto = dagProto.getVertex(4);
    assertEquals("processor2", vproto.getProcessorDescriptor().getClassName());
    payload = new String(vproto.getProcessorDescriptor().getTezUserPayload().getUserPayload().toByteArray());
    assertEquals("processor2Bytes", payload);

    // v4 is next
    vproto = dagProto.getVertex(5);
    assertEquals("processor4", vproto.getProcessorDescriptor().getClassName());
    payload = new String(vproto.getProcessorDescriptor().getTezUserPayload().getUserPayload().toByteArray());
    assertEquals("processor4Bytes", payload);

    // and finally v5
    vproto = dagProto.getVertex(6);
    assertEquals("processor5", vproto.getProcessorDescriptor().getClassName());
    payload = new String(vproto.getProcessorDescriptor().getTezUserPayload().getUserPayload().toByteArray());
    assertEquals("processor5Bytes", payload);
  }

  @Test (timeout=5000)
  public void testCredentialsSerde() {
    DAG dag = DAG.create("testDag");
    Vertex v1 = createStubbedVertex(1, 10);
    Vertex v2 = createStubbedVertex(2);

    InputDescriptor inputDescriptor = InputDescriptor.create("input").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    OutputDescriptor outputDescriptor = OutputDescriptor.create("output").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("outputBytes".getBytes())));
    Edge edge = Edge.create(v1, v2, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge);

    Credentials dagCredentials = new Credentials();
    Token<TokenIdentifier> token1 = new Token<TokenIdentifier>();
    Token<TokenIdentifier> token2 = new Token<TokenIdentifier>();
    dagCredentials.addToken(new Text("Token1"), token1);
    dagCredentials.addToken(new Text("Token2"), token2);
    
    dag.setCredentials(dagCredentials);

    DAGPlan dagProto = dag.createDag(new TezConfiguration(), null, null, null, true);

    assertTrue(dagProto.hasCredentialsBinary());
    
    Credentials fetchedCredentials = DagTypeConverters.convertByteStringToCredentials(dagProto
        .getCredentialsBinary());
    
    assertEquals(2, fetchedCredentials.numberOfTokens());
    assertNotNull(fetchedCredentials.getToken(new Text("Token1")));
    assertNotNull(fetchedCredentials.getToken(new Text("Token2")));
  }

  @Test(timeout = 5000)
  public void testInvalidExecContext_1() {
    DAG dag = DAG.create("dag1");
    dag.setExecutionContext(VertexExecutionContext.createExecuteInAm(true));
    Vertex v1 = Vertex.create("testvertex", ProcessorDescriptor.create("processor1"), 1);
    dag.addVertex(v1);

    try {
      dag.createDag(new TezConfiguration(false), null, null, null, true);
      fail("Expecting dag create to fail due to invalid ServicePluginDescriptor");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("AM execution"));
    }

    dag.setExecutionContext(VertexExecutionContext.createExecuteInContainers(true));

    try {
      dag.createDag(new TezConfiguration(false), null, null, null, true);
      fail("Expecting dag create to fail due to invalid ServicePluginDescriptor");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("container execution"));
    }

  }

  @Test(timeout = 5000)
  public void testInvalidExecContext_2() {

    ServicePluginsDescriptor servicePluginsDescriptor = ServicePluginsDescriptor
        .create(false,
            new TaskSchedulerDescriptor[]{TaskSchedulerDescriptor.create("plugin", null)},
            new ContainerLauncherDescriptor[]{ContainerLauncherDescriptor.create("plugin", null)},
            new TaskCommunicatorDescriptor[]{TaskCommunicatorDescriptor.create("plugin", null)});

    VertexExecutionContext validExecContext = VertexExecutionContext.create("plugin", "plugin",
        "plugin");
    VertexExecutionContext invalidExecContext1 =
        VertexExecutionContext.create("invalidplugin", "plugin", "plugin");
    VertexExecutionContext invalidExecContext2 =
        VertexExecutionContext.create("plugin", "invalidplugin", "plugin");
    VertexExecutionContext invalidExecContext3 =
        VertexExecutionContext.create("plugin", "plugin", "invalidplugin");


    DAG dag = DAG.create("dag1");
    dag.setExecutionContext(VertexExecutionContext.createExecuteInContainers(true));
    Vertex v1 = Vertex.create("testvertex", ProcessorDescriptor.create("processor1"), 1);
    dag.addVertex(v1);

    // Should succeed. Default context is containers.
    dag.createDag(new TezConfiguration(false), null, null, null, true,
        servicePluginsDescriptor, null);


    // Set execute in AM should fail
    v1.setExecutionContext(VertexExecutionContext.createExecuteInAm(true));
    try {
      dag.createDag(new TezConfiguration(false), null, null, null, true,
          servicePluginsDescriptor, null);
      fail("Expecting dag create to fail due to invalid ServicePluginDescriptor");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("AM execution"));
    }

    // Valid context
    v1.setExecutionContext(validExecContext);
    dag.createDag(new TezConfiguration(false), null, null, null, true,
        servicePluginsDescriptor, null);

    // Invalid task scheduler
    v1.setExecutionContext(invalidExecContext1);
    try {
      dag.createDag(new TezConfiguration(false), null, null, null, true,
          servicePluginsDescriptor, null);
      fail("Expecting dag create to fail due to invalid ServicePluginDescriptor");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("testvertex"));
      assertTrue(e.getMessage().contains("task scheduler"));
      assertTrue(e.getMessage().contains("invalidplugin"));
    }

    // Invalid ContainerLauncher
    v1.setExecutionContext(invalidExecContext2);
    try {
      dag.createDag(new TezConfiguration(false), null, null, null, true,
          servicePluginsDescriptor, null);
      fail("Expecting dag create to fail due to invalid ServicePluginDescriptor");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("testvertex"));
      assertTrue(e.getMessage().contains("container launcher"));
      assertTrue(e.getMessage().contains("invalidplugin"));
    }

    // Invalid task comm
    v1.setExecutionContext(invalidExecContext3);
    try {
      dag.createDag(new TezConfiguration(false), null, null, null, true,
          servicePluginsDescriptor, null);
      fail("Expecting dag create to fail due to invalid ServicePluginDescriptor");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("testvertex"));
      assertTrue(e.getMessage().contains("task communicator"));
      assertTrue(e.getMessage().contains("invalidplugin"));
    }

  }

  @Test(timeout = 5000)
  public void testServiceDescriptorPropagation() {
    DAG dag = DAG.create("testDag");

    VertexExecutionContext defaultExecutionContext =
        VertexExecutionContext.create("plugin", "plugin", "plugin");
    VertexExecutionContext v1Context = VertexExecutionContext.createExecuteInAm(true);

    ServicePluginsDescriptor servicePluginsDescriptor = ServicePluginsDescriptor
        .create(true, new TaskSchedulerDescriptor[]{TaskSchedulerDescriptor.create("plugin", null)},
            new ContainerLauncherDescriptor[]{ContainerLauncherDescriptor.create("plugin", null)},
            new TaskCommunicatorDescriptor[]{TaskCommunicatorDescriptor.create("plugin", null)});

    Vertex v1 = createStubbedVertex(1, 10).setExecutionContext(v1Context);
    Vertex v2 = createStubbedVertex(2);

    InputDescriptor inputDescriptor = InputDescriptor.create("input").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    OutputDescriptor outputDescriptor = OutputDescriptor.create("output").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("outputBytes".getBytes())));
    Edge edge = Edge.create(v1, v2, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge);
    dag.setExecutionContext(defaultExecutionContext);

    DAGPlan dagProto = dag.createDag(new TezConfiguration(), null, null, null, true,
        servicePluginsDescriptor, null);

    assertEquals(2, dagProto.getVertexCount());
    assertEquals(1, dagProto.getEdgeCount());

    assertTrue(dagProto.hasDefaultExecutionContext());
    VertexExecutionContextProto defaultContextProto = dagProto.getDefaultExecutionContext();
    assertFalse(defaultContextProto.getExecuteInContainers());
    assertFalse(defaultContextProto.getExecuteInAm());
    assertEquals("plugin", defaultContextProto.getTaskSchedulerName());
    assertEquals("plugin", defaultContextProto.getContainerLauncherName());
    assertEquals("plugin", defaultContextProto.getTaskCommName());

    VertexPlan v1Proto = dagProto.getVertex(0);
    assertTrue(v1Proto.hasExecutionContext());
    VertexExecutionContextProto v1ContextProto = v1Proto.getExecutionContext();
    assertFalse(v1ContextProto.getExecuteInContainers());
    assertTrue(v1ContextProto.getExecuteInAm());
    assertFalse(v1ContextProto.hasTaskSchedulerName());
    assertFalse(v1ContextProto.hasContainerLauncherName());
    assertFalse(v1ContextProto.hasTaskCommName());

    VertexPlan v2Proto = dagProto.getVertex(1);
    assertFalse(v2Proto.hasExecutionContext());
  }

  @Test(timeout = 5000)
  public void testInvalidJavaOpts() {
    DAG dag = DAG.create("testDag");
    Vertex v1 = createStubbedVertex(1, 10);
    v1.setTaskLaunchCmdOpts(" -XX:+UseG1GC ");

    dag.addVertex(v1);

    TezConfiguration conf = new TezConfiguration(false);
    conf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, "  -XX:+UseParallelGC ");
    try {
      dag.createDag(conf, null, null, null, true, null, new JavaOptsChecker());
      fail("Expected dag creation to fail for invalid java opts");
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid/conflicting GC options"));
    }

    // Should not fail as java opts valid
    conf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, "  -XX:-UseParallelGC ");
    dag.createDag(conf, null, null, null, true, null, new JavaOptsChecker());

    // Should not fail as no checker enabled
    conf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, "  -XX:+UseParallelGC ");
    dag.createDag(conf, null, null, null, true, null, null);

  }

  private Vertex createStubbedVertex(int vertexNum) {
    return createStubbedVertex(vertexNum, 1);
  }

  private Vertex createStubbedVertex(int vertexNum, int parallelism) {
    String processorName = "processor" + vertexNum;
    ProcessorDescriptor pd = ProcessorDescriptor.create(processorName)
        .setUserPayload(UserPayload.create(ByteBuffer.wrap((processorName + "Bytes").getBytes())));
    Vertex v = Vertex.create("v" + vertexNum, pd, parallelism, Resource.newInstance(1024, 1));
    v.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .addTaskLocalFiles(new HashMap<String, LocalResource>());
    return v;
  }
}
