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

package org.apache.tez.dag.app.dag.impl;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("rawtypes")
public class FixedOrder extends DAGScheduler {

  private static final Logger LOG =
                            LoggerFactory.getLogger(FixedOrder.class);

  private final DAG dag;
  private final EventHandler handler;

  private final Map<String, Integer> vertexToPriority;

  public FixedOrder(DAG dag, EventHandler dispatcher) {
    this.dag = dag;
    this.handler = dispatcher;
    Set<String> vertexNames = dag.getVertices().values().stream().map(Vertex::getName).collect(Collectors.toSet());

    vertexToPriority = new HashMap<>();
    String[] vertices =
        dag.getConf().getTrimmedStrings(TezConfiguration.TEZ_VERTEX_PRIORITIES,
            TezConfiguration.TEZ_VERTEX_PRIORITIES_DEFAULT);
    int i = 1;
    for (String vertexName: vertices) {
      i++;
      if (!vertexNames.contains(vertexName)) {
        throw  new RuntimeException("Vertex " + vertexName + " is "
            + "not a vertex from this graph, valid vertex names are " + vertexNames);
      }
      vertexNames.remove(vertexName);
      vertexToPriority.put(vertexName, i);
    }
    if (!vertexNames.isEmpty()) {
      throw new RuntimeException("Priorities for vertices "
          + vertexNames + "haven't been set");
    }
  }
  
  @Override
  public void scheduleTaskEx(DAGEventSchedulerUpdate event) {
    TaskAttempt attempt = event.getAttempt();
    Vertex vertex = dag.getVertex(attempt.getVertexID());
    int vertexDistanceFromRoot = vertex.getDistanceFromRoot();

    String vertexName = dag.getVertex(attempt.getVertexID()).getName();

    // natural priority. Handles failures and retries.
    int priorityLowLimit = getPriorityLowLimit(dag, vertex);
    int priorityHighLimit = priorityLowLimit - 2;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Scheduling " + attempt.getID() + ", with vertex name " + vertexName +", between priorityLow: " + priorityLowLimit
          + " and priorityHigh: " + priorityHighLimit);
    }

    TaskAttemptEventSchedule attemptEvent = new TaskAttemptEventSchedule(
        attempt.getID(), priorityLowLimit, priorityHighLimit);
                                      
    sendEvent(attemptEvent);
  }

  @Override
  public int getPriorityLowLimit(final DAG dag, final Vertex vertex) {
    return vertexToPriority.get(vertex.getName()) + 1;
  }
  
  @Override
  public void taskCompletedEx(DAGEventSchedulerUpdate event) {
  }
  
  @SuppressWarnings("unchecked")
  void sendEvent(TaskAttemptEventSchedule event) {
    handler.handle(event);
  }

}
