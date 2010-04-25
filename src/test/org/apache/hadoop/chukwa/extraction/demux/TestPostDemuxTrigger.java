/*
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
package org.apache.hadoop.chukwa.extraction.demux;

import junit.framework.TestCase;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.CHUKWA_CONSTANT;
import org.apache.hadoop.chukwa.datatrigger.TriggerEvent;
import org.apache.hadoop.fs.Path;

import java.util.Iterator;

public class TestPostDemuxTrigger extends TestCase {

  static final Path[] SAMPLE_PATHS = new Path[] { new Path("/") };

  protected void setUp() throws Exception {
    MockTriggerAction.reset();
  }

  public void testSuccessTrigger() throws Exception {
    ChukwaConfiguration conf = new ChukwaConfiguration();
    conf.set(CHUKWA_CONSTANT.POST_DEMUX_SUCCESS_ACTION,
            "org.apache.hadoop.chukwa.extraction.demux.MockTriggerAction");

    PostProcessorManager postProcessManager = new PostProcessorManager(conf);
    assertTrue("processPostMoveTriggers returned false",
            postProcessManager.processPostMoveTriggers(SAMPLE_PATHS));

    assertEquals("Trigger never invoked", SAMPLE_PATHS.length,
            MockTriggerAction.getTriggerEvents().size());
    Iterator events = MockTriggerAction.getTriggerEvents().iterator();
    assertEquals("Incorrect Trigger event found", TriggerEvent.POST_DEMUX_SUCCESS,
            events.next());
  }

  public void testMultiSuccessTrigger() throws Exception {
    ChukwaConfiguration conf = new ChukwaConfiguration();
    conf.set(CHUKWA_CONSTANT.POST_DEMUX_SUCCESS_ACTION,
            "org.apache.hadoop.chukwa.extraction.demux.MockTriggerAction," +
            "org.apache.hadoop.chukwa.extraction.demux.MockTriggerAction");

    PostProcessorManager postProcessManager = new PostProcessorManager(conf);
    assertTrue("processPostMoveTriggers returned false",
            postProcessManager.processPostMoveTriggers(SAMPLE_PATHS));

    assertEquals("Trigger never invoked", 2 * SAMPLE_PATHS.length,
            MockTriggerAction.getTriggerEvents().size());
    Iterator events = MockTriggerAction.getTriggerEvents().iterator();
    assertEquals("Incorrect Trigger event found", TriggerEvent.POST_DEMUX_SUCCESS,
            events.next());
    assertEquals("Incorrect Trigger event found", TriggerEvent.POST_DEMUX_SUCCESS,
            events.next());
  }

}
