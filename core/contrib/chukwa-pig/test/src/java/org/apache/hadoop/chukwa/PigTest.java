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

package org.apache.hadoop.chukwa;

import static org.apache.pig.ExecType.LOCAL;
import static org.apache.pig.ExecType.MAPREDUCE;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.test.MiniCluster;
import org.junit.After;
import org.junit.Before;

public abstract class PigTest extends TestCase {

  protected final Log log = LogFactory.getLog(getClass());

  protected ExecType execType = LOCAL;

  protected MiniCluster cluster;
  protected PigServer pigServer;

  protected abstract ExecType getExecType();
  
  @Before
  @Override
  protected void setUp() throws Exception {

    if (getExecType() == MAPREDUCE) {
      log.info("MapReduce cluster");
      cluster = MiniCluster.buildCluster();
      pigServer = new PigServer(MAPREDUCE, cluster.getProperties());
    } else {
      log.info("Local cluster");
      pigServer = new PigServer(LOCAL);
    }
  }

  @After
  @Override
  protected void tearDown() throws Exception {
    pigServer.shutdown();
  }

}
