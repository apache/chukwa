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

package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.Ps.InvalidPsRecord;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.Ps.PsOutput;

import junit.framework.TestCase;

public class TestPsOutput extends TestCase {

  public void testGetRecordList() throws IOException, InvalidPsRecord {
    // below is from command
    // "ps axo pid,user,vsize,size,pcpu,pmem,time,start_time,start,cmd"
    String output = "  PID USER        VSZ    SZ %CPU %MEM     TIME START  STARTED CMD\n"
        + "    1 root       2064   284  0.0  0.0 00:00:02  2008   Dec 29 init [5]\n"
        + "    2 root          0     0  0.0  0.0 00:00:01  2008   Dec 29 [migration/0]\n"
        + "20270 chzhang    4248   588  0.0  0.0 00:00:00 15:32 15:32:36 ps axo pid,user,vsize,size,pcpu,pmem,time,start_time,start,cmd\n"
        + "28371 angelac2   7100  1716  0.0  0.0 00:00:00 Feb27   Feb 27 /usr/libexec/gconfd-2 5\n";

    PsOutput pso = new PsOutput(output);
    ArrayList<HashMap<String, String>> processes = pso.getProcessList();
    assertEquals(4, processes.size());
    assertEquals("Dec29", processes.get(0).get("STARTED"));
    assertEquals("15:32:36", processes.get(2).get("STARTED"));
    assertEquals(
        "ps axo pid,user,vsize,size,pcpu,pmem,time,start_time,start,cmd",
        processes.get(2).get("CMD"));
  }

}
