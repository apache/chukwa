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
package org.apache.hadoop.chukwa.datacollection.writer;

import java.util.List;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.conf.Configuration;

/**
 * Minimal writer; does nothing with data.
 * 
 * Useful primarily as an end-of-pipeline stage, if stuff in the middle
 * is accomplishing something useful.
 *
 */
public class NullWriter implements ChukwaWriter {
  
  //in kb per sec
  int maxDataRate = Integer.MAX_VALUE;
  public static final String RATE_OPT_NAME = "nullWriter.dataRate";
  @Override
  public CommitStatus add(List<Chunk> chunks) throws WriterException {
    try {
      int dataBytes =0;
      for(Chunk c: chunks)
        dataBytes +=c.getData().length;
      if(maxDataRate > 0)
        Thread.sleep(dataBytes / maxDataRate);
    } catch(Exception e) {}
    return COMMIT_OK;
  }

  @Override
  public void close() throws WriterException {
    return;
  }

  @Override
  public void init(Configuration c) throws WriterException {
    maxDataRate = c.getInt(RATE_OPT_NAME, 0);
    return;
  }

}
