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
package org.apache.hadoop.chukwa.datacollection.sender;


/**
 * Encapsulates all of the communication overhead needed for chunks to be delivered
 * to a collector.
 */
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.sender.ChukwaHttpSender.CommitListEntry;

public interface ChukwaSender {

  /**
   * 
   * @param chunksToSend a list of chunks to commit
   * @return the list of committed chunks
   * @throws InterruptedException if interrupted while trying to send
   */
  public List<CommitListEntry> send(List<Chunk> chunksToSend)
      throws InterruptedException, java.io.IOException;

  public void setCollectors(Iterator<String> collectors);
  
  public void stop();

}
