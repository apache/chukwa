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
package org.apache.hadoop.chukwa.datacollection.writer.gora;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.PipelineableWriter;
import org.apache.hadoop.chukwa.datacollection.writer.WriterException;
import org.apache.hadoop.chukwa.datacollection.writer.solr.SolrWriter;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * This class leverages <a href="http://gora.apache.org">Apache Gora</a>
 * as a pipeline writer implementation for mapping Chukwa data chunks and 
 * metadata as {@link org.apache.hadoop.chukwa.datacollection.writer.gora.ChukwaChunk}'s. 
 *
 */
public class GoraWriter extends PipelineableWriter {
  
  private static Logger log = Logger.getLogger(SolrWriter.class);
  
  DataStore<String, ChukwaChunk> chunkStore;

  /**
   * Default constructor for this class.
   * @throws WriterException if error writing
   */
  public GoraWriter() throws WriterException {
    log.debug("Initializing configuration for GoraWriter pipeline...");
    init(ChukwaAgent.getStaticConfiguration());
  }

  /**
   * {@link org.apache.gora.store.DataStore} objects are created from a factory. It is necessary to 
   * provide the key and value class. The datastore class parameters is optional, 
   * and if not specified it will be read from the <code>gora.properties</code> file.
   * @throws WriterException if error occurs
   * @see org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter#init(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void init(Configuration c) throws WriterException {
    try {
      chunkStore = DataStoreFactory.getDataStore(String.class, ChukwaChunk.class, c);
    } catch (GoraException e) {
      log.error(ExceptionUtil.getStackTrace(e));
      e.printStackTrace();
    } 
  }

  /**
   * <p>
   * If the {@link org.apache.gora.store.DataStore} instance is not null, we
   * execute a {@link org.apache.gora.store.DataStore#flush()}. This forces 
   * the write caches to be flushed. DataStore implementations may optimize 
   * their writing by deferring the actual put / delete operations until 
   * this moment.
   * </p>
   * <p>Otherwise, we utilize {@link org.apache.gora.store.DataStore#close()}
   * which closes the DataStore. This should release any resources held by 
   * the implementation, so that the instance is ready for GC. All other 
   * DataStore methods cannot be used after this method was called. 
   * Subsequent calls of this method are ignored.
   * </p>  
   * @see org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter#close()
   */
  @Override
  public void close() throws WriterException {
    if (chunkStore != null) {
      chunkStore.flush();
    } else {
      chunkStore.close();
    }
    log.debug("Gora datastore successfully closed.");
  }

  @Override
  public CommitStatus add(List<Chunk> chunks) throws WriterException {
    CommitStatus cStatus = ChukwaWriter.COMMIT_OK;
    for(Chunk chunk : chunks) {
      try {
        ChukwaChunk chukwaChunk = ChukwaChunk.newBuilder().build();
        chukwaChunk.setSource(chunk.getSource());
        chukwaChunk.setDatatype(chunk.getDataType());
        chukwaChunk.setSequenceID(chunk.getSeqID());
        chukwaChunk.setName(chunk.getStreamName());
        chukwaChunk.setTags(chunk.getTags());
        chukwaChunk.setData(ByteBuffer.wrap(chunk.getData()));
      } catch (Exception e) {
        log.error(ExceptionUtil.getStackTrace(e));
        throw new WriterException("Failed to store data to Solr Cloud.");
      }
    }
    if (next != null) {
      cStatus = next.add(chunks); //pass data through
    }
    return cStatus;
  }
}
