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

package org.apache.hadoop.chukwa.datacollection.adaptor;


import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;

/**
 * An adaptor is a component that runs within the Local Agent, producing chunks
 * of monitoring data.
 * 
 * An adaptor can, but need not, have an associated thread. If an adaptor lacks
 * a thread, it needs to arrange some mechanism to periodically get control and
 * send reports such as a callback somewhere.
 * 
 * Adaptors must be able to stop and resume without losing data, using a byte
 * offset in the stream.
 * 
 * If an adaptor crashes at byte offset n, and is restarted at byte offset k,
 * with k &lt; n, it is allowed to send different values for bytes k through n the
 * second time around. However, the stream must still be parseable, assuming
 * that bytes 0-k come from the first run,and bytes k - n come from the second.
 * 
 * Note that Adaptor implements neither equals() nor hashCode(). It is never
 * safe to compare two adaptors with equals(). It is safe to use adaptors
 * as hash table keys, though two distinct Adaptors will appear as two distinct
 * keys.  This is the desired behavior, since it means that messages intended
 * for one Adaptor will never be received by another, even across Adaptor
 *  restarts.
 */
public interface Adaptor {
  /**
   * Start this adaptor
   * @param adaptorID Adaptor ID
   * 
   * @param type the application type, who is starting this adaptor
   * @param offset the stream offset of the first byte sent by this adaptor
   * @param dest Chunk receiving destination
   * @throws AdaptorException if adaptor can not be started
   */
  public void start(String adaptorID, String type, long offset,
      ChunkReceiver dest) throws AdaptorException;

  /**
   * Return the adaptor's state Should not include class name or byte
   * offset, which are written by caller. The datatype should, however,
   * be written by this method.
   * 
   * @return the adaptor state as a string
   */
  public String getCurrentStatus();

  public String getType();

  /**
   * Parse args, return stream name.  Do not start running.
   * 
   * Return the stream name, given params.
   * The stream name is the part of the Adaptor status that's used to 
   * determine uniqueness. 
   * @param datatype Data type
   * @param params Adaptor parameters
   * @param c Adaptor Manager
   * 
   * @return Stream name as a string, null if params are malformed
   */
  public String parseArgs(String datatype, String params, AdaptorManager c);
  
  /**
   * Signals this adaptor to come to an orderly stop. The adaptor ought to push
   * out all the data it can before exiting depending of the shutdown policy
   * @param shutdownPolicy is defined as forcefully or gracefully
   * 
   * @return the logical offset at which the adaptor was when the method return
   * @throws AdaptorException Exception on shutdown
   */
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy) throws AdaptorException;

}
