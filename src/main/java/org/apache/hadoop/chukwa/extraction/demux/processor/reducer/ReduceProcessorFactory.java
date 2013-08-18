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

package org.apache.hadoop.chukwa.extraction.demux.processor.reducer;


import java.util.HashMap;

import org.apache.log4j.Logger;

public class ReduceProcessorFactory {
  static Logger log = Logger.getLogger(ReduceProcessorFactory.class);

  // TODO
  // add new mapper package at the end.
  // We should have a more generic way to do this.
  // Ex: read from config
  // list of alias
  // and
  // alias -> processor class

  // ******** WARNING ********
  // If the ReduceProcessor is not there use Identity instead

  private static HashMap<String, ReduceProcessor> processors = new HashMap<String, ReduceProcessor>(); // registry

  private ReduceProcessorFactory() {
  }

  /**
   * Register a specific parser for a {@link ReduceProcessor} implementation.
   */
  public static synchronized void register(String reduceType,
                                           ReduceProcessor processor) {
    log.info("register " + processor.getClass().getName()
            + " for this recordType :" + reduceType);
    if (processors.containsKey(reduceType)) {
      throw new DuplicateReduceProcessorException(
              "Duplicate processor for recordType:" + reduceType);
    }
    ReduceProcessorFactory.processors.put(reduceType, processor);
  }

  public static ReduceProcessor getProcessor(String processorClass) throws UnknownReduceTypeException {
    if (processors.containsKey(processorClass)) {
      return processors.get(processorClass);
    } else {
      ReduceProcessor processor = null;
      try {
        processor = (ReduceProcessor) Class.forName(processorClass).newInstance();
      } catch (ClassNotFoundException e) {
        throw new UnknownReduceTypeException("Unknown reducer class for:" + processorClass, e);
      } catch (Exception e) {
        throw new UnknownReduceTypeException("error constructing processor", e);
      }
      // TODO using a ThreadSafe/reuse flag to actually decide if we want
      // to reuse the same processor again and again
      register(processorClass, processor);
      return processor;
    }

  }


}
