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
package org.apache.hadoop.chukwa.datacollection.adaptor.jms;

import javax.jms.Message;
import javax.jms.JMSException;

/**
 * Class that knows how to transform a JMS Message to a byte array. The byte
 * array will become the bytes bound to the Chukwa chunk.
 */
public interface JMSMessageTransformer {

  /**
   * Parse any transformer-specific args to initialize the transformer. Return
   * a null if the arguments could not be parsed. This method will always be
   * invoked before transform is called only if transformer arguments were
   * passed. If they weren't, this method will never be called.
   *
   * @param args Arguments needed to configur the transformer.
   * @return
   */
  public String parseArgs(String args);

  /**
   * Transform a Message to an array of bytes. Return null for a message that
   * should be ignored.
   *
   * @param message JMS message received by a JMS Adaptor.
   * @return the bytes that should be bound to the Chukwa chunk.
   * @throws JMSException
   */
  public byte[] transform(Message message) throws JMSException;
}
