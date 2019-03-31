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

import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.JMSException;

/**
 * Basic JMSMessageTransformer that uses the payload message of a JMS
 * TextMessage as the Chukwa record payload. If the message is not an instance
 * of TextMessage, or it is, but the payload is null or empty, returns null.
 */
public class JMSTextMessageTransformer implements JMSMessageTransformer {
  protected Log log = LogFactory.getLog(getClass());

  public String parseArgs(String s) {
    return s;      
  }

  public byte[] transform(Message message) throws JMSException {
    if (!(message instanceof TextMessage)) {
      log.warn("Invalid message type received: " + message);
      return null;
    }

    String text = ((TextMessage)message).getText();
    if (text != null && text.length() > 0) {
      return text.getBytes(Charset.forName("UTF-8"));
    }

    return null;
  }
}
