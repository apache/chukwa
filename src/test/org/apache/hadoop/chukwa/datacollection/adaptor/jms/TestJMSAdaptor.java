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

import junit.framework.TestCase;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;
import org.apache.hadoop.chukwa.Chunk;

import org.apache.activemq.ActiveMQConnection;

import javax.jms.Message;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

/**
 * Tests the functionality of JMSAdapter and JMSTextMessageTransformer
 */
public class TestJMSAdaptor extends TestCase implements ChunkReceiver {
  String DATA_TYPE = "Test";
  String MESSAGE_PAYLOAD = "Some JMS message payload";

  TopicConnection connection = null;
  TopicSession session = null;
  TopicPublisher publisher = null;
  int bytesReceived = 0;
  int messagesReceived = 0;

  protected void setUp() throws Exception {
    connection =  ActiveMQConnection.makeConnection("vm://localhost");
    session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic("test.topic");
    publisher = session.createPublisher(topic);
    messagesReceived = 0;
    bytesReceived = 0;
  }

  protected void tearDown() throws Exception {
    session.close();
    connection.close();
  }

  public void testJMSTextMessage() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE, "vm://localhost -t test.topic",
            AdaptorManager.NULL);
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }
    assertEquals("Message not received", 1, messagesReceived);
  }

  public void testJMSTextMessageWithTransformer() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE, "vm://localhost -t test.topic -x org.apache.hadoop.chukwa.datacollection.adaptor.jms.JMSTextMessageTransformer",
            AdaptorManager.NULL);
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }
    assertEquals("Message not received", 1, messagesReceived);
  }

  public void testJMSTextMessageWithSelector() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE,
            "vm://localhost -t test.topic -s \"foo='bar'\"",
            AdaptorManager.NULL);
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    publisher.publish(message);
    
    message = session.createTextMessage(MESSAGE_PAYLOAD);
    message.setStringProperty("foo", "bar");
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }

    assertEquals("Message not received", 1, messagesReceived);
  }

  public void testJMSTextMessageWithMultiWordSelector() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE,
            "vm://localhost -t test.topic -s \"foo='bar' and bar='foo'\"",
            AdaptorManager.NULL);    
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    publisher.publish(message);

    message = session.createTextMessage(MESSAGE_PAYLOAD);
    message.setStringProperty("foo", "bar");
    publisher.publish(message);

    message = session.createTextMessage(MESSAGE_PAYLOAD);
    message.setStringProperty("foo", "bar");
    message.setStringProperty("bar", "foo");
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }

    assertEquals("Message not received", 1, messagesReceived);
  }

  public void add(Chunk c) {
    bytesReceived += c.getData().length;
    assertEquals("Unexpected data length",
            MESSAGE_PAYLOAD.length(), c.getData().length);
    assertEquals("Unexpected data type", DATA_TYPE, c.getDataType());
    assertEquals("Chunk sequenceId should be total bytes received.",
            bytesReceived, c.getSeqID());
    assertEquals("Unexpected message payload",
            MESSAGE_PAYLOAD, new String(c.getData()));
    messagesReceived++;
  }
}