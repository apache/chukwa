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
import java.util.ArrayList;

/**
 * Tests the functionality JMSMessagePropertyTransformer.
 */
public class TestJMSMessagePropertyTransformer extends TestCase implements ChunkReceiver {
  String DATA_TYPE = "Test";
  String MESSAGE_PAYLOAD = "Some JMS message payload";

  TopicConnection connection = null;
  TopicSession session = null;
  TopicPublisher publisher = null;
  ArrayList<String> chunkPayloads;
  int bytesReceived = 0;

  protected void setUp() throws Exception {
    connection =  ActiveMQConnection.makeConnection("vm://localhost");
    session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic("test.topic");
    publisher = session.createPublisher(topic);
    chunkPayloads = new ArrayList<String>();
    bytesReceived = 0;
  }

  protected void tearDown() throws Exception {
    session.close();
    connection.close();
  }

  public void testJMSMessageProperties() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE, "vm://localhost -t test.topic " +
                    "-x org.apache.hadoop.chukwa.datacollection.adaptor.jms.JMSMessagePropertyTransformer " +
                    "-p \"foo,bar,num\"",
            AdaptorManager.NULL);
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    message.setStringProperty("bar", "bar_value");
    message.setStringProperty("bat", "bat_value");
    message.setStringProperty("foo", "foo_value");
    message.setIntProperty("num", 1);
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }
    assertEquals("Message not received", 1, chunkPayloads.size());
    assertEquals("Incorrect chunk payload found",
            "foo_value\tbar_value\t1", chunkPayloads.get(0));
  }

  public void testJMSMessagePropertiesNoQuotes() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE, "vm://localhost -t test.topic " +
                    "-x org.apache.hadoop.chukwa.datacollection.adaptor.jms.JMSMessagePropertyTransformer " +
                    "-p foo,bar,num",
            AdaptorManager.NULL);
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    message.setStringProperty("bar", "bar_value");
    message.setStringProperty("bat", "bat_value");
    message.setStringProperty("foo", "foo_value");
    message.setIntProperty("num", 1);
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }
    assertEquals("Message not received", 1, chunkPayloads.size());
    assertEquals("Incorrect chunk payload found",
            "foo_value\tbar_value\t1", chunkPayloads.get(0));
  }

  public void testJMSMessagePropertiesWithDelimiter() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE, "vm://localhost -t test.topic " +
                    "-x org.apache.hadoop.chukwa.datacollection.adaptor.jms.JMSMessagePropertyTransformer " +
                    "-p \"foo,bar,num -d ' '\"",
            AdaptorManager.NULL);
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    message.setStringProperty("bar", "bar_value");
    message.setStringProperty("bat", "bat_value");
    message.setStringProperty("foo", "foo_value");
    message.setIntProperty("num", 1);
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }
    assertEquals("Message not received", 1, chunkPayloads.size());
    assertEquals("Incorrect chunk payload found", "foo_value bar_value 1", chunkPayloads.get(0));
  }

  public void testJMSMessagePropertiesWithNoQuotesDelimiter() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE, "vm://localhost -t test.topic " +
                    "-x org.apache.hadoop.chukwa.datacollection.adaptor.jms.JMSMessagePropertyTransformer " +
                    "-p \"foo,bar,num -d ^^^\"",
            AdaptorManager.NULL);
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    message.setStringProperty("bar", "bar_value");
    message.setStringProperty("bat", "bat_value");
    message.setStringProperty("foo", "foo_value");
    message.setIntProperty("num", 1);
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }
    assertEquals("Message not received", 1, chunkPayloads.size());
    assertEquals("Incorrect chunk payload found", "foo_value^^^bar_value^^^1", chunkPayloads.get(0));
  }

  public void testJMSMessagePropertiesWithMultiWordDelimiter() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE, "vm://localhost -t test.topic " +
                    "-x org.apache.hadoop.chukwa.datacollection.adaptor.jms.JMSMessagePropertyTransformer " +
                    "-p \"foo,bar,num -d '[ insert between values ]'\"",
            AdaptorManager.NULL);
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    message.setStringProperty("bar", "bar_value");
    message.setStringProperty("bat", "bat_value");
    message.setStringProperty("foo", "foo_value");
    message.setIntProperty("num", 1);
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }
    assertEquals("Message not received", 1, chunkPayloads.size());
    assertEquals("Incorrect chunk payload found",
            "foo_value[ insert between values ]bar_value[ insert between values ]1",
            chunkPayloads.get(0));
  }

  public void testJMSPropMissingWithAllRequired() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE, "vm://localhost -t test.topic " +
                    "-x org.apache.hadoop.chukwa.datacollection.adaptor.jms.JMSMessagePropertyTransformer " +
                    "-p \"foo,bar,num\"",
            AdaptorManager.NULL);
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    message.setStringProperty("bar", "bar_value");
    message.setStringProperty("bat", "bat_value");
    message.setIntProperty("num", 1);
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }
    assertEquals("Message should not have been received", 0, chunkPayloads.size());
  }

  public void testJMSPropMissingWithSomeRequired() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE, "vm://localhost -t test.topic " +
                    "-x org.apache.hadoop.chukwa.datacollection.adaptor.jms.JMSMessagePropertyTransformer " +
                    "-p \"foo,bar,num -r foo\"",
            AdaptorManager.NULL);
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    message.setStringProperty("bar", "bar_value");
    message.setStringProperty("bat", "bat_value");
    message.setIntProperty("num", 1);
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }
    assertEquals("Message should not have been received", 0, chunkPayloads.size());
  }

  public void testJMSPropMissingWithSomeRequired2() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE, "vm://localhost -t test.topic " +
                    "-x org.apache.hadoop.chukwa.datacollection.adaptor.jms.JMSMessagePropertyTransformer " +
                    "-p \"foo,bar,num -r foo\"",
            AdaptorManager.NULL);
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    message.setStringProperty("foo", "foo_value");
    message.setStringProperty("bat", "bat_value");
    message.setIntProperty("num", 1);
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }
    assertEquals("Message not received", 1, chunkPayloads.size());
    assertEquals("Incorrect chunk payload found", "foo_value\t\t1", chunkPayloads.get(0));
  }

  public void testJMSPropfoundWithSomeRequired() throws Exception {

    JMSAdaptor adaptor = new JMSAdaptor();
    adaptor.parseArgs(DATA_TYPE, "vm://localhost -t test.topic " +
                    "-x org.apache.hadoop.chukwa.datacollection.adaptor.jms.JMSMessagePropertyTransformer " +
                    "-p \"foo,bar,num -r foo\"",
            AdaptorManager.NULL);
    adaptor.start("id", DATA_TYPE, 0, this);

    Message message = session.createTextMessage(MESSAGE_PAYLOAD);
    message.setStringProperty("bar", "bar_value");
    message.setStringProperty("bat", "bat_value");
    message.setStringProperty("foo", "foo_value");
    message.setIntProperty("num", 1);
    publisher.publish(message);

    synchronized(this) {
      wait(1000);
    }
    assertEquals("Message not received", 1, chunkPayloads.size());
    assertEquals("Incorrect chunk payload found", "foo_value\tbar_value\t1", chunkPayloads.get(0));
  }



  public void add(Chunk c) {
    bytesReceived += c.getData().length;
    assertEquals("Unexpected data type", DATA_TYPE, c.getDataType());
    assertEquals("Chunk sequenceId should be total bytes received.",
            bytesReceived, c.getSeqID());
    chunkPayloads.add(new String(c.getData()));
  }
}