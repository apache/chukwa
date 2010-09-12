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

import org.apache.hadoop.chukwa.datacollection.adaptor.AbstractAdaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorException;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorShutdownPolicy;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.log4j.Logger;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.Connection;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.JMSException;
import javax.jms.ConnectionFactory;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Queue;
import javax.jms.MessageConsumer;

/**
 * Adaptor that is able to listen to a JMS topic or queue for messages, receive
 * the message, and transform it to a Chukwa chunk. Transformation is handled by
 * a JMSMessageTransformer. The default JMSMessageTransformer used is the
 * JMSTextMessageTransformer.
 * <P>
 * This adaptor is added to an Agent like so:
 * <code>
 * add JMSAdaptor <dataType> <brokerURL> <-t <topicName>|-q <queueName>> [-s <JMSSelector>]
 *  [-x <transformerName>] [-p <transformerConfigs>] <offset>
 * </code>
 * <ul>
 * <li><code>dataType</code> - The chukwa data type.</li>
 * <li><code>brokerURL</code> - The JMS broker URL to bind to.</li>
 * <li><code>topicName</code> - The JMS topic to listen on.</li>
 * <li><code>queueName</code> - The JMS queue to listen on.</li>
 * <li><code>JMSSelector</code> - The JMS selector to filter with. Surround
 * with quotes if selector contains multiple words.</li>
 * <li><code>transformerName</code> - Class name of the JMSMessageTransformer to
 * use.</li>
 * <li><code>transformerConfigs</code> - Properties to be passed to the
 * JMSMessageTransformer to use.  Surround with quotes if configs contain
 * multiple words.</li>
 * </ul>
 *
 * @see JMSMessageTransformer
 * @see JMSTextMessageTransformer
 */
public class JMSAdaptor extends AbstractAdaptor {

  static Logger log = Logger.getLogger(JMSAdaptor.class);

  ConnectionFactory connectionFactory = null;
  Connection connection;
  String brokerURL;
  String topic;
  String queue;
  String selector = null;
  JMSMessageTransformer transformer;

  volatile long bytesReceived = 0;
  String status; // used to write checkpoint info. See getStatus() below
  String source; // added to the chunk to identify the stream

  class JMSListener implements MessageListener {

    public void onMessage(Message message) {
      if (log.isDebugEnabled()) {
        log.debug("got a JMS message");
      }
      
      try {

        byte[] bytes = transformer.transform(message);
        if (bytes == null) {
          return;
        }

        bytesReceived += bytes.length;

        if (log.isDebugEnabled()) {
          log.debug("Adding Chunk from JMS message: " + new String(bytes));
        }

        Chunk c = new ChunkImpl(type, source, bytesReceived, bytes, JMSAdaptor.this);
        dest.add(c);

      } catch (JMSException e) {
        log.error("can't read JMS messages in " + adaptorID, e);
      }
      catch (InterruptedException e) {
        log.error("can't add JMS messages in " + adaptorID, e);
      }
    }
  }

  /**
   * This adaptor received configuration like this:
   * <brokerURL> <-t <topicName>|-q <queueName>> [-s <JMSSelector>] [-x <transformerName>]
   * [-p <transformerProperties>]
   *
   * @param s
   * @return
   */
  @Override
  public String parseArgs(String s) {
    if (log.isDebugEnabled()) {
      log.debug("Parsing args to initialize adaptor: " + s);
    }

    String[] tokens = s.split(" ");
    if (tokens.length < 1) {
      throw new IllegalArgumentException("Configuration must include brokerURL.");
    }

    brokerURL = tokens[0];

    if (brokerURL.length() < 6 || brokerURL.indexOf("://") == -1) {
      throw new IllegalArgumentException("Invalid brokerURL: " + brokerURL);
    }

    String transformerName = null;
    String transformerConfs = null;
    for (int i = 1; i < tokens.length; i++) {
      String value = tokens[i];
      if ("-t".equals(value)) {
        topic = tokens[++i];
      }
      else if ("-q".equals(value)) {
        queue = tokens[++i];
      }
      else if ("-s".equals(value) && i <= tokens.length - 1) {
        selector = tokens[++i];

        // selector can have multiple words
        if (selector.startsWith("\"")) {
          for(int j = i + 1; j < tokens.length; j++) {
            selector = selector + " " + tokens[++i];
            if(tokens[j].endsWith("\"")) {
              break;
            }
          }
          selector = trimQuotes(selector);
        }
      }
      else if ("-x".equals(value)) {
        transformerName = tokens[++i];
      }
      else if ("-p".equals(value)) {
        transformerConfs = tokens[++i];

        // transformerConfs can have multiple words
        if (transformerConfs.startsWith("\"")) {
          for(int j = i + 1; j < tokens.length; j++) {
            transformerConfs = transformerConfs + " " + tokens[++i];
            if(tokens[j].endsWith("\"")) {
              break;
            }
          }
          transformerConfs = trimQuotes(transformerConfs);
        }
      }
    }

    if (topic == null && queue == null) {
      log.error("topicName or queueName must be set");
      return null;
    }

    if (topic != null && queue != null) {
      log.error("Either topicName or queueName must be set, but not both");
      return null;
    }

    // create transformer
    if (transformerName != null) {
      try {
        Class classDefinition = Class.forName(transformerName);
        Object object = classDefinition.newInstance();
        transformer = (JMSMessageTransformer)object;
      } catch (Exception e) {
        log.error("Couldn't find class for transformerName=" + transformerName, e);
        return null;
      }
    }
    else {
      transformer = new JMSTextMessageTransformer();
    }

    // configure transformer
    if (transformerConfs != null) {
      String result = transformer.parseArgs(transformerConfs);
      if (result == null) {
        log.error("JMSMessageTransformer couldn't parse transformer configs: " +
                transformerConfs);
        return null;
      }
    }

    status = s;

    if(topic != null) {
      source = "jms:"+brokerURL + ",topic:" + topic;
    }
    else if(queue != null) {
      source = "jms:"+brokerURL + ",queue:" + queue;
    }

    return s;
  }

  @Override
  public void start(long offset) throws AdaptorException {

    try {
      bytesReceived = offset;

      connectionFactory = initializeConnectionFactory(brokerURL);
      connection = connectionFactory.createConnection();

      log.info("Starting JMS adaptor: " + adaptorID + " started on brokerURL=" + brokerURL +
              ", topic=" + topic + ", selector=" + selector +
              ", offset =" + bytesReceived);

      // this is where different initialization could be used for a queue
      if(topic != null) {
        initializeTopic(connection, topic, selector, new JMSListener());
      }
      else if(queue != null) {
        initializeQueue(connection, queue, selector, new JMSListener());
      }
      connection.start();

    } catch(Exception e) {
      throw new AdaptorException(e);
    }
  }

  /**
   * Override this to initialize with a different connection factory.
   * @param brokerURL
   * @return
   */
  protected ConnectionFactory initializeConnectionFactory(String brokerURL) {
    return new ActiveMQConnectionFactory(brokerURL);
  }

  /**
   * Status is used to write checkpoints. Checkpoints are written as:
   * ADD <adaptorKey> = <adaptorClass> <currentStatus> <offset>
   *
   * Once they're reloaded, adaptors are re-initialized with
   * <adaptorClass> <currentStatus> <offset>
   *
   * While doing so, this gets passed by to the parseArgs method:
   * <currentStatus>
   *
   * Without the first token in <currentStatus>, which is expected to be <dataType>.
   *
   * @return
   */
  @Override
  public String getCurrentStatus() {
    return type + " " + status;
  }

  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    try {
      connection.close();

    } catch(Exception e) {
      log.error("Exception closing JMS connection.", e);
    }

    return bytesReceived;
  }

  private void initializeTopic(Connection connection,
                                      String topic,
                                      String selector,
                                      JMSListener listener) throws JMSException {
    TopicSession session = ((TopicConnection)connection).
            createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic jmsTopic = session.createTopic(topic);
    MessageConsumer consumer = session.createConsumer(jmsTopic, selector, true);
    consumer.setMessageListener(listener);
  }

  private void initializeQueue(Connection connection,
                                      String topic,
                                      String selector,
                                      JMSListener listener) throws JMSException {
    QueueSession session = ((QueueConnection)connection).
            createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    Queue queue = session.createQueue(topic);
    MessageConsumer consumer = session.createConsumer(queue, selector, true);
    consumer.setMessageListener(listener);
  }

  private static String trimQuotes(String value) {
    // trim leading and trailing quotes
    if (value.charAt(0) == '"') {
      value = value.substring(1);
    }
    if (value.charAt(value.length() - 1) == '"') {
      value = value.substring(0, value.length() - 1);
    }
    return value;
  }

}