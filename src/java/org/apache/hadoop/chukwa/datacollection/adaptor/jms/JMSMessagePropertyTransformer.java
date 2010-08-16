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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Message;
import javax.jms.JMSException;
import java.util.ArrayList;

/**
 * JMSMessageTransformer that uses the properties of a JMS Message to build a
 * Chukwa record payload. The value for each property configured will be used
 * to create the record, with the delimiter value between each. The default
 * delimiter is a tab (i.e., '\t').
 * <P>
 * To configure this transformer, set the -p field of the adaptor to the
 * following (surrounded with double quotes):
 * <code>
 * <propertyNames> [-d <delimiter>] [-r <requiredPropertyNames>]
 * </code>
 * <ul>
 * <li><code>propertyNames</code> - Comma-separated list of JMS properties.</li>
 * <li><code>delimiter</code> - Delimiter to use, in single quotes.</li>
 * <li><code>requiredPropertyNames</code> - Comma-separated list of required
 * JMS properties. Default behavior is that all properties are required.</li>
 * </ul>
 *
 */
public class JMSMessagePropertyTransformer implements JMSMessageTransformer {
  protected Log log = LogFactory.getLog(getClass());

  private static final String DEFAULT_DELIMITER = "\t";

  ArrayList<String> propertyNames = null;
  ArrayList<String> requiredPropertyNames = null;
  String delimiter = DEFAULT_DELIMITER;

  public String parseArgs(String args) {
    if (args == null || args.length() == 0) {
      log.error("propertyNames must be set for this transformer");
      return null;
    }

    log.info("Initializing JMSMessagePropertyTransformer: args=" + args);

    propertyNames = new ArrayList<String>();
    
    String[] tokens = args.split(" ");
    for (String propertyName : tokens[0].split(",")) {
      propertyNames.add(propertyName);
    }

    for(int i = 1; i < tokens.length; i++) {
      String token = tokens[i];

      if ("-d".equals(token) && i <= tokens.length - 2) {
        String value = tokens[++i];

        // we lost all spaces with the split, so we have to put them back, yuck.
        while (i <= tokens.length - 2 && !tokens[i + 1].startsWith("-")) {
          value = value + " " + tokens[++i];
        }

        delimiter = trimSingleQuotes(value);
      }
      else if ("-r".equals(token) && i <= tokens.length - 2) {
        // requiredPropertyNames = null means all are required.
        requiredPropertyNames = new ArrayList<String>();

        String[] required = tokens[++i].split(",");
        for (String r : required) {
          requiredPropertyNames.add(r);
        }
      }
    }

    log.info("Initialized JMSMessagePropertyTransformer: delimiter='" +
            delimiter + "', propertyNames=" + propertyNames +
            ", requiredProperties=" +
            (requiredPropertyNames == null ? "ALL" : requiredPropertyNames));
    return args;
  }

  /**
   * Transforms message propertes into a byte array delimtied by delimiter. If
   * all of the configured message properties are not found, returns null.
   * <P>
   * The could be enhanced to support the concept of optional/required properties.
   * @param message
   * @return
   * @throws JMSException
   */
  public byte[] transform(Message message) throws JMSException {

    if (propertyNames == null || propertyNames.size() == 0) {
      log.error("No message properties configured for this JMS transformer.");
      return null;
    }

    int valuesFound = 0;
    StringBuilder sb = new StringBuilder();
    for (String propertyName : propertyNames) {
      Object propertyValue = message.getObjectProperty(propertyName);
      String value = transformValue(propertyName, propertyValue);

      // is a required value not found?
      if (value == null) {
        if (requiredPropertyNames == null ||
            requiredPropertyNames.contains(propertyName)) {
          return null;
        }
      }

      if (valuesFound > 0) {
        sb.append(delimiter);
      }

      if (value != null) {
        sb.append(value);
      }

      valuesFound++;
    }

    if (sb.length() == 0 || valuesFound != propertyNames.size()) {
      return null;
    }

    return sb.toString().getBytes();
  }

  /**
   * Transforms the propertyValue found into the string that should be used for
   * the message. Can handle String values and Number values. Override this method
   * to handle other Java types, or to apply other value transformation logic.
   *
   * @param propertyName The name of the JMS property
   * @param propertyValue The value of the property, which might be null.
   * @return
   */
  protected String transformValue(String propertyName, Object propertyValue) {

    if (propertyValue == null) {
      return null;
    }
    else if (propertyValue instanceof String) {
      return (String)propertyValue;
    }
    else if (propertyValue instanceof Number) {
      return propertyValue.toString();
    }

    return null;
  }

  private static String trimSingleQuotes(String value) {
    if (value.length() == 0) {
      return value;
    }

    // trim leading and trailing quotes
    if (value.charAt(0) == '\'') {
      value = value.substring(1);
    }
    if (value.length() > 0 && value.charAt(value.length() - 1) == '\'') {
      value = value.substring(0, value.length() - 1);
    }
    
    return value;
  }

}
