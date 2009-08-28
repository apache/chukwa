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


import java.io.*;
import java.net.URL;
import java.util.*;
import org.apache.hadoop.conf.Configuration;

/***
 * An iterator returning a list of Collectors to try. This class is
 * nondeterministic, since it puts collectors back on the list after some
 * period.
 * 
 * No node will be polled more than once per maxRetryRateMs milliseconds.
 * hasNext() will continue return true if you have not called it recently.
 * 
 * 
 */
public class RetryListOfCollectors implements Iterator<String>, Cloneable {

  int maxRetryRateMs;
  List<String> collectors;
  long lastLookAtFirstNode;
  int nextCollector = 0;
  private String portNo;
  Configuration conf;
  public static final String RETRY_RATE_OPT = "chukwaAgent.connector.retryRate";

  public RetryListOfCollectors(File collectorFile, Configuration conf)
      throws IOException {
    this(conf);
    try {
      BufferedReader br = new BufferedReader(new FileReader(collectorFile));
      String line, parsedline;
      while ((line = br.readLine()) != null) {
        parsedline = canonicalizeLine(line);
        collectors.add(parsedline);
      }
      
      br.close();
    } catch (FileNotFoundException e) {
      System.err.println("Error in RetryListOfCollectors() opening file"
            + collectorFile.getCanonicalPath() + ", double check that you have"
            + "set the CHUKWA_CONF_DIR environment variable. Also, ensure file"
            + " exists and is in classpath");
      throw e;
    } catch (IOException e) {
      System.err
          .println("I/O error in RetryListOfcollectors instantiation in readLine() from specified collectors file");
      throw e;
    }
    shuffleList();
  }

  private String canonicalizeLine(String line) {
    String parsedline;
    if (!line.contains("://")) {
      // no protocol, assume http
      if (line.matches(".*:\\d+.*")) {
        parsedline = "http://" + line+"/";
      } else {
        parsedline = "http://" + line + ":" + portNo;
      }
    } else {
      if (line.matches(".*:\\d+.*")) {
        parsedline = line;
      } else {
        parsedline = line + ":" + portNo;
      }
    }
    if(!parsedline.matches(".*\\w/.*")) //no resource name
      parsedline = parsedline+"/";
    return parsedline;
  }

  /**
   * This is only used for debugging. Possibly it should sanitize urls the same way the other
   * constructor does.
   * @param collectors
   * @param maxRetryRateMs
   */
  public RetryListOfCollectors(final List<String> collectors, Configuration conf) {
    this(conf);
    this.collectors.addAll(collectors);
    //we don't shuffle the list here -- this constructor is only used for test purposes
  }
  
  public RetryListOfCollectors(Configuration conf) {
    collectors = new ArrayList<String>();
    this.conf = conf;
    portNo = conf.get("chukwaCollector.http.port", "8080");
    maxRetryRateMs = conf.getInt(RETRY_RATE_OPT, 15 * 1000);
    lastLookAtFirstNode = 0;
  }

  // for now, use a simple O(n^2) algorithm.
  // safe, because we only do this once, and on smallish lists
  public void shuffleList() {
    ArrayList<String> newList = new ArrayList<String>();
    Random r = new java.util.Random();
    while (!collectors.isEmpty()) {
      int toRemove = r.nextInt(collectors.size());
      String next = collectors.remove(toRemove);
      newList.add(next);
    }
    collectors = newList;
  }

  public boolean hasNext() {
    return collectors.size() > 0
        && ((nextCollector != 0) || (System.currentTimeMillis()
            - lastLookAtFirstNode > maxRetryRateMs));
  }

  public String next() {
    if (hasNext()) {
      int currCollector = nextCollector;
      nextCollector = (nextCollector + 1) % collectors.size();
      if (currCollector == 0)
        lastLookAtFirstNode = System.currentTimeMillis();
      return collectors.get(currCollector);
    } else
      return null;
  }

  public void add(String collector) {
    collectors.add(collector);
  }

  public void remove() {
    throw new UnsupportedOperationException();
    // FIXME: maybe just remove a collector from our list and then
    // FIXME: make sure next doesn't break (i.e. reset nextCollector if
    // necessary)
  }

  /**
   * 
   * @return total number of collectors in list
   */
  int total() {
    return collectors.size();
  }
  
  public RetryListOfCollectors clone() {
    try {
      RetryListOfCollectors clone = (RetryListOfCollectors) super.clone();
      return clone;
    } catch(CloneNotSupportedException e) {
      return null;
    }
  }

}
