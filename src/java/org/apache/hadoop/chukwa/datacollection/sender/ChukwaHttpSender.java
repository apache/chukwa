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


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.datacollection.sender.metrics.HttpSenderMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.log4j.Logger;

/**
 * Encapsulates all of the http setup and connection details needed for chunks
 * to be delivered to a collector.
 * 
 * This class should encapsulate the details of the low level data formatting.
 * The Connector is responsible for picking what to send and to whom;
 * retry policy is encoded in the collectors iterator.
 * 
 * This class is not thread safe. Synchronization is the caller's responsibility.
 * 
 * <p>
 * On error, tries the list of available collectors, pauses for a minute, and
 * then repeats.
 * </p>
 * <p>
 * Will wait forever for collectors to come up.
 * </p>
 */
public class ChukwaHttpSender implements ChukwaSender {
  final int MAX_RETRIES_PER_COLLECTOR; // fast retries, in http client
  final int SENDER_RETRIES;
  final int WAIT_FOR_COLLECTOR_REBOOT;
  final int COLLECTOR_TIMEOUT;
  
  public static final String COLLECTOR_TIMEOUT_OPT = "chukwaAgent.sender.collectorTimeout";
  // FIXME: this should really correspond to the timer in RetryListOfCollectors

  static final HttpSenderMetrics metrics = new HttpSenderMetrics("chukwaAgent", "httpSender");
  
  static Logger log = Logger.getLogger(ChukwaHttpSender.class);
  static HttpClient client = null;
  static MultiThreadedHttpConnectionManager connectionManager = null;
  String currCollector = null;
  int postID = 0;

  protected Iterator<String> collectors;

  static {
    connectionManager = new MultiThreadedHttpConnectionManager();
    client = new HttpClient(connectionManager);
    connectionManager.closeIdleConnections(1000);
  }

  public static class CommitListEntry {
    public Adaptor adaptor;
    public long uuid;
    public long start; //how many bytes of stream
    public CommitListEntry(Adaptor a, long uuid, long start) {
      adaptor = a;
      this.uuid = uuid;
      this.start = start;
    }
  }

  // FIXME: probably we're better off with an EventListRequestEntity
  static class BuffersRequestEntity implements RequestEntity {
    List<DataOutputBuffer> buffers;

    public BuffersRequestEntity(List<DataOutputBuffer> buf) {
      buffers = buf;
    }

    public long getContentLength() {
      long len = 4;// first we send post length, then buffers
      for (DataOutputBuffer b : buffers)
        len += b.getLength();
      return len;
    }

    public String getContentType() {
      return "application/octet-stream";
    }

    public boolean isRepeatable() {
      return true;
    }

    public void writeRequest(OutputStream out) throws IOException {
      DataOutputStream dos = new DataOutputStream(out);
      dos.writeInt(buffers.size());
      for (DataOutputBuffer b : buffers)
        dos.write(b.getData(), 0, b.getLength());
    }
  }

  public ChukwaHttpSender(Configuration c) {
    // setup default collector
    ArrayList<String> tmp = new ArrayList<String>();
    this.collectors = tmp.iterator();

    MAX_RETRIES_PER_COLLECTOR = c.getInt("chukwaAgent.sender.fastRetries", 4);
    SENDER_RETRIES = c.getInt("chukwaAgent.sender.retries", 144000);
    WAIT_FOR_COLLECTOR_REBOOT = c.getInt("chukwaAgent.sender.retryInterval",
        20 * 1000);
    COLLECTOR_TIMEOUT = c.getInt(COLLECTOR_TIMEOUT_OPT, 30*1000);
  }

  /**
   * Set up a list of connectors for this client to send {@link Chunk}s to
   * 
   * @param collectors
   */
  public void setCollectors(Iterator<String> collectors) {
    this.collectors = collectors;
    // setup a new destination from our list of collectors if one hasn't been
    // set up
    if (currCollector == null) {
      if (collectors.hasNext()) {
        currCollector = collectors.next();
      } else
        log.error("No collectors to try in send(), won't even try to do doPost()");
    }
  }

  /**
   * grab all of the chunks currently in the chunkQueue, stores a copy of them
   * locally, calculates their size, sets them up
   * 
   * @return array of chunk id's which were ACKed by collector
   */
  @Override
  public List<CommitListEntry> send(List<Chunk> toSend)
      throws InterruptedException, IOException {
    List<DataOutputBuffer> serializedEvents = new ArrayList<DataOutputBuffer>();
    List<CommitListEntry> commitResults = new ArrayList<CommitListEntry>();

    int thisPost = postID++;
    int toSendSize = toSend.size();
    log.info("collected " + toSendSize + " chunks for post_"+thisPost);

    // Serialize each chunk in turn into it's own DataOutputBuffer and add that
    // buffer to serializedEvents
    for (Chunk c : toSend) {
      DataOutputBuffer b = new DataOutputBuffer(c.getSerializedSizeEstimate());
      try {
        c.write(b);
      } catch (IOException err) {
        log.error("serialization threw IOException", err);
      }
      serializedEvents.add(b);
      // store a CLE for this chunk which we will use to ack this chunk to the
      // caller of send()
      // (e.g. the agent will use the list of CLE's for checkpointing)
      commitResults.add(new CommitListEntry(c.getInitiator(), c.getSeqID(), 
         c.getSeqID() - c.getData().length));
    }
    toSend.clear();

    // collect all serialized chunks into a single buffer to send
    RequestEntity postData = new BuffersRequestEntity(serializedEvents);

    PostMethod method = new PostMethod();
    method.setRequestEntity(postData);
    log.info(">>>>>> HTTP post_"+thisPost + " to " + currCollector + " length = " + postData.getContentLength());

    List<CommitListEntry> results =  postAndParseResponse(method, commitResults);
    log.info("post_" + thisPost + " sent " + toSendSize + " chunks, got back " + results.size() + " acks");
    return results;
  }
  
  /**
   * 
   * @param method the data to push
   * @param expectedCommitResults the list
   * @return the list of committed chunks
   * @throws IOException
   * @throws InterruptedException
   */
  public List<CommitListEntry> postAndParseResponse(PostMethod method, 
        List<CommitListEntry> expectedCommitResults)
  throws IOException, InterruptedException{
    reliablySend(method, "chukwa"); //FIXME: shouldn't need to hardcode this here
    return expectedCommitResults;
  }

  /**
   *  Responsible for executing the supplied method on at least one collector
   * @param method
   * @return
   * @throws InterruptedException
   * @throws IOException if no collector responds with an OK
   */
  protected List<String> reliablySend(HttpMethodBase method, String pathSuffix) throws InterruptedException, IOException {
    int retries = SENDER_RETRIES;
    while (currCollector != null) {
      // need to pick a destination here
      try {

        // send it across the network    
        List<String> responses = doRequest(method, currCollector+ pathSuffix);

        retries = SENDER_RETRIES; // reset count on success

        return responses;
      } catch (Throwable e) {
        log.error("Http post exception on "+ currCollector +": "+ e.toString());
        log.debug("Http post exception on "+ currCollector, e);
        ChukwaHttpSender.metrics.httpThrowable.inc();
        if (collectors.hasNext()) {
          ChukwaHttpSender.metrics.collectorRollover.inc();
          boolean repeatPost = failedCollector(currCollector);
          currCollector = collectors.next();
          if(repeatPost)
            log.info("Found a new collector to roll over to, retrying HTTP Post to collector "
                + currCollector);
          else {
            log.info("Using " + currCollector + " in the future, but not retrying this post");
            break;
          }
        } else {
          if (retries > 0) {
            log.warn("No more collectors to try rolling over to; waiting "
                + WAIT_FOR_COLLECTOR_REBOOT + " ms (" + retries
                + " retries left)");
            Thread.sleep(WAIT_FOR_COLLECTOR_REBOOT);
            retries--;
          } else {
            log.error("No more collectors to try rolling over to; aborting post");
            throw new IOException("no collectors");
          }
        }
      } finally {
        // be sure the connection is released back to the connection manager
        method.releaseConnection();
      }
    } // end retry loop
    return new ArrayList<String>();
  }

  /**
   * A hook for taking action when a collector is declared failed.
   * Returns whether to retry current post, or junk it
   * @param downCollector
   */
  protected boolean failedCollector(String downCollector) {
    log.debug("declaring "+ downCollector + " down");
    return true;
  }

  /**
   * Responsible for performing a single operation to a specified collector URL.
   * 
   * @param dest the URL being requested. (Including hostname)
   */
  protected List<String> doRequest(HttpMethodBase method, String dest)
      throws IOException, HttpException {

    HttpMethodParams pars = method.getParams();
    pars.setParameter(HttpMethodParams.RETRY_HANDLER,
        (Object) new HttpMethodRetryHandler() {
          public boolean retryMethod(HttpMethod m, IOException e, int exec) {
            return !(e instanceof java.net.ConnectException)
                && (exec < MAX_RETRIES_PER_COLLECTOR);
          }
        });

    pars.setParameter(HttpMethodParams.SO_TIMEOUT, new Integer(COLLECTOR_TIMEOUT));

    method.setParams(pars);
    method.setPath(dest);

    // Send POST request
    ChukwaHttpSender.metrics.httpPost.inc();
    
    int statusCode = client.executeMethod(method);

    if (statusCode != HttpStatus.SC_OK) {
      ChukwaHttpSender.metrics.httpException.inc();
      
      if (statusCode == HttpStatus.SC_REQUEST_TIMEOUT ) {
        ChukwaHttpSender.metrics.httpTimeOutException.inc();
      }
      
      log.error(">>>>>> HTTP response from " + dest + " statusLine: " + method.getStatusLine());
      // do something aggressive here
      throw new HttpException("got back a failure from server");
    }
    // implicitly "else"
    log.info(">>>>>> HTTP Got success back from "+ dest + "; response length "
            + method.getResponseContentLength());

    // FIXME: should parse acks here
    InputStream rstream = null;

    // Get the response body
    byte[] resp_buf = method.getResponseBody();
    rstream = new ByteArrayInputStream(resp_buf);
    BufferedReader br = new BufferedReader(new InputStreamReader(rstream));
    String line;
    List<String> resp = new ArrayList<String>();
    while ((line = br.readLine()) != null) {
      if (log.isDebugEnabled()) {
        log.debug("response: " + line);
      }
      resp.add(line);
    }
    return resp;
  }

  @Override
  public void stop() {
  }
}
