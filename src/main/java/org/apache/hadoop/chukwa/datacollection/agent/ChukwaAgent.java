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

package org.apache.hadoop.chukwa.datacollection.agent;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorException;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorShutdownPolicy;
import org.apache.hadoop.chukwa.datacollection.adaptor.NotifyOnCommitAdaptor;
import org.apache.hadoop.chukwa.datacollection.OffsetStatsManager;
import org.apache.hadoop.chukwa.datacollection.agent.metrics.AgentMetrics;
import org.apache.hadoop.chukwa.datacollection.connector.Connector;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.test.ConsoleOutConnector;
import org.apache.hadoop.chukwa.util.AdaptorNamingUtils;
import org.apache.hadoop.chukwa.util.ChukwaUtil;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * The local agent daemon that runs on each machine. This class is designed to
 * be embeddable, for use in testing.
 * <P>
 * The agent will start an HTTP REST interface listening on port. Configs for
 * the agent are:
 * <ul>
 * <li><code>chukwaAgent.http.port</code> Port to listen on (default=9090).</li>
 * <li><code>chukwaAgent.http.rest.controller.packages</code> Java packages to
 * inspect for JAX-RS annotated classes to be added as servlets to the REST
 * server.</li>
 * </ul>
 * 
 */
public class ChukwaAgent implements AdaptorManager {
  // boolean WRITE_CHECKPOINTS = true;
  static AgentMetrics agentMetrics = new AgentMetrics("ChukwaAgent", "metrics");

  private final static Logger log = Logger.getLogger(ChukwaAgent.class);  
  private OffsetStatsManager<Adaptor> adaptorStatsManager = null;
  private Timer statsCollector = null;
  private static Configuration conf = null;
  private volatile static ChukwaAgent agent = null;
  public Connector connector = null;
  private boolean stopped = false;
  
  private ChukwaAgent() {
    agent = new ChukwaAgent(new ChukwaConfiguration());
  }

  private ChukwaAgent(Configuration conf) {
    agent = this;
    ChukwaAgent.conf = conf;
    // almost always just reading this; so use a ConcurrentHM.
    // since we wrapped the offset, it's not a structural mod.
    adaptorPositions = new ConcurrentHashMap<Adaptor, Offset>();
    adaptorsByName = new HashMap<String, Adaptor>();
    checkpointNumber = 0;
    stopped = false;
  }

  public static ChukwaAgent getAgent() {
    if(agent == null || agent.isStopped()) {
        agent = new ChukwaAgent();
    } 
    return agent;
  }

  public static ChukwaAgent getAgent(Configuration conf) {
    if(agent == null || agent.isStopped()) {
      agent = new ChukwaAgent(conf);
    }
    return agent;
  }

  public void start() throws AlreadyRunningException {
    boolean checkPointRestore = conf.getBoolean(
        "chukwaAgent.checkpoint.enabled", true);
    checkPointBaseName = conf.get("chukwaAgent.checkpoint.name",
        "chukwa_checkpoint_");
    final int checkPointIntervalMs = conf.getInt(
        "chukwaAgent.checkpoint.interval", 5000);
    final int statsIntervalMs = conf.getInt(
        "chukwaAgent.stats.collection.interval", 10000);
    int statsDataTTLMs = conf.getInt(
        "chukwaAgent.stats.data.ttl", 1200000);

    if (conf.get("chukwaAgent.checkpoint.dir") != null)
      checkpointDir = new File(conf.get("chukwaAgent.checkpoint.dir", null));
    else
      checkPointRestore = false;

    if (checkpointDir != null && !checkpointDir.exists()) {
      boolean result = checkpointDir.mkdirs();
      if(!result) {
        log.error("Failed to create check point directory.");
      }
    }
    String tags = conf.get("chukwaAgent.tags", "cluster=\"unknown\"");
    DataFactory.getInstance().addDefaultTag(conf.get("chukwaAgent.tags", "cluster=\"unknown_cluster\""));

    log.info("Config - CHECKPOINT_BASE_NAME: [" + checkPointBaseName + "]");
    log.info("Config - checkpointDir: [" + checkpointDir + "]");
    log.info("Config - CHECKPOINT_INTERVAL_MS: [" + checkPointIntervalMs
        + "]");
    log.info("Config - DO_CHECKPOINT_RESTORE: [" + checkPointRestore + "]");
    log.info("Config - STATS_INTERVAL_MS: [" + statsIntervalMs + "]");
    log.info("Config - tags: [" + tags + "]");

    if (checkPointRestore) {
      log.info("checkpoints are enabled, period is " + checkPointIntervalMs);
    }

    File initialAdaptors = null;
    if (conf.get("chukwaAgent.initial_adaptors") != null)
      initialAdaptors = new File(conf.get("chukwaAgent.initial_adaptors"));

    try {
      if (checkPointRestore) {
        restoreFromCheckpoint();
      }
    } catch (IOException e) {
      log.warn("failed to restart from checkpoint: ", e);
    }

    try {
      if (initialAdaptors != null && initialAdaptors.exists())
        readAdaptorsFile(initialAdaptors); 
    } catch (IOException e) {
      log.warn("couldn't read user-specified file "
          + initialAdaptors.getAbsolutePath());
    }

    controlSock = new AgentControlSocketListener(this);
    try {
      controlSock.tryToBind(); // do this synchronously; if it fails, we know
      // another agent is running.
      controlSock.start(); // this sets us up as a daemon
      log.info("control socket started on port " + controlSock.portno);
    } catch (IOException e) {
      log.info("failed to bind to socket; aborting agent launch", e);
      throw new AlreadyRunningException();
    }

    // start the HTTP server with stats collection
    try {
      adaptorStatsManager = new OffsetStatsManager<Adaptor>(statsDataTTLMs);
      statsCollector = new Timer("ChukwaAgent Stats Collector");

      startHttpServer(conf);

      statsCollector.scheduleAtFixedRate(new StatsCollectorTask(),
          statsIntervalMs, statsIntervalMs);
    } catch (Exception e) {
      log.error("Couldn't start HTTP server", e);
      throw new RuntimeException(e);
    }

    // shouldn't start check pointing until we're finishing launching
    // adaptors on boot
    if (checkPointIntervalMs > 0 && checkpointDir != null) {
      checkpointer = new Timer();
      checkpointer.schedule(new CheckpointTask(), 0, checkPointIntervalMs);
    }
  }

  // doesn't need an equals(), comparator, etc
  public static class Offset {
    public Offset(long l, String id) {
      offset = l;
      this.id = id;
    }

    final String id;
    volatile long offset;
    public long offset() {
      return this.offset;
    }
    
    public String adaptorID() {
      return id;
    }
  }

  public static class AlreadyRunningException extends Exception {

    private static final long serialVersionUID = 1L;

    public AlreadyRunningException() {
      super("Agent already running; aborting");
    }
  }

  private static Map<Adaptor, Offset> adaptorPositions;

  // basically only used by the control socket thread.
  //must be locked before access
  private static Map<String, Adaptor> adaptorsByName;

  private File checkpointDir; // lock this object to indicate checkpoint in
  // progress
  private String checkPointBaseName; // base filename for checkpoint files
  // checkpoints

  private Timer checkpointer;
  private volatile boolean needNewCheckpoint = false; // set to true if any
  // event has happened
  // that should cause a new checkpoint to be written
  private int checkpointNumber; // id number of next checkpoint.
  // should be protected by grabbing lock on checkpointDir

  private AgentControlSocketListener controlSock;

  public int getControllerPort() {
    return controlSock.getPort();
  }

  public OffsetStatsManager<Adaptor> getAdaptorStatsManager() {
    return adaptorStatsManager;
  }

  /**
   * @param args is command line arguements
   * @throws AdaptorException if error registering adaptors
   */
  public static void main(String[] args) throws AdaptorException {

    try {
      if (args.length > 0 && args[0].equals("-help")) {
        System.out.println("usage:  LocalAgent [-noCheckPoint]"
            + "[default collector URL]");
        return;
      }

      Configuration conf = ChukwaUtil.readConfiguration();
      agent = ChukwaAgent.getAgent(conf);
      if (agent.anotherAgentIsRunning()) {
        log.error("another agent is running (or port has been usurped). "
                + "Bailing out now");
        throw new AlreadyRunningException();
      }

      int uriArgNumber = 0;
      if (args.length > 0) {
        if (args[uriArgNumber].equals("local")) {
          agent.connector = new ConsoleOutConnector(agent);
        } else {
          if (!args[uriArgNumber].contains("://")) {
            args[uriArgNumber] = "http://" + args[uriArgNumber];
          }
          agent.connector = new HttpConnector(agent, args[uriArgNumber]);
        }
      } else {
        String connectorType = conf.get("chukwa.agent.connector", 
            "org.apache.hadoop.chukwa.datacollection.connector.PipelineConnector");
        agent.connector = (Connector) Class.forName(connectorType).newInstance();
      }
      agent.start();
      agent.connector.start();

      log.info("local agent started on port " + agent.getControlSock().portno);
      System.out.close();
      System.err.close();
    } catch (AlreadyRunningException e) {
      log.error("agent started already on this machine with same portno;"
          + " bailing out");
      System.out
          .println("agent started already on this machine with same portno;"
              + " bailing out");
      return;
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private boolean anotherAgentIsRunning() {
    boolean result = false;
    if(controlSock!=null) {
      result = !controlSock.isBound();
    }
    return result;
  }

  /**
   * @return the number of running adaptors inside this local agent
   */
  @Override
  public int adaptorCount() {
    synchronized(adaptorsByName) {
      return adaptorsByName.size();
    }
  }

  private void startHttpServer(Configuration conf) throws Exception {
    ChukwaRestServer.startInstance(conf);
  }
  
  private void stopHttpServer() throws Exception {
    ChukwaRestServer.stopInstance();
  }
  
  /**
   * Take snapshots of offset data so we can report flow rate stats.
   */
  private class StatsCollectorTask extends TimerTask {

    public void run() {
      long now = System.currentTimeMillis();

      for(String adaptorId : getAdaptorList().keySet()) {
        Adaptor adaptor = getAdaptor(adaptorId);
        if(adaptor == null) continue;

        Offset offset = adaptorPositions.get(adaptor);
        if(offset == null) continue;

        adaptorStatsManager.addOffsetDataPoint(adaptor, offset.offset, now);
      }
    }
  }

  // words should contain (space delimited):
  // 0) command ("add")
  // 1) Optional adaptor name, followed by =
  // 2) AdaptorClassname
  // 3) dataType (e.g. "hadoop_log")
  // 4) params <optional>
  // (e.g. for files, this is filename,
  // but can be arbitrarily many space
  // delimited agent specific params )
  // 5) offset
  private Pattern addCmdPattern = Pattern.compile("[aA][dD][dD]\\s+" // command "add",
                                                             // any case, plus
                                                             // at least one
                                                             // space
      + "(?:"   //noncapturing group
      +	"([^\\s=]+)" //containing a string (captured) 
      + "\\s*=\\s*" //then an equals sign, potentially set off with whitespace
      + ")?" //end optional noncapturing group 
      + "([^\\s=]+)\\s+" // the adaptor classname, plus at least one space. No '=' in name
      + "(\\S+)\\s+" // datatype, plus at least one space
      + "(?:" // start a non-capturing group, for the parameters
      + "(.*?)\\s+" // capture the actual parameters reluctantly, followed by
                    // whitespace
      + ")?" // end non-matching group for params; group is optional
      + "(\\d+)\\s*"); // finally, an offset and some trailing whitespace

  /**
   * Most of the Chukwa wire protocol is implemented in @link{AgentControlSocketListener}
   * 
   * Unlike the rest of the chukwa wire protocol, add commands can appear in
   * initial_adaptors and checkpoint files. So it makes sense to handle them here.
   * 
   */
  public String processAddCommand(String cmd) {
    try {
      return processAddCommandE(cmd);
    } catch(AdaptorException e) {
      return null;
    }
  }
  

  public String processAddCommandE(String cmd) throws AdaptorException {
    Matcher m = addCmdPattern.matcher(cmd);
    if (m.matches()) {
      long offset; // check for obvious errors first
      try {
        offset = Long.parseLong(m.group(5));
      } catch (NumberFormatException e) {
        log.warn("malformed line " + cmd);
        throw new AdaptorException("bad input syntax");
      }

      String adaptorID = m.group(1);
      String adaptorClassName = m.group(2);
      String dataType = m.group(3);
      String params = m.group(4);
      if (params == null)
        params = "";
      
      Adaptor adaptor = AdaptorFactory.createAdaptor(adaptorClassName);
      if (adaptor == null) {
        log.warn("Error creating adaptor of class " + adaptorClassName);
        throw new AdaptorException("Can't load class " + adaptorClassName);
      }
      String coreParams = adaptor.parseArgs(dataType,params,this);
      if(coreParams == null) {
        log.warn("invalid params for adaptor: " + params);       
        throw new AdaptorException("invalid params for adaptor: " + params);
      }
      
      if(adaptorID == null) { //user didn't specify, so synthesize
        try {
         adaptorID = AdaptorNamingUtils.synthesizeAdaptorID(adaptorClassName, dataType, coreParams);
        } catch(NoSuchAlgorithmException e) {
          log.fatal("MD5 apparently doesn't work on your machine; bailing", e);
          shutdown(true);
        }
      } else if(!adaptorID.startsWith("adaptor_"))
        adaptorID = "adaptor_"+adaptorID;
      
      synchronized (adaptorsByName) {
        
        if(adaptorsByName.containsKey(adaptorID))
          return adaptorID;
        adaptorsByName.put(adaptorID, adaptor);
        adaptorPositions.put(adaptor, new Offset(offset, adaptorID));
        needNewCheckpoint = true;
        try {
          adaptor.start(adaptorID, dataType, offset, DataFactory
              .getInstance().getEventQueue());
          log.info("started a new adaptor, id = " + adaptorID + " function=["+adaptor.toString()+"]");
          ChukwaAgent.agentMetrics.adaptorCount.set(adaptorsByName.size());
          ChukwaAgent.agentMetrics.addedAdaptor.inc();
          return adaptorID;

        } catch (Exception e) {
          Adaptor failed = adaptorsByName.remove(adaptorID);
          adaptorPositions.remove(failed);
          adaptorStatsManager.remove(failed);
          log.warn("failed to start adaptor", e);
          if(e instanceof AdaptorException)
            throw (AdaptorException)e;
        }
      }
    } else if (cmd.length() > 0)
      log.warn("only 'add' command supported in config files; cmd was: " + cmd);
    // no warning for blank line

    return null;
  }



  /**
   * Tries to restore from a checkpoint file in checkpointDir. There should
   * usually only be one checkpoint present -- two checkpoints present implies a
   * crash during writing the higher-numbered one. As a result, this method
   * chooses the lowest-numbered file present.
   * 
   * Lines in the checkpoint file are processed one at a time with
   * processCommand();
   * 
   * @return true if the restore succeeded
   * @throws IOException
   */
  private boolean restoreFromCheckpoint() throws IOException {
    synchronized (checkpointDir) {
      String[] checkpointNames = checkpointDir.list(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.startsWith(checkPointBaseName);
        }
      });

      if (checkpointNames == null) {
        log.error("Unable to list files in checkpoint dir");
        return false;
      } else if (checkpointNames.length == 0) {
        log.info("No checkpoints found in " + checkpointDir);
        return false;
      } else if (checkpointNames.length > 2) {
        log.warn("expected at most two checkpoint files in " + checkpointDir
            + "; saw " + checkpointNames.length);
      }

      String lowestName = null;
      int lowestIndex = Integer.MAX_VALUE;
      for (String n : checkpointNames) {
        int index = Integer
            .parseInt(n.substring(checkPointBaseName.length()));
        if (index < lowestIndex) {
          lowestName = n;
          lowestIndex = index;
        }
      }

      checkpointNumber = lowestIndex + 1;
      File checkpoint = new File(checkpointDir, lowestName);
      readAdaptorsFile(checkpoint);
    }
    return true;
  }

  private void readAdaptorsFile(File checkpoint) throws FileNotFoundException,
      IOException {
    log.info("starting adaptors listed in " + checkpoint.getAbsolutePath());
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new FileInputStream(checkpoint), Charset.forName("UTF-8")));
    String cmd = null;
    while ((cmd = br.readLine()) != null)
      processAddCommand(cmd);
    br.close();
  }

  /**
   * Called periodically to write checkpoints
   * 
   * @throws IOException
   */
  private void writeCheckpoint() throws IOException {
    needNewCheckpoint = false;
    synchronized (checkpointDir) {
      log.info("writing checkpoint " + checkpointNumber);

      FileOutputStream fos = new FileOutputStream(new File(checkpointDir,
          checkPointBaseName + checkpointNumber));
      PrintWriter out = new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(fos, Charset.forName("UTF-8"))));

      for (Map.Entry<String, String> stat : getAdaptorList().entrySet()) {
        out.println("ADD "+ stat.getKey()+ " = " + stat.getValue());
      }

      out.close();
      File lastCheckpoint = new File(checkpointDir, checkPointBaseName
          + (checkpointNumber - 1));
      log.debug("hopefully removing old checkpoint file "
          + lastCheckpoint.getAbsolutePath());
      boolean result = lastCheckpoint.delete();
      if(!result) {
        log.warn("Unable to delete lastCheckpoint file: "+lastCheckpoint.getAbsolutePath());
      }
      checkpointNumber++;
    }
  }

  public String reportCommit(Adaptor src, long uuid) {
    needNewCheckpoint = true;
    Offset o = adaptorPositions.get(src);
    if (o != null) {
      synchronized (o) { // order writes to offset, in case commits are
                         // processed out of order
        if (uuid > o.offset)
          o.offset = uuid;
      }
      log.debug("got commit up to " + uuid + " on " + src + " = " + o.id);
      if(src instanceof NotifyOnCommitAdaptor) {
        ((NotifyOnCommitAdaptor) src).committed(uuid);
      }
      return o.id;
    } else {
      log.warn("got commit up to " + uuid + "  for adaptor " + src
          + " that doesn't appear to be running: " + adaptorCount()
          + " total");
      return null;
    }
  }

  private class CheckpointTask extends TimerTask {
    public void run() {
      try {
        if (needNewCheckpoint) {
          writeCheckpoint();
        }
      } catch (IOException e) {
        log.warn("failed to write checkpoint", e);
      }
    }
  }

  
  private String formatAdaptorStatus(Adaptor a) {
    return a.getClass().getCanonicalName() + " " + a.getCurrentStatus() + 
   " " + adaptorPositions.get(a).offset;
  }
  
/**
 * Expose the adaptor list.  Keys are adaptor ID numbers, values are the 
 * adaptor status strings.
 * @return adaptor list
 */
  public Map<String, String> getAdaptorList() {
    Map<String, String> adaptors = new HashMap<String, String>(adaptorsByName.size());
    synchronized (adaptorsByName) {
      for (Map.Entry<String, Adaptor> a : adaptorsByName.entrySet()) {
        adaptors.put(a.getKey(), formatAdaptorStatus(a.getValue()));
      }
    }
    return adaptors;
  }
  

  public long stopAdaptor(String name, boolean gracefully) {
    if (gracefully) 
      return stopAdaptor(name, AdaptorShutdownPolicy.GRACEFULLY);
    else
      return stopAdaptor(name, AdaptorShutdownPolicy.HARD_STOP);
  }

  /**
   * Stop the adaptor with given ID number. Takes a parameter to indicate
   * whether the adaptor should force out all remaining data, or just exit
   * abruptly.
   * 
   * If the adaptor is written correctly, its offset won't change after
   * returning from shutdown.
   * 
   * @param name the adaptor to stop
   * @param shutdownMode if true, shutdown, if false, hardStop
   * @return the number of bytes synched at stop. -1 on error
   */
  public long stopAdaptor(String name, AdaptorShutdownPolicy shutdownMode) {
    Adaptor toStop;
    long offset = -1;

    // at most one thread can get past this critical section with toStop != null
    // so if multiple callers try to stop the same adaptor, all but one will
    // fail
    synchronized (adaptorsByName) {
      toStop = adaptorsByName.remove(name);
    }
    if (toStop == null) {
      log.warn("trying to stop " + name + " that isn't running");
      return offset;
    } else {
      adaptorPositions.remove(toStop);
      adaptorStatsManager.remove(toStop);
    }
    ChukwaAgent.agentMetrics.adaptorCount.set(adaptorsByName.size());
    ChukwaAgent.agentMetrics.removedAdaptor.inc();
    
    try {
      offset = toStop.shutdown(shutdownMode);
      log.info("shutdown ["+ shutdownMode + "] on " + name + ", "
          + toStop.getCurrentStatus());
    } catch (AdaptorException e) {
      log.error("adaptor failed to stop cleanly", e);
    } finally {
      needNewCheckpoint = true;
    }
    return offset;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }
  
  public static Configuration getStaticConfiguration() {
    return conf;
  }
  
  @Override
  public Adaptor getAdaptor(String name) {
    synchronized(adaptorsByName) {
      return adaptorsByName.get(name);
    }
  }

  public Offset offset(Adaptor a) {
    Offset o = adaptorPositions.get(a);
    return o;
  }
  
  public Connector getConnector() {
    return connector;
  }

  public void shutdown() {
    shutdown(false);
  }

  /**
   * Triggers agent shutdown. For now, this method doesn't shut down adaptors
   * explicitly. It probably should.
   * @param force sets flag to exit forcefully
   */
  public void shutdown(boolean force) {
    controlSock.shutdown(); // make sure we don't get new requests

    if (statsCollector != null) {
      statsCollector.cancel();
    }

    try {
      stopHttpServer();
    } catch (Exception e) {
      log.error("Couldn't stop jetty server.", e);
    }

    // adaptors

    synchronized (adaptorsByName) {
      // shut down each adaptor
      for (Adaptor a : adaptorsByName.values()) {
        try {
          a.shutdown(AdaptorShutdownPolicy.HARD_STOP);
        } catch (AdaptorException e) {
          log.warn("failed to cleanly stop " + a, e);
        }
      }
    }
    if (checkpointer != null) {
      checkpointer.cancel();
      try {
        if (needNewCheckpoint)
          writeCheckpoint(); // write a last checkpoint here, before stopping
      } catch (IOException e) {
        log.debug(ExceptionUtil.getStackTrace(e));
      }
    }
    adaptorsByName.clear();
    adaptorPositions.clear();
    adaptorStatsManager.clear();
    agent.stop();
    if (force)
      return;
  }

  /**
   * Set agent into stop state.
   */
  private void stop() {
    stopped = true;
  }

  /**
   * Check if agent is in stop state.
   * @return true if agent is in stop state.
   */
  private boolean isStopped() {
    return stopped;
  }

  /**
   * Returns the control socket for this agent.
   */
  private AgentControlSocketListener getControlSock() {
    return controlSock;
  }

  public String getAdaptorName(Adaptor initiator) {
    Offset o = adaptorPositions.get(initiator);
    if(o != null)
      return o.id;
    else return null;
  }
}
