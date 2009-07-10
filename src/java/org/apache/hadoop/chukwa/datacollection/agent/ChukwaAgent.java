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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorException;
import org.apache.hadoop.chukwa.datacollection.agent.metrics.AgentMetrics;
import org.apache.hadoop.chukwa.datacollection.connector.Connector;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.test.ConsoleOutConnector;
import org.apache.hadoop.chukwa.util.AdaptorNamingUtils;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * The local agent daemon that runs on each machine. This class is designed to
 * be embeddable, for use in testing.
 * 
 */
public class ChukwaAgent implements AdaptorManager {
  // boolean WRITE_CHECKPOINTS = true;
  static AgentMetrics agentMetrics = new AgentMetrics("ChukwaAgent", "chukwaAgent");

  static Logger log = Logger.getLogger(ChukwaAgent.class);
  static ChukwaAgent agent = null;

  public static ChukwaAgent getAgent() {
    return agent;
  }

  Configuration conf = null;
  Connector connector = null;

  // doesn't need an equals(), comparator, etc
  private static class Offset {
    public Offset(long l, String id) {
      offset = l;
      this.id = id;
    }

    private final String id;
    private volatile long offset;
  }

  public static class AlreadyRunningException extends Exception {

    private static final long serialVersionUID = 1L;

    public AlreadyRunningException() {
      super("Agent already running; aborting");
    }
  }

  private final Map<Adaptor, Offset> adaptorPositions;

  // basically only used by the control socket thread.
  //must be locked before access
  private final Map<String, Adaptor> adaptorsByName;

  private File checkpointDir; // lock this object to indicate checkpoint in
  // progress
  private String CHECKPOINT_BASE_NAME; // base filename for checkpoint files
  // checkpoints
  private static String tags = "";

  private Timer checkpointer;
  private volatile boolean needNewCheckpoint = false; // set to true if any
  // event has happened
  // that should cause a new checkpoint to be written
  private int checkpointNumber; // id number of next checkpoint.
  // should be protected by grabbing lock on checkpointDir

  private final AgentControlSocketListener controlSock;

  public int getControllerPort() {
    return controlSock.getPort();
  }

  /**
   * @param args
   * @throws AdaptorException
   */
  public static void main(String[] args) throws AdaptorException {

    DaemonWatcher.createInstance("Agent");

    try {
      if (args.length > 0 && args[0].equals("-help")) {
        System.out.println("usage:  LocalAgent [-noCheckPoint]"
            + "[default collector URL]");
        System.exit(0);
      }
      Configuration conf = readConfig();
      ChukwaAgent localAgent = new ChukwaAgent(conf);

      if (agent.anotherAgentIsRunning()) {
        System.out
            .println("another agent is running (or port has been usurped). "
                + "Bailing out now");
        DaemonWatcher.bailout(-1);
      }

      int uriArgNumber = 0;
      if (args.length > 0) {
        if (args[uriArgNumber].equals("local"))
          agent.connector = new ConsoleOutConnector(agent);
        else {
          if (!args[uriArgNumber].contains("://"))
            args[uriArgNumber] = "http://" + args[uriArgNumber];
          agent.connector = new HttpConnector(agent, args[uriArgNumber]);
        }
      } else
        agent.connector = new HttpConnector(agent);

      agent.connector.start();

      log.info("local agent started on port " + agent.getControlSock().portno);

    } catch (AlreadyRunningException e) {
      log.error("agent started already on this machine with same portno;"
          + " bailing out");
      System.out
          .println("agent started already on this machine with same portno;"
              + " bailing out");
      System.exit(0); // better safe than sorry
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private boolean anotherAgentIsRunning() {
    return !controlSock.isBound();
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

  public ChukwaAgent() throws AlreadyRunningException {
    this(new Configuration());
  }

  public ChukwaAgent(Configuration conf) throws AlreadyRunningException {
    ChukwaAgent.agent = this;
    this.conf = conf;

    // almost always just reading this; so use a ConcurrentHM.
    // since we wrapped the offset, it's not a structural mod.
    adaptorPositions = new ConcurrentHashMap<Adaptor, Offset>();
    adaptorsByName = new HashMap<String, Adaptor>();
    checkpointNumber = 0;

    boolean DO_CHECKPOINT_RESTORE = conf.getBoolean(
        "chukwaAgent.checkpoint.enabled", true);
    CHECKPOINT_BASE_NAME = conf.get("chukwaAgent.checkpoint.name",
        "chukwa_checkpoint_");
    final int CHECKPOINT_INTERVAL_MS = conf.getInt(
        "chukwaAgent.checkpoint.interval", 5000);

    if (conf.get("chukwaAgent.checkpoint.dir") != null)
      checkpointDir = new File(conf.get("chukwaAgent.checkpoint.dir", null));
    else
      DO_CHECKPOINT_RESTORE = false;

    if (checkpointDir != null && !checkpointDir.exists()) {
      checkpointDir.mkdirs();
    }
    tags = conf.get("chukwaAgent.tags", "cluster=\"unknown\"");
    DataFactory.getInstance().addDefaultTag(conf.get("chukwaAgent.tags", "cluster=\"unknown\""));

    log.info("Config - CHECKPOINT_BASE_NAME: [" + CHECKPOINT_BASE_NAME + "]");
    log.info("Config - checkpointDir: [" + checkpointDir + "]");
    log.info("Config - CHECKPOINT_INTERVAL_MS: [" + CHECKPOINT_INTERVAL_MS
        + "]");
    log.info("Config - DO_CHECKPOINT_RESTORE: [" + DO_CHECKPOINT_RESTORE + "]");
    log.info("Config - tags: [" + tags + "]");

    if (DO_CHECKPOINT_RESTORE) {
      log.info("checkpoints are enabled, period is " + CHECKPOINT_INTERVAL_MS);
    }

    File initialAdaptors = null;
    if (conf.get("chukwaAgent.initial_adaptors") != null)
      initialAdaptors = new File(conf.get("chukwaAgent.initial_adaptors"));

    try {
      if (DO_CHECKPOINT_RESTORE) {
        restoreFromCheckpoint();
      }
    } catch (IOException e) {
      log.warn("failed to restart from checkpoint: ", e);
    }

    try {
      if (initialAdaptors != null && initialAdaptors.exists()
          && checkpointNumber == 0)
        readAdaptorsFile(initialAdaptors); // don't read after checkpoint
                                           // restore
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

      // shouldn't start checkpointing until we're finishing launching
      // adaptors on boot
      if (CHECKPOINT_INTERVAL_MS > 0 && checkpointDir != null) {
        checkpointer = new Timer();
        checkpointer.schedule(new CheckpointTask(), 0, CHECKPOINT_INTERVAL_MS);
      }
    } catch (IOException e) {
      log.info("failed to bind to socket; aborting agent launch", e);
      throw new AlreadyRunningException();
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
    Matcher m = addCmdPattern.matcher(cmd);
    if (m.matches()) {
      long offset; // check for obvious errors first
      try {
        offset = Long.parseLong(m.group(5));
      } catch (NumberFormatException e) {
        log.warn("malformed line " + cmd);
        return null;
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
        return null;
      }
      String coreParams = adaptor.parseArgs(params);
      if(coreParams == null) {
        log.warn("invalid params for adaptor: " + params);
        return null;
      }
      
      if(adaptorID == null) { //user didn't specify, so synthesize
        try {
         adaptorID = AdaptorNamingUtils.synthesizeAdaptorID(adaptorClassName, dataType, coreParams);
        } catch(NoSuchAlgorithmException e) {
          log.fatal("MD5 apparently doesn't work on your machine; bailing", e);
          shutdown(true);
        }
      }
      
      synchronized (adaptorsByName) {
        
        if(adaptorsByName.containsKey(adaptorID))
          return adaptorID;
        adaptorsByName.put(adaptorID, adaptor);
        adaptorPositions.put(adaptor, new Offset(offset, adaptorID));
        needNewCheckpoint = true;
        try {
          adaptor.start(adaptorID, dataType, offset, DataFactory
              .getInstance().getEventQueue(), this);
          log.info("started a new adaptor, id = " + adaptorID);
          ChukwaAgent.agentMetrics.adaptorCount.set(adaptorsByName.size());
          ChukwaAgent.agentMetrics.addedAdaptor.inc();
          return adaptorID;

        } catch (Exception e) {
          log.warn("failed to start adaptor", e);
          // FIXME: don't we need to clean up the adaptor maps here?
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
          return name.startsWith(CHECKPOINT_BASE_NAME);
        }
      });

      if (checkpointNames == null) {
        log.error("Unable to list files in checkpoint dir");
        return false;
      }
      if (checkpointNames.length == 0) {
        log.info("No checkpoints found in " + checkpointDir);
        return false;
      }

      if (checkpointNames.length > 2)
        log.warn("expected at most two checkpoint files in " + checkpointDir
            + "; saw " + checkpointNames.length);
      else if (checkpointNames.length == 0)
        return false;

      String lowestName = null;
      int lowestIndex = Integer.MAX_VALUE;
      for (String n : checkpointNames) {
        int index = Integer
            .parseInt(n.substring(CHECKPOINT_BASE_NAME.length()));
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
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new FileInputStream(checkpoint)));
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
          CHECKPOINT_BASE_NAME + checkpointNumber));
      PrintWriter out = new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(fos)));

      for (Map.Entry<String, String> stat : getAdaptorList().entrySet()) {
        out.println("ADD "+ stat.getKey()+ " = " + stat.getValue());
      }

      out.close();
      File lastCheckpoint = new File(checkpointDir, CHECKPOINT_BASE_NAME
          + (checkpointNumber - 1));
      log.debug("hopefully removing old checkpoint file "
          + lastCheckpoint.getAbsolutePath());
      lastCheckpoint.delete();
      checkpointNumber++;
    }
  }

  public void reportCommit(Adaptor src, long uuid) {
    needNewCheckpoint = true;
    Offset o = adaptorPositions.get(src);
    if (o != null) {
      synchronized (o) { // order writes to offset, in case commits are
                         // processed out of order
        if (uuid > o.offset)
          o.offset = uuid;
      }

      log.info("got commit up to " + uuid + " on " + src + " = " + o.id);
    } else {
      log.warn("got commit up to " + uuid + "  for adaptor " + src
          + " that doesn't appear to be running: " + adaptorCount()
          + " total");
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

  
  private String formatAdaptorStatus(Adaptor a) throws AdaptorException {
    return a.getClass().getCanonicalName() + " " + a.getCurrentStatus() + 
   " " + adaptorPositions.get(a).offset;
  }
  
/**
 * Expose the adaptor list.  Keys are adaptor ID numbers, values are the 
 * adaptor status strings.
 * @return
 */
  public Map<String, String> getAdaptorList() {
    Map<String, String> adaptors = new HashMap<String, String>();
    synchronized (adaptorsByName) {
      for (Map.Entry<String, Adaptor> a : adaptorsByName.entrySet()) {
        try {
          adaptors.put(a.getKey(), formatAdaptorStatus(a.getValue()));
        } catch (AdaptorException e) {
          log.error(e);
        }
      }
    }
    return adaptors;
  }

  /**
   * Stop the adaptor with given ID number. Takes a parameter to indicate
   * whether the adaptor should force out all remaining data, or just exit
   * abruptly.
   * 
   * If the adaptor is written correctly, its offset won't change after
   * returning from shutdown.
   * 
   * @param number the adaptor to stop
   * @param gracefully if true, shutdown, if false, hardStop
   * @return the number of bytes synched at stop. -1 on error
   */
  public long stopAdaptor(String name, boolean gracefully) {
    Adaptor toStop;
    long offset = -1;

    // at most one thread can get past this critical section with toStop != null
    // so if multiple callers try to stop the same adaptor, all but one will
    // fail
    synchronized (adaptorsByName) {
      toStop = adaptorsByName.remove(name);
    }
    if (toStop == null) {
      log.warn("trying to stop adaptor " + name + " that isn't running");
      return offset;
    } else {
      adaptorPositions.remove(toStop);
    }
    ChukwaAgent.agentMetrics.adaptorCount.set(adaptorsByName.size());
    ChukwaAgent.agentMetrics.removedAdaptor.inc();
    
    try {
      if (gracefully) {
        offset = toStop.shutdown();
        log.info("shutdown on adaptor: " + name + ", "
            + toStop.getCurrentStatus());
      } else {
        toStop.hardStop();
        log.info("hardStop on adaptorId: " + name + ", "
            + toStop.getCurrentStatus());
      }
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
  
  @Override
  public Adaptor getAdaptor(String name) {
    synchronized(adaptorsByName) {
      return adaptorsByName.get(name);
    }
  }

  Connector getConnector() {
    return connector;
  }

  private static Configuration readConfig() {
    Configuration conf = new Configuration();

    String chukwaHomeName = System.getenv("CHUKWA_HOME");
    if (chukwaHomeName == null) {
      chukwaHomeName = "";
    }
    File chukwaHome = new File(chukwaHomeName).getAbsoluteFile();

    log.info("Config - CHUKWA_HOME: [" + chukwaHome.toString() + "]");

    String chukwaConfName = System.getProperty("CHUKWA_CONF_DIR");
    File chukwaConf;
    if (chukwaConfName != null)
      chukwaConf = new File(chukwaConfName).getAbsoluteFile();
    else
      chukwaConf = new File(chukwaHome, "conf");

    log.info("Config - CHUKWA_CONF_DIR: [" + chukwaConf.toString() + "]");
    File agentConf = new File(chukwaConf, "chukwa-agent-conf.xml");
    conf.addResource(new Path(agentConf.getAbsolutePath()));
    if (conf.get("chukwaAgent.checkpoint.dir") == null)
      conf.set("chukwaAgent.checkpoint.dir", new File(chukwaHome, "var")
          .getAbsolutePath());
    conf.set("chukwaAgent.initial_adaptors", new File(chukwaConf,
        "initial_adaptors").getAbsolutePath());
    return conf;
  }

  public void shutdown() {
    shutdown(false);
  }

  /**
   * Triggers agent shutdown. For now, this method doesn't shut down adaptors
   * explicitly. It probably should.
   */
  public void shutdown(boolean exit) {
    controlSock.shutdown(); // make sure we don't get new requests
    if (checkpointer != null) {
      checkpointer.cancel();
      try {
        if (needNewCheckpoint)
          writeCheckpoint(); // write a last checkpoint here, before stopping
      } catch (IOException e) {
      }
    }
    // adaptors

    synchronized (adaptorsByName) {
      // shut down each adaptor
      for (Adaptor a : adaptorsByName.values()) {
        try {
          a.hardStop();
        } catch (AdaptorException e) {
          log.warn("failed to cleanly stop " + a, e);
        }
      }
    }
    adaptorsByName.clear();
    adaptorPositions.clear();
    if (exit)
      System.exit(0);
  }

  /**
   * Returns the last offset at which a given adaptor was checkpointed
   * 
   * @param a the adaptor in question
   * @return that adaptor's last-checkpointed offset
   */
  long getOffset(Adaptor a) { //FIXME: do we need this method?
    return adaptorPositions.get(a).offset;
  }

  /**
   * Returns the control socket for this agent.
   */
  private AgentControlSocketListener getControlSock() {
    return controlSock;
  }
}
