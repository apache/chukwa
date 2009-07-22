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

package org.apache.hadoop.chukwa.datacollection.controller;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.log4j.Logger;

/**
 * A convenience library for applications to communicate to the
 * {@link ChukwaAgent}. Can be used to register and unregister new
 * {@link Adaptor}s. Also contains functions for applications to use for
 * handling log rations.
 */
public class ChukwaAgentController {
  static Logger log = Logger.getLogger(ChukwaAgentController.class);
   
  public class AddAdaptorTask extends TimerTask {

    String adaptorName;
    String type;
    String params;
    private long offset;
    long numRetries;
    long retryInterval;

    AddAdaptorTask(String adaptorName, String type, String params, long offset,
                   long numRetries, long retryInterval) {
      this.adaptorName = adaptorName;
      this.type = type;
      this.params = params;
      this.offset = offset;
      this.numRetries = numRetries;
      this.retryInterval = retryInterval;
    }

    @Override
    public void run() {
      try {
        log.info("Trying to resend the add command [" + adaptorName + "]["
            + offset + "][" + params + "] [" + numRetries + "]");
        addByName(null, adaptorName, type, params, offset, numRetries, retryInterval);
      } catch (Exception e) {
        log.warn("Exception in AddAdaptorTask.run", e);
        e.printStackTrace();
      }
    }
  }

  // our default adaptors, provided here for convenience
  public static final String CharFileTailUTF8 = "org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8";
  public static final String CharFileTailUTF8NewLineEscaped = "org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped";

  static String DEFAULT_FILE_TAILER = CharFileTailUTF8NewLineEscaped;
  static int DEFAULT_PORT = 9093;
  static String DEFAULT_HOST = "localhost";
  static int numArgs = 0;

  class Adaptor {
    public String id;
    final public String className;
    final public String params;
    final public String appType;
    public long offset;

    Adaptor(String className, String appType, String params, long offset) {
      this.className = className;
      this.appType = appType;
      this.params = params;
      this.offset = offset;
    }

    Adaptor(String id, String className, String appType, String params,
            long offset) {
      this.id = id;
      this.className = className;
      this.appType = appType;
      this.params = params;
      this.offset = offset;
    }

    /**
     * Registers this {@link Adaptor} with the agent running at the specified
     * hostname and portno
     * 
     * @return The id of the this {@link Adaptor}, assigned by the agent
     *         upon successful registration
     * @throws IOException
     */
    String register() throws IOException {
      Socket s = new Socket(hostname, portno);
      try {
        s.setSoTimeout(60000);
      } catch (SocketException e) {
        log.warn("Error while settin soTimeout to 60000");
        e.printStackTrace();
      }
      PrintWriter bw = new PrintWriter(new OutputStreamWriter(s
          .getOutputStream()));
      if(id != null)
        bw.println("ADD " + id + " = " + className + " " + appType + " " + params + " " + offset);
      else
        bw.println("ADD " + className + " " + appType + " " + params + " " + offset);
      bw.flush();
      BufferedReader br = new BufferedReader(new InputStreamReader(s
          .getInputStream()));
      String resp = br.readLine();
      if (resp != null) {
        String[] fields = resp.split(" ");
        if (fields[0].equals("OK")) {
            id = fields[fields.length - 1];
        }
      }
      s.close();
      return id;
    }

    void unregister() throws IOException {
      Socket s = new Socket(hostname, portno);
      try {
        s.setSoTimeout(60000);
      } catch (SocketException e) {
        log.warn("Error while settin soTimeout to 60000");
        e.printStackTrace();
      }
      PrintWriter bw = new PrintWriter(new OutputStreamWriter(s
          .getOutputStream()));
      bw.println("SHUTDOWN " + id);
      bw.flush();

      BufferedReader br = new BufferedReader(new InputStreamReader(s
          .getInputStream()));
      String resp = br.readLine();
      if (resp == null || !resp.startsWith("OK")) {
        // error. What do we do?
      } else if (resp.startsWith("OK")) {
        String[] respSplit = resp.split(" ");
        String newOffset = respSplit[respSplit.length - 1];
        try {
          offset = Long.parseLong(newOffset);
        } catch (NumberFormatException nfe) {
          log.error("adaptor didn't shutdown gracefully.\n" + nfe);
        }
      }

      s.close();
    }

    public String toString() {
      String[] namePieces = className.split("\\.");
      String shortName = namePieces[namePieces.length - 1];
      return id + " " + shortName + " " + appType + " " + params + " " + offset;
    }
  }

  Map<String, ChukwaAgentController.Adaptor> runningAdaptors = new HashMap<String, Adaptor>();
  Map<String, ChukwaAgentController.Adaptor> runningInstanceAdaptors = new HashMap<String, Adaptor>();
  Map<String, ChukwaAgentController.Adaptor> pausedAdaptors;
  String hostname;
  int portno;

  public ChukwaAgentController() {
    portno = DEFAULT_PORT;
    hostname = DEFAULT_HOST;
    pausedAdaptors = new HashMap<String, Adaptor>();

    syncWithAgent();
  }

  public ChukwaAgentController(String hostname, int portno) {
    this.hostname = hostname;
    this.portno = portno;
    pausedAdaptors = new HashMap<String, Adaptor>();

    syncWithAgent();
  }

  private boolean syncWithAgent() {
    // set up adaptors by using list here
    try {
      runningAdaptors = list();
      return true;
    } catch (IOException e) {
      System.err.println("Error initializing ChukwaClient with list of "
              + "currently registered adaptors, clearing our local list of adaptors");
      // e.printStackTrace();
      // if we can't connect to the LocalAgent, reset/clear our local view of
      // the Adaptors.
      runningAdaptors = new HashMap<String, ChukwaAgentController.Adaptor>();
      return false;
    }
  }

  /**
   * Registers a new adaptor. Makes no guarantee about success. On failure, we
   * print a message to stderr and ignore silently so that an application
   * doesn't crash if it's attempt to register an adaptor fails. This call does
   * not retry a conection. for that use the overloaded version of this which
   * accepts a time interval and number of retries
   * 
   * @return the id number of the adaptor, generated by the agent
   */
  public String add(String adaptorName, String type, String params, long offset) {
    return addByName(null, adaptorName, type, params, offset, 20, 15 * 1000);// retry for
                                                                 // five
                                                                 // minutes,
                                                                 // every
                                                                 // fifteen
                                                                 // seconds
  }

  /**
   * Registers a new adaptor. Makes no guarantee about success. On failure, to
   * connect to server, will retry <code>numRetries</code> times, every
   * <code>retryInterval</code> milliseconds.
   * 
   * @return the id number of the adaptor, generated by the agent
   */
  public String addByName(String adaptorID, String adaptorName, String type, String params, long offset,
      long numRetries, long retryInterval) {
    ChukwaAgentController.Adaptor adaptor = new ChukwaAgentController.Adaptor(
        adaptorName, type, params, offset);
    adaptor.id = adaptorID;
    if (numRetries >= 0) {
      try {
        adaptorID = adaptor.register();

        if (adaptorID != null) {
          runningAdaptors.put(adaptorID, adaptor);
          runningInstanceAdaptors.put(adaptorID, adaptor);
        } else {
          System.err.println("Failed to successfully add the adaptor in AgentClient, adaptorID returned by add() was negative.");
        }
      } catch (IOException ioe) {
        log.warn("AgentClient failed to contact the agent ("
            + hostname + ":" + portno + ")");
        
        log.warn("Scheduling a agent connection retry for adaptor add() in another "
                + retryInterval
                + " milliseconds, "
                + numRetries
                + " retries remaining");

        Timer addFileTimer = new Timer();
        addFileTimer.schedule(new AddAdaptorTask(adaptorName, type, params,
            offset, numRetries - 1, retryInterval), retryInterval);
      }
    } else {
      System.err.println("Giving up on connecting to the local agent");
    }
    return adaptorID;
  }

  public synchronized ChukwaAgentController.Adaptor remove(String adaptorID)
      throws IOException {
    syncWithAgent();
    ChukwaAgentController.Adaptor a = runningAdaptors.remove(adaptorID);
    if ( a != null ) {
      a.unregister();
    }
    return a;

  }

  public void remove(String className, String appType, String filename)
      throws IOException {
    syncWithAgent();
    // search for FileTail adaptor with string of this file name
    // get its id, tell it to unregister itself with the agent,
    // then remove it from the list of adaptors
    for (Adaptor a : runningAdaptors.values()) {
      if (a.className.equals(className) && a.params.equals(filename)
          && a.appType.equals(appType)) {
        remove(a.id);
      }
    }
  }

  public void removeAll() {
    syncWithAgent();
    ArrayList<String> keyset = new ArrayList<String>();
    keyset.addAll( runningAdaptors.keySet());

    for (String id : keyset) {
      try {
        remove(id);
      } catch (IOException ioe) {
        System.err.println("Error removing an adaptor in removeAll()");
        ioe.printStackTrace();
      }
      log.info("Successfully removed adaptor " + id);
    }
  }

  public void removeInstanceAdaptors() {
    // Remove adaptors created by this instance of chukwa agent controller.
    // Instead of removing using id, this is removed by using the stream name
    // and record type.  This prevents the system to shutdown the wrong
    // adaptor after agent crashes.
    for (Adaptor a : runningInstanceAdaptors.values()) {
      try {
        remove(a.className, a.appType, a.params);
      } catch (IOException ioe) {
        log.warn("Error removing an adaptor in removeInstanceAdaptors()");
        ioe.printStackTrace();
      }
    }
  }

  Map<String, ChukwaAgentController.Adaptor> list() throws IOException {
    Socket s = new Socket(hostname, portno);
    try {
      s.setSoTimeout(60000);
    } catch (SocketException e) {
      log.warn("Error while settin soTimeout to 60000");
      e.printStackTrace();
    }
    PrintWriter bw = new PrintWriter(
        new OutputStreamWriter(s.getOutputStream()));

    bw.println("LIST");
    bw.flush();
    BufferedReader br = new BufferedReader(new InputStreamReader(s
        .getInputStream()));
    String ln;
    Map<String, Adaptor> listResult = new HashMap<String, Adaptor>();
    while ((ln = br.readLine()) != null) {
      if (ln.equals("")) {
        break;
      } else {
        String[] parts = ln.split("\\s+");
        if (parts.length >= 4) { // should have id, className appType, params,
                                 // offset
          String id = parts[0].substring(0, parts[0].length() - 1); // chop
                                                                        // off
                                                                        // the
                                                                        // right
                                                                        // -
                                                                        // paren
          long offset = Long.parseLong(parts[parts.length - 1]);
          String tmpParams = parts[3];
          for (int i = 4; i < parts.length - 1; i++) {
            tmpParams += " " + parts[i];
          }
          listResult.put(id, new Adaptor(id, parts[1], parts[2], tmpParams,
              offset));
        }
      }
    }
    s.close();
    return listResult;
  }

  // ************************************************************************
  // The following functions are convenience functions, defining an easy
  // to use API for application developers to integrate chukwa into their app
  // ************************************************************************

  /**
   * Registers a new "LineFileTailUTF8" adaptor and starts it at offset 0.
   * Checks to see if the file is being watched already, if so, won't register
   * another adaptor with the agent. If you have run the tail adaptor on this
   * file before and rotated or emptied the file you should use
   * {@link ChukwaAgentController#pauseFile(String, String)} and
   * {@link ChukwaAgentController#resumeFile(String, String)} which will store
   * the adaptors metadata and re-use them to pick up where it left off.
   * 
   * @param type the datatype associated with the file to pass through
   * @param filename of the file for the tail adaptor to start monitoring
   * @return the id number of the adaptor, generated by the agent
   */
  public String addFile(String appType, String filename, long numRetries,
      long retryInterval) {
    filename = new File(filename).getAbsolutePath();
    // TODO: Mabye we want to check to see if the file exists here?
    // Probably not because they might be talking to an agent on a different
    // machine?

    // check to see if this file is being watched already, if yes don't set up
    // another adaptor for it
    boolean isDuplicate = false;
    for (Adaptor a : runningAdaptors.values()) {
      if (a.className.equals(DEFAULT_FILE_TAILER) && a.appType.equals(appType)
          && a.params.endsWith(filename)) {
        isDuplicate = true;
      }
    }
    if (!isDuplicate) {
      return addByName(null, DEFAULT_FILE_TAILER, appType, 0L + " " + filename, 0L,
          numRetries, retryInterval);
    } else {
      log.info("An adaptor for filename \"" + filename
          + "\", type \"" + appType
          + "\", exists already, addFile() command aborted");
      return null;
    }
  }

  public String addFile(String appType, String filename) {
    return addFile(appType, filename, 0, 0);
  }

  /**
   * Pause all active adaptors of the default file tailing type who are tailing
   * this file This means we actually stop the adaptor and it goes away forever,
   * but we store it state so that we can re-launch a new adaptor with the same
   * state later.
   * 
   * @param appType
   * @param filename
   * @return array of adaptorID numbers which have been created and assigned the
   *         state of the formerly paused adaptors
   * @throws IOException
   */
  public Collection<String> pauseFile(String appType, String filename)
      throws IOException {
    syncWithAgent();
    // store the unique streamid of the file we are pausing.
    // search the list of adaptors for this filename
    // store the current offset for it
    List<String> results = new ArrayList<String>();
    for (Adaptor a : runningAdaptors.values()) {
      if (a.className.equals(DEFAULT_FILE_TAILER) && a.params.endsWith(filename)
          && a.appType.equals(appType)) {
        pausedAdaptors.put(a.id, a); // add it to our list of paused adaptors
        remove(a.id); // tell the agent to remove/unregister it
        results.add(a.id);
      }
    }
    return results;
  }

  public boolean isFilePaused(String appType, String filename) {
    for (Adaptor a : pausedAdaptors.values()) {
      if (a.className.equals(DEFAULT_FILE_TAILER) && a.params.endsWith(filename)
          && a.appType.equals(appType)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Resume all adaptors for this filename that have been paused
   * 
   * @param appType the appType
   * @param filename filename by which to lookup adaptors which are paused (and
   *        tailing this file)
   * @return an array of the new adaptor ID numbers which have resumed where the
   *         old adaptors left off
   * @throws IOException
   */
  public Collection<String> resumeFile(String appType, String filename)
      throws IOException {
    syncWithAgent();
    // search for a record of this paused file
    List<String> results = new ArrayList<String>();
    for (Adaptor a : pausedAdaptors.values()) {
      if (a.className.equals(DEFAULT_FILE_TAILER) && a.params.endsWith(filename)
          && a.appType.equals(appType)) {
        String newID = add(DEFAULT_FILE_TAILER, a.appType, a.offset + " "
            + filename, a.offset);
        pausedAdaptors.remove(a.id);
        a.id = newID;
        results.add(a.id);
      }
    }
    return results;
  }

  public void removeFile(String appType, String filename) throws IOException {
    syncWithAgent();
    // search for FileTail adaptor with string of this file name
    // get its id, tell it to unregister itself with the agent,
    // then remove it from the list of adaptors
    for (Adaptor a : runningAdaptors.values()) {
      if (a.className.equals(DEFAULT_FILE_TAILER) && a.params.endsWith(filename)
          && a.appType.equals(appType)) {
        remove(a.id);
      }
    }
  }

  // ************************************************************************
  // command line utilities
  // ************************************************************************

  public static void main(String[] args) {
    ChukwaAgentController c = getClient(args);
    if (numArgs >= 3 && args[0].toLowerCase().equals("addfile")) {
      doAddFile(c, args[1], args[2]);
    } else if (numArgs >= 3 && args[0].toLowerCase().equals("removefile")) {
      doRemoveFile(c, args[1], args[2]);
    } else if (numArgs >= 1 && args[0].toLowerCase().equals("list")) {
      doList(c);
    } else if (numArgs >= 1 && args[0].equalsIgnoreCase("removeall")) {
      doRemoveAll(c);
    } else {
      System.err.println("usage: ChukwaClient addfile <apptype> <filename> [-h hostname] [-p portnumber]");
      System.err.println("       ChukwaClient removefile adaptorID [-h hostname] [-p portnumber]");
      System.err.println("       ChukwaClient removefile <apptype> <filename> [-h hostname] [-p portnumber]");
      System.err.println("       ChukwaClient list [IP] [port]");
      System.err.println("       ChukwaClient removeAll [IP] [port]");
    }
  }

  private static ChukwaAgentController getClient(String[] args) {
    int portno = 9093;
    String hostname = "localhost";

    numArgs = args.length;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-h") && args.length > i + 1) {
        hostname = args[i + 1];
        log.debug("Setting hostname to: " + hostname);
        numArgs -= 2; // subtract for the flag and value
      } else if (args[i].equals("-p") && args.length > i + 1) {
        portno = Integer.parseInt(args[i + 1]);
        log.debug("Setting portno to: " + portno);
        numArgs -= 2; // subtract for the flat, i.e. -p, and value
      }
    }
    return new ChukwaAgentController(hostname, portno);
  }

  private static String doAddFile(ChukwaAgentController c, String appType,
      String params) {
    log.info("Adding adaptor with filename: " + params);
    String adaptorID = c.addFile(appType, params);
    if (adaptorID != null) {
      log.info("Successfully added adaptor, id is:" + adaptorID);
    } else {
      System.err.println("Agent reported failure to add adaptor, adaptor id returned was:"
              + adaptorID);
    }
    return adaptorID;
  }

  private static void doRemoveFile(ChukwaAgentController c, String appType,
      String params) {
    try {
      log.debug("Removing adaptor with filename: " + params);
      c.removeFile(appType, params);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void doList(ChukwaAgentController c) {
    try {
      Iterator<Adaptor> adptrs = c.list().values().iterator();
      while (adptrs.hasNext()) {
        log.debug(adptrs.next().toString());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void doRemoveAll(ChukwaAgentController c) {
    log.info("Removing all adaptors");
    c.removeAll();
  }
}
