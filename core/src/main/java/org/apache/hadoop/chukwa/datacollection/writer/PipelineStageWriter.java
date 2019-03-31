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

package org.apache.hadoop.chukwa.datacollection.writer;


import java.io.IOException;
import java.util.List;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.log4j.Logger;

/**
 * A pipeline of Pipelineable writers
 * Controlled by option 'chukwaCollector.pipeline', which should be a comma-
 * separated list of classnames. 
 * 
 */
public class PipelineStageWriter implements ChukwaWriter {
  Logger log = Logger.getLogger(PipelineStageWriter.class);

  ChukwaWriter writer; // head of pipeline

  public PipelineStageWriter() throws WriterException {
    Configuration conf = new ChukwaConfiguration();
    init(conf);
  }
  
  public PipelineStageWriter(Configuration conf) throws WriterException {
    init(conf);
  }
  
  @Override
  public CommitStatus add(List<Chunk> chunks) throws WriterException {
    return writer.add(chunks);
  }

  @Override
  public void close() throws WriterException {
    writer.close();
  }

  @Override
  public void init(Configuration conf) throws WriterException {
    if (conf.get("chukwa.pipeline") != null) {
      String pipeline = conf.get("chukwa.pipeline");
      try {
        String[] classes = pipeline.split(",");
        log.info("using pipelined writers, pipe length is " + classes.length);
        PipelineableWriter lastWriter = null;
        if (classes.length > 1) {
          lastWriter = (PipelineableWriter) conf.getClassByName(classes[0])
              .newInstance();
          lastWriter.init(conf);
          writer = lastWriter;
        }

        for (int i = 1; i < classes.length - 1; ++i) {
          Class<?> stageClass = conf.getClassByName(classes[i]);
          Object st = stageClass.newInstance();
          if (!(st instanceof PipelineableWriter))
            log.error("class " + classes[i]
                + " in processing pipeline isn't a PipelineableWriter.");

          PipelineableWriter stage = (PipelineableWriter) stageClass
              .newInstance();
          stage.init(conf);
          // throws exception if types don't match or class not found; this is
          // OK.

          lastWriter.setNextStage(stage);
          lastWriter = stage;
        }
        // if authentication type is kerberos; login using the specified kerberos principal and keytab file
        for(int i=0; i<classes.length; i++) {
          if(classes[i].contains("HBaseWriter")) {
            try {
              loginToKerberos (conf);
            } catch(IOException e) {
              throw new WriterException("Unable to login to Kerberos.");
            }
          }
        }
        
        Class<?> stageClass = conf.getClassByName(classes[classes.length - 1]);
        Object st = stageClass.newInstance();

        if (!(st instanceof ChukwaWriter)) {
          log.error("class " + classes[classes.length - 1]
              + " at end of processing pipeline isn't a ChukwaWriter");
          throw new WriterException("bad pipeline");
        } else {
          ((ChukwaWriter)st).init(conf);
          if (lastWriter != null)
            lastWriter.setNextStage((ChukwaWriter) st);
          else
            writer = (ChukwaWriter) st; // one stage pipeline
        }
        return;
      } catch (WriterException | 
          ClassNotFoundException | 
          IllegalAccessException | 
          InstantiationException e) {
        // if anything went wrong (missing class, etc) we wind up here.
        log.error("failed to set up pipeline, defaulting to SeqFileWriter", e);
        // fall through to default case
        throw new WriterException("bad pipeline");
      }
    } else {
      throw new WriterException("must set chukwa.pipeline");
    }
  }
  
  /**
   * If authentication type is "kerberos", this method authenticates the Chukwa agent with Kerberized HBase, using the
   * Kerberos principal and keytab file specified in chukwa-agent-conf.xml config file.<br>
   * Does nothing for other authentication type.
   * 
   * @throws IOException in event of login failure
   */
  private static void loginToKerberos (Configuration config) throws IOException {
    String agentAuthType = config.get ("chukwaAgent.hadoop.authentication.type");
    if (null != agentAuthType && "kerberos".equalsIgnoreCase (agentAuthType)) {
      SecurityUtil.login (config, "chukwaAgent.hadoop.authentication.kerberos.keytab",
        "chukwaAgent.hadoop.authentication.kerberos.principal");
    }
  }

}
