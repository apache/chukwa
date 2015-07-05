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
package org.apache.hadoop.chukwa.datacollection.writer.parquet;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.PipelineableWriter;
import org.apache.hadoop.chukwa.datacollection.writer.WriterException;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ChukwaParquetWriter extends PipelineableWriter {
  private static Logger LOG = Logger.getLogger(ChukwaParquetWriter.class);
  public static final String OUTPUT_DIR_OPT= "chukwaCollector.outputDir";
  private int blockSize = 64 * 1024 * 1024;
  private int pageSize = 64 * 1024;
  private Schema avroSchema = null;
  private AvroParquetWriter<GenericRecord> parquetWriter = null;
  protected String outputDir = null;
  private Calendar calendar = Calendar.getInstance();
  private String localHostAddr = null;
  private long rotateInterval = 300000L;
  private long startTime = 0;
  private Path previousPath = null;
  private String previousFileName = null;
  private FileSystem fs = null;
  
  public ChukwaParquetWriter() throws WriterException {
    this(ChukwaAgent.getStaticConfiguration());
  }

  public ChukwaParquetWriter(Configuration c) throws WriterException {
    setup(c);
  }

  @Override
  public void init(Configuration c) throws WriterException {
  }

  private void setup(Configuration c) throws WriterException {
    try {
      localHostAddr = "_" + InetAddress.getLocalHost().getHostName() + "_";
    } catch (UnknownHostException e) {
      localHostAddr = "-NA-";
    }
    outputDir = c.get(OUTPUT_DIR_OPT, "/chukwa/logs");
    blockSize = c.getInt("dfs.blocksize", 64 * 1024 * 1024);
    rotateInterval = c.getLong("chukwaCollector.rotateInterval", 300000L);
    if(fs == null) {
      try {
        fs = FileSystem.get(c);
      } catch (IOException e) {
        throw new WriterException(e);
      }
    }

    String input = "{\"namespace\": \"chukwa.apache.org\"," +
      "\"type\": \"record\"," +
      "\"name\": \"Chunk\"," +
      "\"fields\": [" +
          "{\"name\": \"dataType\", \"type\": \"string\"}," +
          "{\"name\": \"data\", \"type\": \"bytes\"}," +
          "{\"name\": \"source\", \"type\": \"string\"}," +
          "{\"name\": \"stream\", \"type\": \"string\"}," +
          "{\"name\": \"tags\", \"type\": \"string\"}," +
          "{\"name\": \"seqId\",  \"type\": [\"long\", \"null\"]}" +
      "]"+
     "}";

    // load your Avro schema
    avroSchema = new Schema.Parser().parse(input);
    // generate the corresponding Parquet schema
    rotate();
  }

  @Override
  public void close() throws WriterException {
    try {
      parquetWriter.close();
      fs.rename(previousPath, new Path(previousFileName + ".done"));
    } catch (IOException e) {
      throw new WriterException(e);
    }
  }

  @Override
  public CommitStatus add(List<Chunk> chunks) throws WriterException {
    long elapsedTime = 0;
    CommitStatus rv = ChukwaWriter.COMMIT_OK;
    for(Chunk chunk : chunks) {
      try {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("dataType", chunk.getDataType());
        record.put("data", ByteBuffer.wrap(chunk.getData()));
        record.put("tags", chunk.getTags());
        record.put("seqId", chunk.getSeqID());
        record.put("source", chunk.getSource());
        record.put("stream", chunk.getStreamName());
        parquetWriter.write(record);
        elapsedTime = System.currentTimeMillis() - startTime;
        if(elapsedTime > rotateInterval) {
          rotate();
        }
      } catch (IOException e) {
        LOG.warn("Failed to store data to HDFS.");
        LOG.warn(ExceptionUtil.getStackTrace(e));
      }
    }
    if (next != null) {
      rv = next.add(chunks); //pass data through
    }
    return rv;
  }
  
  private void rotate() throws WriterException {
    if(parquetWriter!=null) {
      try {
        parquetWriter.close();
        fs.rename(previousPath, new Path(previousFileName + ".done"));
      } catch (IOException e) {
        LOG.warn("Fail to close Chukwa write ahead log.");
      }
    }
    startTime = System.currentTimeMillis();
    calendar.setTimeInMillis(startTime);

    String newName = new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS")
        .format(calendar.getTime());
    newName += localHostAddr + new java.rmi.server.UID().toString();
    newName = newName.replace("-", "");
    newName = newName.replace(":", "");
    newName = newName.replace(".", "");
    newName = outputDir + "/" + newName.trim();
    LOG.info("writing: "+newName);
    Path path = new Path(newName);
    try {
      parquetWriter = new AvroParquetWriter<GenericRecord>(path, avroSchema, CompressionCodecName.SNAPPY, blockSize, pageSize);
      previousPath = path;
      previousFileName = newName;
    } catch (IOException e) {
      throw new WriterException(e);
    }
  }
}
