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

package org.apache.hadoop.chukwa;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;

public class ChunkImpl implements org.apache.hadoop.io.Writable, Chunk {
  public final static int PROTOCOL_VERSION = 1;

  protected DataFactory dataFactory = DataFactory.getInstance();
  private String source = "";
  private String streamName = "";
  private String dataType = "";
  private String tags = "";
  private byte[] data = null;
  private int[] recordEndOffsets;
  private int protocolVersion = 1;
  private String debuggingInfo = "";

  private transient Adaptor initiator;
  long seqID;

  private static String localHostAddr;
  static {
    try {
      setHostAddress(InetAddress.getLocalHost().getHostName().toLowerCase());
    } catch (UnknownHostException e) {
      setHostAddress("localhost");
    }
  }
  
  public static void setHostAddress(String host) {
    ChunkImpl.localHostAddr = host;
  }
  
  
  public static ChunkImpl getBlankChunk() {
    return new ChunkImpl();
  }

  ChunkImpl() {
  }

  public ChunkImpl(String dataType, String streamName, long seq, byte[] data,
                   Adaptor source) {
    this.seqID = seq;
    this.source = localHostAddr;
    this.tags = dataFactory.getDefaultTags();
    this.streamName = streamName;
    this.dataType = dataType;
    this.data = (byte[]) data.clone();
    this.initiator = source;
  }

  /**
   * @see org.apache.hadoop.chukwa.Chunk#getData()
   */
  public byte[] getData() {
    return data.clone();
  }

  /**
   * @see org.apache.hadoop.chukwa.Chunk#setData(byte[])
   */
  public void setData(byte[] logEvent) {
    this.data = (byte[]) logEvent.clone();
  }

  /**
   * @see org.apache.hadoop.chukwa.Chunk#getStreamName()
   */
  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(String logApplication) {
    this.streamName = logApplication;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String logSource) {
    this.source = logSource;
  }

  public String getDebugInfo() {
    return debuggingInfo;
  }

  public void setDebugInfo(String a) {
    this.debuggingInfo = a;
  }

  /**
   * @see org.apache.hadoop.chukwa.Chunk#getSeqID()
   */
  public long getSeqID() {
    return seqID;
  }

  public void setSeqID(long l) {
    seqID = l;
  }

  public int getProtocolVersion() {
    return protocolVersion;
  }

  public void setProtocolVersion(int pv) {
    this.protocolVersion = pv;
  }

  public Adaptor getInitiator() {
    return initiator;
  }

  public void setInitiator(Adaptor a) {
    initiator = a;
  }

  public void setLogSource() {
    source = localHostAddr;
  }

  public int[] getRecordOffsets() {
    if (recordEndOffsets == null)
      recordEndOffsets = new int[] { data.length - 1 };
    return recordEndOffsets.clone();
  }

  public void setRecordOffsets(int[] offsets) {
    recordEndOffsets = (int[]) offsets.clone();
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String t) {
    dataType = t;
  }

  @Override
  public void addTag(String tags) {
    this.tags += " "+ tags;
  }

  /**
   * @see org.apache.hadoop.chukwa.Chunk#getTags()
   */
  public String getTags() {
    return tags;
  }
  
  /**
   * @see org.apache.hadoop.chukwa.Chunk#getTag(java.lang.String)
   */
  public String getTag(String tagName) {
    Pattern tagPattern = Pattern.compile("\\b"+tagName+"=\"([^\"]*)\"");
    if (tags != null) {
      Matcher matcher = tagPattern.matcher(tags);
      if (matcher.find()) {
        return matcher.group(1);
      }
    }
    return null;
  }
  
  /**
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    setProtocolVersion(in.readInt());
    if (protocolVersion != PROTOCOL_VERSION) {
      throw new IOException(
          "Protocol version mismatched, drop data.  source version: "
              + protocolVersion + ", collector version:" + PROTOCOL_VERSION);
    }
    setSeqID(in.readLong());
    setSource(in.readUTF());
    tags = in.readUTF(); // no public set method here
    setStreamName(in.readUTF());
    setDataType(in.readUTF());
    setDebugInfo(in.readUTF());

    int numRecords = in.readInt();
    recordEndOffsets = new int[numRecords];
    for (int i = 0; i < numRecords; ++i)
      recordEndOffsets[i] = in.readInt();
    data = new byte[recordEndOffsets[recordEndOffsets.length - 1] + 1];
    in.readFully(data);

  }

  /**
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(PROTOCOL_VERSION);
    out.writeLong(seqID);
    out.writeUTF(source);
    out.writeUTF(tags);
    out.writeUTF(streamName);
    out.writeUTF(dataType);
    out.writeUTF(debuggingInfo);

    if (recordEndOffsets == null)
      recordEndOffsets = new int[] { data.length - 1 };

    out.writeInt(recordEndOffsets.length);
    for (int i = 0; i < recordEndOffsets.length; ++i)
      out.writeInt(recordEndOffsets[i]);

    out.write(data, 0, recordEndOffsets[recordEndOffsets.length - 1] + 1); 
    // byte at last offset is valid
  }

  public static ChunkImpl read(DataInput in) throws IOException {
    ChunkImpl w = new ChunkImpl();
    w.readFields(in);
    return w;
  }

  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(source);
    buffer.append(":");
    buffer.append(streamName);
    buffer.append(new String(data, Charset.forName("UTF-8")));
    buffer.append("/");
    buffer.append(seqID);
    return buffer.toString();
  }



  /**
   * @see org.apache.hadoop.chukwa.Chunk#getSerializedSizeEstimate()
   */
  public int getSerializedSizeEstimate() {
    int size = 2 * (source.length() + streamName.length() + dataType.length() 
        + debuggingInfo.length()); // length of strings (pessimistic)
    size += data.length + 4;
    if (recordEndOffsets == null)
      size += 8;
    else
      size += 4 * (recordEndOffsets.length + 1); // +1 for length of array
    size += 8; // uuid
    return size;
  }

  public void setRecordOffsets(java.util.Collection<Integer> carriageReturns) {
    recordEndOffsets = new int[carriageReturns.size()];
    int i = 0;
    for (Integer offset : carriageReturns)
      recordEndOffsets[i++] = offset;
  }
  
  public int getLength() {
    return data.length;
  }

}
