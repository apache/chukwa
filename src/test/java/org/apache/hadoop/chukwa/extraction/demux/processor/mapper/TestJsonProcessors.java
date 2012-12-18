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
package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import junit.framework.TestCase;

public class TestJsonProcessors extends TestCase {

	/**
	 * Process the chunk with the passed Processor and compare with the input
	 * JSONObject
	 * 
	 * @param p
	 * @param inData
	 * @param chunk
	 * @return
	 */
	private String testProcessor(AbstractProcessor p, JSONObject inData,
			Chunk chunk) {
		ChukwaTestOutputCollector<ChukwaRecordKey, ChukwaRecord> output = new ChukwaTestOutputCollector<ChukwaRecordKey, ChukwaRecord>();
		p.process(new ChukwaArchiveKey(), chunk, output, null);
		HashMap<ChukwaRecordKey, ChukwaRecord> outData = output.data;

		// First get all ChukwaRecords and then get all field-data pairs within
		// each record
		Iterator<Entry<ChukwaRecordKey, ChukwaRecord>> recordIter = outData
				.entrySet().iterator();
		while (recordIter.hasNext()) {
			Entry<ChukwaRecordKey, ChukwaRecord> recordEntry = recordIter
					.next();
			ChukwaRecord value = recordEntry.getValue();
			String[] fields = value.getFields();
			for (String field : fields) {
				//ignore ctags
				if(field.equals("ctags")) {
					continue;
				}
				String data = value.getValue(field);
				String expected = String.valueOf(inData.get(field));
				/*System.out.println("Metric, expected data, received data- " +
				 field + ", " + expected + ", " +data);
				*/
				if (!expected.equals(data)) {
					StringBuilder sb = new StringBuilder(
							"Failed to verify metric - ");
					sb.append("field:").append(field);
					sb.append(", expected:").append(expected);
					sb.append(", but received:").append(data);
					return sb.toString();
				}
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private JSONObject getJSONObject(){
		String csource = "localhost";
		try {
			csource = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			csource = "localhost";
		}
		JSONObject json = new JSONObject();
		json.put("capp", "Test");
		json.put("csource", csource);
		return json;
	}

	@SuppressWarnings("unchecked")
	public void testJobTrackerProcessor() {
		// test metric for each record type
		JSONObject json = getJSONObject();
		json.put("memHeapUsedM", "286");
		json.put("maps_killed", "3");
		json.put("waiting_maps", "1");
		json.put("RpcProcessingTime_avg_time", "0.003");
		byte[] data = json.toString().getBytes();
		JobTrackerProcessor p = new JobTrackerProcessor();
		ChunkImpl ch = new ChunkImpl("TestType", "Test", data.length, data,
				null);
		String failMsg = testProcessor(p, json, ch);
		assertNull(failMsg, failMsg);

		// test gauge metric
		json.put("maps_killed", "5");
		data = json.toString().getBytes();
		ch = new ChunkImpl("TestType", "Test", data.length, data, null);
		json.put("maps_killed", "2");
		failMsg = testProcessor(p, json, ch);
		assertNull(failMsg, failMsg);
	}

	@SuppressWarnings("unchecked")
	public void testNamenodeProcessor() {
		// test metric for each record type
		JSONObject json = getJSONObject();
		json.put("BlocksTotal", "1234");
		json.put("FilesCreated", "33");
		json.put("RpcQueueTime_avg_time", "0.001");
		json.put("gcCount", "112");
		json.put("Transactions_num_ops", "3816");
		byte[] data = json.toString().getBytes();
		NamenodeProcessor p = new NamenodeProcessor();
		ChunkImpl ch = new ChunkImpl("TestType", "Test", data.length, data,
				null);
		String failMsg = testProcessor(p, json, ch);
		assertNull(failMsg, failMsg);

		// test gauge metric
		json.put("FilesCreated", "55");
		json.put("gcCount", "115");
		data = json.toString().getBytes();
		ch = new ChunkImpl("TestType", "Test", data.length, data, null);
		json.put("FilesCreated", "22");
		json.put("gcCount", "3");
		failMsg = testProcessor(p, json, ch);
		assertNull(failMsg, failMsg);
	}

	@SuppressWarnings("unchecked")
	public void testDatanodeProcessor() {
		// test metric for each record type
		JSONObject json = getJSONObject();
		json.put("heartBeats_num_ops", "10875");
		json.put("FilesCreated", "33");
		json.put("RpcQueueTime_avg_time", "0.001");
		json.put("gcCount", "112");
		json.put("Capacity", "22926269645");
		byte[] data = json.toString().getBytes();
		DatanodeProcessor p = new DatanodeProcessor();
		ChunkImpl ch = new ChunkImpl("TestType", "Test", data.length, data,
				null);
		String failMsg = testProcessor(p, json, ch);
		assertNull(failMsg, failMsg);

		// test gauge metric
		json.put("heartBeats_num_ops", "10980");
		json.put("gcCount", "115");
		data = json.toString().getBytes();
		ch = new ChunkImpl("TestType", "Test", data.length, data, null);
		json.put("heartBeats_num_ops", "105");
		json.put("gcCount", "3");
		failMsg = testProcessor(p, json, ch);
		assertNull(failMsg, failMsg);
	}

	@SuppressWarnings("unchecked")
	public void testHBaseMasterProcessor() {
		// test metric for each record type
		JSONObject json = getJSONObject();
		json.put("splitSizeNumOps", "108");
		json.put("AverageLoad", "3.33");
		byte[] data = json.toString().getBytes();
		HBaseMasterProcessor p = new HBaseMasterProcessor();
		ChunkImpl ch = new ChunkImpl("TestType", "Test", data.length, data,
				null);
		String failMsg = testProcessor(p, json, ch);
		assertNull(failMsg, failMsg);

		// test gauge metric
		json.put("splitSizeNumOps", "109");
		data = json.toString().getBytes();
		ch = new ChunkImpl("TestType", "Test", data.length, data, null);
		json.put("splitSizeNumOps", "1");
		failMsg = testProcessor(p, json, ch);
		assertNull(failMsg, failMsg);
	}

	@SuppressWarnings("unchecked")
	public void testHBaseRegionServerProcessor() {
		// test metric for each record type
		JSONObject json = getJSONObject();
		json.put("blockCacheSize", "2681872");
		byte[] data = json.toString().getBytes();
		HBaseMasterProcessor p = new HBaseMasterProcessor();
		ChunkImpl ch = new ChunkImpl("TestType", "Test", data.length, data,
				null);
		String failMsg = testProcessor(p, json, ch);
		assertNull(failMsg, failMsg);
		// no gauge metrics yet
	}

	@SuppressWarnings("unchecked")
	public void testZookeeperProcessor() {
		// test metric for each record type
		JSONObject json = getJSONObject();
		json.put("packetsSent", "2049");
		json.put("NodeCount", "40");
		byte[] data = json.toString().getBytes();
		ZookeeperProcessor p = new ZookeeperProcessor();
		ChunkImpl ch = new ChunkImpl("TestType", "Test", data.length, data,
				null);
		String failMsg = testProcessor(p, json, ch);
		assertNull(failMsg, failMsg);

		// test gauge metric
		json.put("packetsSent", "2122");
		data = json.toString().getBytes();
		ch = new ChunkImpl("TestType", "Test", data.length, data, null);
		json.put("packetsSent", "73");
		failMsg = testProcessor(p, json, ch);
		assertNull(failMsg, failMsg);
	}
}

