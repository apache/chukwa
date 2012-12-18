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

package org.apache.hadoop.chukwa.datacollection.adaptor.JMX;

import java.util.Date;
import java.util.Map;
import java.util.Queue;

public class MXBeanImpl implements MXBean {

	private Queue<String> q;
	private int i;
	private String s;
	private String[] sarray;
	private Map<Integer, String> m;
	
	public MXBeanImpl() {
		q = null;
		i = -1;
		s = null;
		sarray = null;
		m = null;
	}
	
	public void setQueue(Queue<String> queue){
		this.q = queue;
	}
	
	public void setInt(int i) {
		this.i = i;
	}
	
	public void setString(String s) {
		this.s = s;
	}
	
	public void setStringArray(String[] sarray) {
		this.sarray = sarray;
	}
	
	public void setMap(Map<Integer, String> m) {
		this.m = m;
	}
	
	public int getInt() {
		return i;
	}

	public String getString() {
		return s;
	}

	public String[] getStringArray() {
		return sarray;
	}

	public Map<Integer, String> getMap() {
		return m;
	}

	public QueueSample getCompositeType() {
		synchronized(q) {
			return new QueueSample(new Date(), q.size(), q.peek());
		}
	}
	
}
