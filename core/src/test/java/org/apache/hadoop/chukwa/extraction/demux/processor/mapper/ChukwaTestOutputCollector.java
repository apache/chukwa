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


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.mapred.OutputCollector;

public class ChukwaTestOutputCollector<K, V> implements OutputCollector<K, V> {
  public HashMap<K, V> data = new HashMap<K, V>();

  public void collect(K key, V value) throws IOException {
    data.put(key, value);
  }

  @Override
  public String toString() {
    Iterator<K> it = data.keySet().iterator();
    K key = null;
    V value = null;
    StringBuilder sb = new StringBuilder();

    while (it.hasNext()) {
      key = it.next();
      value = data.get(key);
      sb.append("Key[").append(key).append("] value[").append(value).append(
          "]\n");
    }
    return sb.toString();
  }

}
