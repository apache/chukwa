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

package org.apache.hadoop.chukwa.hicc;


import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

import org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore;

public class ClusterConfig {
  private Set<String> clusterMap = null;

  static public String getContents(File aFile) {
    // ...checks on aFile are elided
    StringBuffer contents = new StringBuffer();

    try {
      BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream(aFile.getAbsolutePath()), Charset.forName("UTF-8")));
      try {
        String line = null; // not declared within while loop
        /*
         * readLine is a bit quirky : it returns the content of a line MINUS the
         * newline. it returns null only for the END of the stream. it returns
         * an empty String if two newlines appear in a row.
         */
        while ((line = input.readLine()) != null) {
          contents.append(line);
          contents.append(System.getProperty("line.separator"));
        }
      } finally {
        input.close();
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }

    return contents.toString();
  }

  public ClusterConfig() {
    long end = System.currentTimeMillis();
    long start = end - 3600000L;
    if(clusterMap==null) {
      clusterMap = ChukwaHBaseStore.getClusterNames(start, end);
    }
  }

  public Iterator<String> getClusters() {
    Iterator<String> i = clusterMap.iterator();
    return i;
  }
}
