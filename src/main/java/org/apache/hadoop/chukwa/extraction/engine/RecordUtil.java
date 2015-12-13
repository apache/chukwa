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
package org.apache.hadoop.chukwa.extraction.engine;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.Chunk;

/**
 * Various utility methods.
 * 
 */
public class RecordUtil {
  static Pattern clusterPattern = Pattern
      .compile("(.*)?cluster=\"(.*?)\"(.*)?");

  public static String getClusterName(Record record) {
    String tags = record.getValue(Record.tagsField);
    if (tags != null) {
      Matcher matcher = clusterPattern.matcher(tags);
      if (matcher.matches()) {
        return matcher.group(2);
      }
    }

    return "undefined";
  }
  /**
   * Uses a precompiled pattern, so theoretically faster than
   * Chunk.getTag().
   * @param chunk 
   * @return 
   * 
   */
  public static String getClusterName(Chunk chunk) {
    String tags = chunk.getTags();
    if (tags != null) {
      Matcher matcher = clusterPattern.matcher(tags);
      if (matcher.matches()) {
        return matcher.group(2);
      }
    }

    return "undefined";
  }
  

}
