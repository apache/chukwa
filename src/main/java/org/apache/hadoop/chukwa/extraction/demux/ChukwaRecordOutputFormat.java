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

package org.apache.hadoop.chukwa.extraction.demux;


import org.apache.hadoop.chukwa.extraction.demux.processor.Util;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.RecordUtil;
import org.apache.hadoop.chukwa.util.HierarchyDataType;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.log4j.Logger;

public class ChukwaRecordOutputFormat extends
    MultipleSequenceFileOutputFormat<ChukwaRecordKey, ChukwaRecord> {
  static Logger log = Logger.getLogger(ChukwaRecordOutputFormat.class);

  @Override
  protected String generateFileNameForKeyValue(ChukwaRecordKey key,
      ChukwaRecord record, String name) {
    //CHUKWA-648:  Make Chukwa Reduce Type to support hierarchy format    
    //Allow the user to define hierarchy data-type separated by slash mark
    //Transform the reduceType from
    // "datatypeLevel1-datatypeLevel2-datatypeLevel3" to
    // "datatypeLevel1/datatypeLevel2/datatypeLevel3"
    String output = RecordUtil.getClusterName(record) + "/"
        + key.getReduceType() + "/"
        + HierarchyDataType.getHierarchyDataTypeDirectory(key.getReduceType())
        + Util.generateTimeOutput(record.getTime());

    // {log.info("ChukwaOutputFormat.fileName: [" + output +"]");}

    return output;
  }
}
