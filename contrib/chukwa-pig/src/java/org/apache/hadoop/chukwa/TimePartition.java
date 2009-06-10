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

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class TimePartition extends EvalFunc<Long>{

  protected long period = 0;
  
  public TimePartition(String strPeriod) {
    period = Long.parseLong(strPeriod);
  }
 
  @Override
  public Long exec(Tuple input) throws IOException {
    if (input == null || input.size() < 1)
      return null;
    try {
      long timestamp = Long.parseLong(input.get(0).toString());
      timestamp =  timestamp - (timestamp % (period));
      
      return timestamp;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public Schema outputSchema(Schema input) {
    Schema schema = null;
    try {
      schema = new Schema(new Schema.FieldSchema(input.getField(0).alias, DataType.LONG));
    } catch (FrontendException e) {
      schema = new Schema(new Schema.FieldSchema(getSchemaName("timePartition", input),
          DataType.LONG));
    }
    return schema;
  }

}
