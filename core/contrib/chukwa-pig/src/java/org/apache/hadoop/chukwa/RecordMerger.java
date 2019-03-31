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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * return time,Map
 * 
 */
public class RecordMerger extends EvalFunc<Map<String,Object>> {
  
  @SuppressWarnings("unchecked")
  @Override
  public Map<String,Object> exec(Tuple input) throws IOException {
    
    Map<String, Object> newPigMapFields = new HashMap<String, Object>();
    DataBag bg = (DataBag) input.get(0);
    Iterator<Tuple> bagIterator = bg.iterator();
    Object s = null;
    while (bagIterator.hasNext()) {

      Map<Object, Object> map = (Map<Object,Object>) bagIterator.next().get(0);
      Iterator<Object> mapIterator = map.keySet().iterator();
      while (mapIterator.hasNext()) {
        s = mapIterator.next();
        newPigMapFields.put(s.toString(), map.get(s));
      }
    }

    return newPigMapFields;
  }

  @Override
  public Schema outputSchema(Schema input) {
    Schema schema = null;
    try {
      schema = new Schema(new Schema.FieldSchema(input.getField(0).alias, DataType.MAP));
    } catch (FrontendException e) {
      e.printStackTrace();
    }
    return schema;
  }
}
