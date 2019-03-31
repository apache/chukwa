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
import java.util.Map;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.TupleFactory;

import junit.framework.Assert;
import junit.framework.TestCase;


public class TestRecordMerger extends TestCase{

  @SuppressWarnings("unchecked")
  public void testRecordMerger() {
    RecordMerger func = new RecordMerger();

    try {
      Map in = new HashMap<String, String>();
      TupleFactory tf = TupleFactory.getInstance();

      in.put("A", new  DataByteArray("100"));
      in.put("B", new  DataByteArray("200"));
      in.put("C", new  DataByteArray("300"));
      
      Map in2 = new HashMap<String, String>();
      
      in2.put("D", new  DataByteArray("400"));
      in2.put("E", new  DataByteArray("500"));
      
      DataBag bg =  DefaultBagFactory.getInstance().newDefaultBag();
      bg.add(tf.newTuple(in));
      bg.add(tf.newTuple(in2));
      
      Map output =  func.exec( tf.newTuple(bg) );
      
      Assert.assertTrue(output.containsKey("A") );
      Assert.assertTrue(output.containsKey("B") );
      Assert.assertTrue(output.containsKey("C") );
      Assert.assertTrue(output.containsKey("D") );
      Assert.assertTrue(output.containsKey("E") );
      
      Assert.assertTrue(output.get("A").toString().equals("100") );
      Assert.assertTrue(output.get("B").toString().equals("200") );
      Assert.assertTrue(output.get("C").toString().equals("300") );
      Assert.assertTrue(output.get("D").toString().equals("400") );
      Assert.assertTrue(output.get("E").toString().equals("500") );
      
      
      
    } catch (IOException e) {
      Assert.fail();
    }
    
  }
}
