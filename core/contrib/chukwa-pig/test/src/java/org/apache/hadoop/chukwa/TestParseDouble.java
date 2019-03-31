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

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;


public class TestParseDouble extends TestCase {

  public void testPARSEDOUBLE() {
    PARSEDOUBLE func = new PARSEDOUBLE();
    String in = "10";
    Double inDouble = Double.parseDouble(in);
    Tuple input = DefaultTupleFactory.getInstance().newTuple(in);

    Double output = null;;
    try {
      output = func.exec(input);
      Assert.assertTrue(output.doubleValue() == inDouble.doubleValue());
    } catch (IOException e) {
      Assert.fail();
    }
  }

  public void testPARSEDOUBLE2() {
    PARSEDOUBLE func = new PARSEDOUBLE();
    String in = "10.86";
    Double inDouble = Double.parseDouble(in);
    Tuple input = DefaultTupleFactory.getInstance().newTuple(in);

    Double output = null;;
    try {
      output = func.exec(input);
      Assert.assertTrue(output.doubleValue() == inDouble.doubleValue());
    } catch (IOException e) {
      Assert.fail();
    }
  }
  
  
  public void testPARSEDOUBLE3() {
    PARSEDOUBLE func = new PARSEDOUBLE();
    String in = "10aaa";
   
    Double output = null;;
    try {
      Tuple input = DefaultTupleFactory.getInstance().newTuple(in);
      output = func.exec(input);
      Assert.assertNull(output);
    } catch (IOException e) {
      Assert.fail();
    }
  }
  
  public void testPARSEDOUBLE4() {
    PARSEDOUBLE func = new PARSEDOUBLE();
    String in = "10.86";
    Double inDouble = Double.parseDouble(in);
    Tuple input = DefaultTupleFactory.getInstance().newTuple(new DataByteArray(in));

    Double output = null;;
    try {
      output = func.exec(input);
      Assert.assertTrue(output.doubleValue() == inDouble.doubleValue());
    } catch (IOException e) {
      Assert.fail();
    }
  }
  
}
