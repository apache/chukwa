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

import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestTimePartition extends TestCase {

  public void test_5sec_TimePartition() {
    TimePartition func = new TimePartition("" + (5*1000L));
    
    Long timestamp = 1243377169372L;
    Tuple input = DefaultTupleFactory.getInstance().newTuple(timestamp);

    try {
      Long timePartition = func.exec(input);
      long expectedTimePartition = 1243377165000L;
      Assert.assertTrue(timePartition.longValue() == expectedTimePartition);
    } catch (IOException e) {
      Assert.fail();
    }
  }
  
  
  public void test_5Min_TimePartition() {
    TimePartition func = new TimePartition("" + (5*60*1000L));
    
    Long timestamp = 1243377169372L;
    Tuple input = DefaultTupleFactory.getInstance().newTuple(timestamp);

    try {
      Long timePartition = func.exec(input);
      long expectedTimePartition = 1243377000000L;
      Assert.assertTrue(timePartition.longValue() == expectedTimePartition);
    } catch (IOException e) {
      Assert.fail();
    }
  }
  
  public void test_60Min_TimePartition() {
    TimePartition func = new TimePartition("" + (60*60*1000L));
    
    Long timestamp = 1243377169372L;
    Tuple input = DefaultTupleFactory.getInstance().newTuple(timestamp);

    try {
      Long timePartition = func.exec(input);
      long expectedTimePartition = 1243375200000L;
      Assert.assertTrue(timePartition.longValue() == expectedTimePartition);
    } catch (IOException e) {
      Assert.fail();
    }
  }
  
  public void test_1Day_TimePartition() {
    TimePartition func = new TimePartition("" + (24*60*60*1000L));
    
    Long timestamp = 1243377169372L;
    Tuple input = DefaultTupleFactory.getInstance().newTuple(timestamp);

    try {
      Long timePartition = func.exec(input);
      long expectedTimePartition = 1243296000000L;
      Assert.assertTrue(timePartition.longValue() == expectedTimePartition);
    } catch (IOException e) {
      Assert.fail();
    }
  }
 
  public void test_largeTimePartition() {
    try {
      TimePartition func = new TimePartition("7776000000" );
      Long timestamp = 1243377169372L;
      Tuple input = DefaultTupleFactory.getInstance().newTuple(timestamp);
      Long timePartition = func.exec(input);
    } catch (Throwable e) {
      Assert.fail();
    }
  }
  
}
