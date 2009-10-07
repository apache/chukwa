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
package org.apache.hadoop.chukwa.util;

import junit.framework.TestCase;
import org.apache.hadoop.chukwa.ChunkImpl;

public class TestFilter extends TestCase {
  
  public void testBasicPatternMatching()  {
   try {
     Filter rules = new Filter("host=foo.*&cluster=bar&datatype=Data");
     assertEquals(3, rules.size());
     byte[] dat = "someText".getBytes();
     ChunkImpl chunkNone = new ChunkImpl("badData","aname", dat.length, dat, null);
     assertFalse(rules.matches(chunkNone));
     assertTrue(Filter.ALL.matches(chunkNone));


       //do the right thing on a non-match
     ChunkImpl chunkSome = new ChunkImpl("badData", "aname", dat.length, dat, null);
     chunkSome.setSource("fooly");
     chunkSome.addTag("cluster=\"bar\"");
     assertFalse(rules.matches( chunkSome));
     assertTrue(Filter.ALL.matches(chunkSome));

     ChunkImpl chunkAll = new ChunkImpl("Data", "aname", dat.length, dat, null);
     chunkAll.setSource("fooly");
     chunkAll.addTag("cluster=\"bar\"");

     assertTrue(rules.matches(chunkAll));
     assertTrue(Filter.ALL.matches(chunkAll));

     
       //check that we match content correctly
     rules = new Filter("content=someText");
     assertTrue(rules.matches(chunkAll));
     rules = new Filter("content=some");
     assertFalse(rules.matches( chunkAll));
     rules = new Filter("datatype=Data&content=.*some.*");
     assertTrue(rules.matches( chunkAll));

   } catch(Exception e) {
     fail("exception " + e);
   } 
  }
  
  public void testClusterPatterns() {
    byte[] dat = "someText".getBytes();
    ChunkImpl chunk1 = new ChunkImpl("Data", "aname", dat.length, dat, null);
    chunk1.setSource("asource");
    assertTrue(Filter.ALL.matches(chunk1));
    Filter rule = new Filter("tags.foo=bar");
    
    assertFalse(rule.matches(chunk1));
    chunk1.addTag("foo=\"bar\"");
    assertTrue(rule.matches(chunk1));
    chunk1.addTag("baz=\"quux\"");
    assertTrue(rule.matches(chunk1));
    assertTrue(Filter.ALL.matches(chunk1));
  }
  
}
