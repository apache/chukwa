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

public class TestXSSFilter extends TestCase {

  public void testFilter() {
    XssFilter xss = new XssFilter();
    String xssTest = "<XSS>";
    String xssFiltered = xss.filter(xssTest);
    assertEquals(xssFiltered, "");
    xssTest = "\'\';!--\"<XSS>=&{()}";
    xssFiltered = xss.filter(xssTest);
    assertEquals(xssFiltered, "&amp;{()}");
    xssTest = "<IMG \"\"\"><SCRIPT>alert(\"XSS\")</SCRIPT>\">";
    xssFiltered = xss.filter(xssTest);
    assertEquals(xssFiltered, "%3Cimg /%3Ealert(%22XSS%22)");
  }
}
