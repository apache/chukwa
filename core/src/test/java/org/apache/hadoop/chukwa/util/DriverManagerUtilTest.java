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

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.chukwa.util.DriverManagerUtil.ConnectionInfo;

import junit.framework.TestCase;

public class DriverManagerUtilTest extends TestCase {

  public void testLoadDriver() throws ClassNotFoundException {
    Class<?> clazz = DriverManagerUtil.loadDriver();
    System.out.println(clazz);
  }

  public void testGetConnectionInfo() {
    {
      String url = "jdbc:mysql://localhost:3306/demo";
      ConnectionInfo ci = new ConnectionInfo(url);
      assertEquals("jdbc:mysql://localhost:3306/demo", ci.getUri());
      assertEquals(0, ci.getProperties().size());
    }
    
    {
      String url = "jdbc:mysql://localhost:3306/demo?user=example";
      ConnectionInfo ci = new ConnectionInfo(url);
      assertEquals("jdbc:mysql://localhost:3306/demo", ci.getUri());
      assertEquals(1, ci.getProperties().size());
      assertEquals("example", ci.getProperties().get("user"));
    }
    
    {
      String url = "jdbc:mysql://localhost:3306/demo?user=example&";
      ConnectionInfo ci = new ConnectionInfo(url);
      assertEquals("jdbc:mysql://localhost:3306/demo", ci.getUri());
      assertEquals(1, ci.getProperties().size());
      assertEquals("example", ci.getProperties().get("user"));
    }
    
    {
      String url = "jdbc:mysql://localhost:3306/demo?user=example&pwd";
      ConnectionInfo ci = new ConnectionInfo(url);
      assertEquals("jdbc:mysql://localhost:3306/demo", ci.getUri());
      assertEquals(2, ci.getProperties().size());
      assertEquals("example", ci.getProperties().get("user"));
      assertEquals("", ci.getProperties().get("pwd"));
    }
    
    {
      String url = "jdbc:mysql://localhost:3306/demo?user=example&pwd=";
      ConnectionInfo ci = new ConnectionInfo(url);
      assertEquals("jdbc:mysql://localhost:3306/demo", ci.getUri());
      assertEquals(2, ci.getProperties().size());
      assertEquals("example", ci.getProperties().get("user"));
      assertEquals("", ci.getProperties().get("pwd"));
    }
    
    {
      String url = "jdbc:mysql://localhost:3306/demo?user=example&pwd=ppppp";
      ConnectionInfo ci = new ConnectionInfo(url);
      assertEquals("jdbc:mysql://localhost:3306/demo", ci.getUri());
      assertEquals(2, ci.getProperties().size());
      assertEquals("example", ci.getProperties().get("user"));
      assertEquals("ppppp", ci.getProperties().get("pwd"));
    }
  }

  public void testGetConnection() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
    if(false) {
      DriverManagerUtil.loadDriver().newInstance();
      String url = "jdbc:mysql://localhost:3306/test?user=root&pwd=";
      Connection conn = DriverManagerUtil.getConnection(url);
    }
  } 
}
