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
package org.apache.hadoop.chukwa.rest.resource;

import org.json.simple.JSONObject;

import org.apache.hadoop.chukwa.rest.bean.ReturnCodeBean;
import org.apache.hadoop.chukwa.rest.bean.UserBean;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.json.simple.JSONArray;

import com.sun.jersey.api.client.Client;

public class TestUserResource extends SetupTestEnv {
  public void testUserSave() {
    try {
    UserBean user = new UserBean();
    user.setId("admin");
    user.setProperty("testKey", "testValue");
    JSONArray ja = new JSONArray();
    user.setViews(ja);
    client = Client.create();
    resource = client.resource("http://localhost:"+restPort);
    ReturnCodeBean result = resource.path("/hicc/v1/user").
    header("Content-Type","application/json").
    header("Authorization", authorization).
    put(ReturnCodeBean.class, user);
    assertEquals(1, result.getCode());
    } catch (Exception e) {
      fail(ExceptionUtil.getStackTrace(e));
    }
  }
  
  public void testUserLoad() {
    client = Client.create();
    resource = client.resource("http://localhost:"+restPort);
    UserBean user = resource.path("/hicc/v1/user/uid/admin").header("Authorization", authorization).get(UserBean.class);
    try {
      assertEquals("admin", user.getId());
      assertEquals("testValue", user.getPropertyValue("testKey"));
    } catch (Exception e) {
      fail(ExceptionUtil.getStackTrace(e));
    }
  }
}
