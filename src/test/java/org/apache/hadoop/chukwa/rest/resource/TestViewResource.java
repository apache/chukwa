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

import javax.ws.rs.core.MultivaluedMap;

import org.apache.hadoop.chukwa.hicc.HiccWebServer;
import org.apache.hadoop.chukwa.rest.bean.ColumnBean;
import org.apache.hadoop.chukwa.rest.bean.PagesBean;
import org.apache.hadoop.chukwa.rest.bean.ReturnCodeBean;
import org.apache.hadoop.chukwa.rest.bean.ViewBean;
import org.apache.hadoop.chukwa.rest.bean.WidgetBean;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class TestViewResource extends SetupTestEnv{
  public void testViewClone() {
    MultivaluedMap form = new MultivaluedMapImpl();
    form.add("owner", "system");
    form.add("view_name","test");
    form.add("view_vid","default");
    client = Client.create();
    resource = client.resource("http://localhost:"+restPort);
    ReturnCodeBean result = resource.path("/hicc/v1/view").
    header("Authorization", authorization).
    post(ReturnCodeBean.class,form);
    assertEquals(1,result.getCode());
  }
  
  public void testViewSave() {
    client = Client.create();
    resource = client.resource("http://localhost:"+restPort);
    ViewBean view = resource.path("/hicc/v1/view/vid/test").header("Authorization", authorization).get(ViewBean.class);
    view.setPermissionType("private");
    client = Client.create();
    resource = client.resource("http://localhost:"+restPort);
    ReturnCodeBean result = (ReturnCodeBean) resource.path("/hicc/v1/view").
    header("Content-Type","application/json").
    header("Authorization", authorization).
    put(ReturnCodeBean.class, view);
    assertEquals(1, result.getCode());
  }
  
  public void testViewLoad() {
    client = Client.create();
    resource = client.resource("http://localhost:"+restPort);
    ViewBean view = resource.path("/hicc/v1/view/vid/test").header("Authorization", authorization).get(ViewBean.class);
    assertEquals("test", view.getName());
    assertEquals("admin", view.getOwner());
    assertEquals("private", view.getPermissionType());
  }
  
  public void testViewDelete() {
    client = Client.create();
    resource = client.resource("http://localhost:"+restPort);
    ReturnCodeBean result = resource.path("/hicc/v1/view/delete/admin/vid/test").
    header("Authorization", authorization).
    delete(ReturnCodeBean.class);
    assertEquals(1,result.getCode());
  }
}
