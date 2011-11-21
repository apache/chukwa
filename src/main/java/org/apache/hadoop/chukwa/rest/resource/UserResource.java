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

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.chukwa.rest.bean.ReturnCodeBean;
import org.apache.hadoop.chukwa.rest.bean.UserBean;
import org.apache.hadoop.chukwa.datastore.UserStore;
import org.apache.hadoop.chukwa.util.ExceptionUtil;


@Path ("/user")
public class UserResource {
  protected static Log log = LogFactory.getLog(UserResource.class);
  
  @GET
  @Path("uid/{uid}")
  public UserBean getProfile(@PathParam("uid") String uid) {
    UserStore user;
    UserBean result;
    try {
      user = new UserStore(uid);
      result = user.get();
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity("User does not exist.").build());
    }
    return result;
  }
  
  @PUT
  @Consumes("application/json")
  public ReturnCodeBean setProfile(UserBean user) {
    try {
      UserStore us = new UserStore(user.getId());
      us.set(user);
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity("User does not exist.").build());
    }
    return new ReturnCodeBean(ReturnCodeBean.SUCCESS,"Saved.");
  }
  
  @GET
  @Path("list")
  @Produces("application/javascript")
  public String getUserList() {
    String result = "";
    try {
      result = UserStore.list().toString();
    } catch (IllegalAccessException e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity("User does not exist.").build());
    }
    return result;
  }
}
