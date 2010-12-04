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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.chukwa.rest.bean.ReturnCodeBean;
import org.apache.hadoop.chukwa.rest.bean.ViewBean;
import org.apache.hadoop.chukwa.datastore.ViewStore;
import org.apache.hadoop.chukwa.util.ExceptionUtil;

@Path ("/view")
public class ViewResource {
  protected static Log log = LogFactory.getLog(ViewResource.class);

  @GET
  @Path("vid/{vid}")
  public ViewBean getView(@Context HttpServletRequest request, @PathParam("vid") String vid) {
    ViewStore view;
    ViewBean vr;
    String uid = request.getRemoteUser();
    try {
      view = new ViewStore(uid, vid);
      vr = view.get();
      if(request.getRemoteUser().intern()!=vr.getOwner().intern() && vr.getPermissionType().intern()!="public".intern()) {
    	  throw new WebApplicationException(Response.status(Response.Status.FORBIDDEN).entity("permission denied.").build());
      }
    } catch (IllegalAccessException e) {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity("View does not exist.").build());
    }
    return vr;
  }
  
  @PUT
  @Consumes("application/json")
  public ReturnCodeBean setView(@Context HttpServletRequest request, ViewBean view) {
    try {
      if(request.getRemoteUser().intern()==view.getOwner().intern()) {
        ViewStore vs = new ViewStore(view.getOwner(), view.getName());
        vs.set(view);
      } else {
          throw new WebApplicationException(Response.status(Response.Status.FORBIDDEN)
              .entity("Permission denied.").build());    	  
      }
    } catch (IllegalAccessException e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity("View save failed.").build());
    }
    return new ReturnCodeBean(ReturnCodeBean.SUCCESS,"Saved");
  }

  @POST
  @Path("permission")
  public ReturnCodeBean changeViewPermission(@Context HttpServletRequest request, @FormParam("owner") String owner, @FormParam("view_vid") String vid, @FormParam("permission") String permission) {
    try {
      if(owner.intern()==request.getRemoteUser().intern()) {
        ViewStore vs = new ViewStore(owner, vid);
        ViewBean view = vs.get();
        vs.delete();
        view.setPermissionType(permission);
        vs.set(view);
      } else {
        throw new Exception("Permission denied.");
      }
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("View save failed.").build());      
    }
    return new ReturnCodeBean(ReturnCodeBean.SUCCESS,"Saved");
  }

  @POST
  public ReturnCodeBean changeView(@Context HttpServletRequest request, @FormParam("owner") String owner, @FormParam("view_vid") String oldVid, @FormParam("view_name") String name) {
    try {
      ViewStore vs;
      if(oldVid!=null) {
        vs = new ViewStore(owner, oldVid);
      } else {
        vs = new ViewStore(null, "default");
      }
      ViewBean view = vs.get();
      view.setOwner(request.getRemoteUser());
      view.setName(name);
      view.setDescription(name);
      if(oldVid==null) {
        view.setPermissionType("private");
      }
      vs = new ViewStore(request.getRemoteUser(), name);
      vs.set(view);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("View save failed.").build());      
    }
    return new ReturnCodeBean(ReturnCodeBean.SUCCESS,"Saved");
  }

  @DELETE
  @Path("delete/{owner}/vid/{vid}")
  public ReturnCodeBean deleteView(@Context HttpServletRequest request, @PathParam("owner") String owner, @PathParam("vid") String vid) {
    try {
      if(owner.intern()==request.getRemoteUser().intern()) {
        ViewStore vs = new ViewStore(owner, vid);
        vs.delete();
      } else {
        throw new WebApplicationException(Response.status(Response.Status.FORBIDDEN)
            .entity("View delete failed.").build());              
      }
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("View delete failed.").build());      
    }
    return new ReturnCodeBean(ReturnCodeBean.SUCCESS,"Deleted");
  }

  @GET
  @Path("list")
  public String getUserViewList(@Context HttpServletRequest request) {
    String result = "";
    String uid = null;
    try {
      if(uid==null) {
        uid = request.getRemoteUser();
      }
      result = ViewStore.list(uid).toJSONString();
    } catch (Exception e) {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity("View does not exist.").build());
    }
    return result;
  }
}
