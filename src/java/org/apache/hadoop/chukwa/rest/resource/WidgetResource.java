package org.apache.hadoop.chukwa.rest.resource;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.rest.bean.CatalogBean;

import org.apache.hadoop.chukwa.rest.bean.ReturnCodeBean;
import org.apache.hadoop.chukwa.rest.bean.WidgetBean;
import org.apache.hadoop.chukwa.datastore.WidgetStore;


import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

@Path ("/widget")
public class WidgetResource {
  private static Log log = LogFactory.getLog(WidgetResource.class);
  
  @GET
  @Path("wid/{wid}")
  public WidgetBean getProfile(@PathParam("wid") String wid) {
    HashMap<String, WidgetBean> list;
    try {
      list = WidgetStore.list();
    } catch (IllegalAccessException e) {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity("Widget does not exist.").build());
    }
    return list.get(wid);
  }
  
  @PUT
  @Consumes("application/json")
  public ReturnCodeBean saveWidget(WidgetBean widget) {
    try {
      WidgetStore ws = new WidgetStore();
      ws.set(widget);
    } catch(Exception e) {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity("Widget save failed.").build());
    }
    return new ReturnCodeBean(ReturnCodeBean.SUCCESS,"Saved");
  }
  
  @GET
  @Path("catalog")
  public CatalogBean getWidgetCatalog() {
    CatalogBean result;
    try {
      result = WidgetStore.getCatalog();
    } catch (IllegalAccessException e) {
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("No catalog exists.").build());
    }
    return result;
  }
}
