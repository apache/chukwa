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
package org.apache.hadoop.chukwa.datacollection.agent.rest;

import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorException;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.hadoop.chukwa.datacollection.OffsetStatsManager;
import org.apache.commons.lang.StringEscapeUtils;
import org.json.JSONObject;
import org.json.JSONException;

import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;
import java.text.DecimalFormat;
import java.util.Map;

/**
 * JAX-RS controller to handle all HTTP request to the Agent that deal with adaptors.
 *
 * To return all adaptors:
 *   GET /rest/v1/adaptor
 *   Optional QS params: viewType=text|xml (default=xml)
 *
 * To return a single adaptor:
 *   GET /rest/v1/adaptor/[adaptorId]
 *   Optional QS params: viewType=text|xml (default=xml)
 *
 * To remove an adaptor:
 *   DELETE /rest/v1/adaptor/[adaptorId]
 *
 * To add an adaptor:
 *   POST /rest/v1/adaptor
 *   Content-Type: application/json
 *   Optional QS params: viewType=text|xml (default=xml)
 *
 *   { "DataType" : "foo",
 *     "AdaptorClass" : "FooAdaptor",
 *     "AdaptorParams" : "params",
 *     "Offset"     : "0" }
 *   The first 3 params above are the only required ones.
 */
@Path("/adaptor")
public class AdaptorController {

  private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat();

  static {
    DECIMAL_FORMAT.setMinimumFractionDigits(2);
    DECIMAL_FORMAT.setMaximumFractionDigits(2);
    DECIMAL_FORMAT.setGroupingUsed(false);
  }

  /**
   * Adds an adaptor to the agent and returns the adaptor info.
   * @param context servletContext
   * @param viewType type of view to return (text|xml)
   * @param postBody JSON post body
   * @return Response object
   */
  @POST
  @Consumes("application/json")
  @Produces({"text/xml","text/plain"})
  public Response addAdaptor(@Context ServletContext context,
                             @QueryParam("viewType") String viewType,
                             String postBody) {
    ChukwaAgent agent = (ChukwaAgent)context.getAttribute("ChukwaAgent");

    if (postBody == null) return badRequestResponse("Empty POST body.");

    // parse the json.
    StringBuilder addCommand = new StringBuilder("add ");
    try {
      JSONObject reqJson = new JSONObject(postBody);

      String dataType = reqJson.getString("DataType");
      //TODO: figure out how to set this per-adaptor
      //String cluster = reqJson.getString("Cluster");
      String adaptorClass = reqJson.getString("AdaptorClass");

      String adaptorParams = fetchOptionalString(reqJson, "AdaptorParams");
      long offset = fetchOptionalLong(reqJson, "Offset", 0);

      addCommand.append(adaptorClass).append(' ');
      addCommand.append(dataType);
      if (adaptorParams != null)
        addCommand.append(' ').append(adaptorParams);
      addCommand.append(' ').append(offset);

    } catch (JSONException e) {
      return badRequestResponse("Invalid JSON passed: '" + postBody + "', error: " + e.getMessage());
    }

    // add the adaptor
    try {
      String adaptorId = agent.processAddCommandE(addCommand.toString());

      return doGetAdaptor(agent, adaptorId, viewType);
    } catch (AdaptorException e) {
      return badRequestResponse("Could not add adaptor for postBody: '" + postBody +
              "', error: " + e.getMessage());
    }
  }

  /**
   * Remove an adaptor from the agent
   * @param context ServletContext
   * @param adaptorId id of adaptor to remove.
   * @return Response object
   */
  @DELETE
  @Path("/{adaptorId}")
  @Produces({"text/plain"})
  public Response removeAdaptor(@Context ServletContext context,
                                @PathParam("adaptorId") String adaptorId) {
    ChukwaAgent agent = (ChukwaAgent)context.getAttribute("ChukwaAgent");

    // validate that we have an adaptorId
    if (adaptorId == null) {
      return badRequestResponse("Missing adaptorId.");
    }

    // validate that we have a valid adaptorId
    if (agent.getAdaptor(adaptorId) == null) {
      return badRequestResponse("Invalid adaptorId: " + adaptorId);
    }

    // stop the agent
    agent.stopAdaptor(adaptorId, true);
    return Response.ok().build();
  }

  /**
   * Get all adaptors
   * @param context ServletContext
   * @param viewType type of view to return (text|xml)
   * @return Response object
   */
  @GET
  @Produces({"text/xml", "text/plain"})
  public Response getAdaptors(@Context ServletContext context,
                              @QueryParam("viewType") String viewType) {
    ChukwaAgent agent = (ChukwaAgent)context.getAttribute("ChukwaAgent");
    return doGetAdaptor(agent, null, viewType);
  }

  /**
   * Get a single adaptor
   * @param context ServletContext
   * @param viewType type of view to return (text|xml)
   * @param adaptorId id of the adaptor to return
   * @return Response object
   */
  @GET
  @Path("/{adaptorId}")
  @Produces({"text/xml","text/plain"})
  public Response getAdaptor(@Context ServletContext context,
                             @QueryParam("viewType") String viewType,
                             @PathParam("adaptorId") String adaptorId) {
    ChukwaAgent agent = (ChukwaAgent)context.getAttribute("ChukwaAgent");
    return doGetAdaptor(agent, adaptorId, viewType);
  }

  /**
   * Handles the common rendering logic of checking for view type and returning
   * a Response with one or all adaptors.
   * @return Response object
   */
  private Response doGetAdaptor(ChukwaAgent agent, String adaptorId, String viewType) {
    if ("text".equals(viewType)) {
      return textResponse(buildAdaptorText(agent, adaptorId));
    }
    else if ("xml".equals(viewType) || viewType == null) {
      return xmlResponse(buildAdaptorXML(agent, adaptorId));
    }
    else {
      return badRequestResponse("Invalid viewType: " + viewType);
    }
  }

  /**
   * Renders info for one or all adaptors in XML.
   */
  protected String buildAdaptorXML(ChukwaAgent agent, String adaptorId) {

    StringBuilder out = new StringBuilder(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>");

    appendStartTag(out, "Response");

    if (adaptorId == null) {
      Map<String, String> adaptorMap = agent.getAdaptorList();
      appendStartTag(out, "Adaptors", "total", adaptorMap.size());

      for (String name : adaptorMap.keySet()) {
        Adaptor adaptor = agent.getAdaptor(name);
        appendAdaptorXML(out, agent, adaptor);
      }

      appendEndTag(out, "Adaptors");
    }
    else {
      Adaptor adaptor = agent.getAdaptor(adaptorId);
      if (adaptor != null) {
        appendAdaptorXML(out, agent, adaptor);
      }
      else {
        appendElement(out, "Error", "Invalid adaptor id: " + adaptorId);
      }
    }

    appendEndTag(out, "Response");

    return out.toString();
  }

  /**
   * Renders info for one or all adaptors in plain text (YAML).
   */
  protected String buildAdaptorText(ChukwaAgent agent, String adaptorId) {
    StringBuilder out = new StringBuilder();

    Map<String, String> adaptorMap = agent.getAdaptorList();
    int indent = 0;

    if (adaptorId == null) {
      appendNvp(out, indent, "adaptor_count", adaptorMap.size());
      appendNvp(out, indent, "adaptors", "");

      indent += 4;
      for(String name : adaptorMap.keySet()) {
        Adaptor adaptor = agent.getAdaptor(name);
        appendAdaptorText(out, indent, agent, adaptor);
      }
    }
    else {
      Adaptor adaptor = agent.getAdaptor(adaptorId);
      if (adaptor != null) {
        appendNvp(out, indent, "adaptor", "");
        indent += 4;
        appendAdaptorText(out, indent, agent, adaptor);
      }
      else {
        appendNvp(out, indent, "error_message", "Invalid adaptor id: " + adaptorId, true);
      }
    }

    return out.toString();
  }

  private void appendAdaptorText(StringBuilder out, int indent,
                                 ChukwaAgent agent, Adaptor adaptor) {
    appendNvp(out, indent, "- adaptor_id", agent.offset(adaptor).adaptorID());
    appendNvp(out, indent, "data_type", adaptor.getType());
    appendNvp(out, indent, "offset", agent.offset(adaptor).offset());
    appendNvp(out, indent, "adaptor_class", adaptor.getClass().getName());
    appendNvp(out, indent, "adaptor_params", adaptor.getCurrentStatus(), true);

    OffsetStatsManager adaptorStats = agent.getAdaptorStatsManager();

    appendNvp(out, indent, "average_rates", "");
    indent += 4;
    appendNvp(out, indent, "- rate",
            DECIMAL_FORMAT.format(adaptorStats.calcAverageRate(adaptor,  60)));
    appendNvp(out, indent, "interval", "60");
    appendNvp(out, indent, "- rate",
            DECIMAL_FORMAT.format(adaptorStats.calcAverageRate(adaptor,  300)));
    appendNvp(out, indent, "interval", "300");
    appendNvp(out, indent, "- rate",
            DECIMAL_FORMAT.format(adaptorStats.calcAverageRate(adaptor,  600)));
    appendNvp(out, indent, "interval", "600");
    indent -= 4;
  }

  private void appendAdaptorXML(StringBuilder out,
                               ChukwaAgent agent, Adaptor adaptor) {
    appendStartTag(out, "Adaptor",
            "id", agent.offset(adaptor).adaptorID(),
            "dataType", adaptor.getType(),
            "offset", agent.offset(adaptor).offset());

    appendElement(out, "AdaptorClass", adaptor.getClass().getName());
    appendElement(out, "AdaptorParams", adaptor.getCurrentStatus());

    OffsetStatsManager adaptorStats = agent.getAdaptorStatsManager();

    appendElement(out, "AverageRate",
            DECIMAL_FORMAT.format(adaptorStats.calcAverageRate(adaptor,  60)),
            "intervalSeconds", "60");
    appendElement(out, "AverageRate",
            DECIMAL_FORMAT.format(adaptorStats.calcAverageRate(adaptor,  300)),
            "intervalSeconds", "300");
    appendElement(out, "AverageRate",
            DECIMAL_FORMAT.format(adaptorStats.calcAverageRate(adaptor,  600)),
            "intervalSeconds", "600");

    appendEndTag(out, "Adaptor");
  }

  // *** static helper methods below. could be moved into a util class ***

  //   * response handling *

  private static Response textResponse(Object content) {
    return Response.ok(content, MediaType.TEXT_PLAIN).build();
  }

  private static Response xmlResponse(String content) {
    return Response.ok(content, MediaType.TEXT_XML).build();
  }

  private static Response badRequestResponse(String content) {
    return Response.status(HttpServletResponse.SC_BAD_REQUEST)
                     .entity(content).build();
  }

  //   * json handling *

  private static String fetchOptionalString(JSONObject json, String name) {
    try {
      return json.getString(name);
    } catch (JSONException e) {}
    return null;
  }

  private static long fetchOptionalLong(JSONObject json, String name, long defaultLong) {
    try {
      return json.getLong(name);
    } catch (JSONException e) {
      return defaultLong;
    }
  }

  //   * plain text response handling *

  /**
   * Helper for appending name/value pairs to the ServletOutputStream in the
   * format [name]: [value]
   */
  protected static void appendNvp(StringBuilder out,
                                  String name, Object value) {
    appendNvp(out, 0, name, value, false);
  }

  /**
   * Helper for appending name/value pairs to the ServletOutputStream in the
   * format [name]: [value] with indent number of spaces prepended.
   */
  protected static void appendNvp(StringBuilder out, int indent,
                                  String name, Object value) {
    appendNvp(out, indent, name, value, false);
  }

  /**
   * Helper for appending name/value pairs to the ServletOutputStream in the
   * format [name]: [value] with indent number of spaces prepended. Set
   * stringLiteral=true if the value might contain special YAML characters.
   */
  protected static void appendNvp(StringBuilder out, int indent,
                                  String name, Object value,
                                  boolean stringLiteral) {

    if (name.startsWith("- ") && indent > 1) indent -= 2;

    indent(out, indent);
    out.append(name);
    out.append(": ");

    if (stringLiteral) {
      out.append('|').append('\n');
      indent(out, indent + 2);
    }

    if (value != null)
      out.append(value.toString()).append('\n');
  }

  /**
   * Helper to insert a number of spaces into the output stream.
   */
  protected static void indent(StringBuilder out, int indent) {
    for (int i = 0; i < indent; i++) {
      out.append(' ');
    }
  }

  //   * XML text response handling *

  /**
   * XML helper to append a Element start tag. Optionally a number of attributeNvps
   * can be passed, which will be inserted as XML atrribute names and values,
   * alternating between names and values.
   */
  protected static void appendStartTag(StringBuilder out,
                                       String name,
                                       Object... attributeNvps) {
    out.append("<");
    out.append(name);
    for(int i = 0; i < attributeNvps.length - 1; i = i + 2) {
      out.append(" ");
      out.append(attributeNvps[i].toString());
      out.append("=\"");
      if (attributeNvps[i + 1] != null)
        out.append(StringEscapeUtils.escapeXml(attributeNvps[i + 1].toString()));
      out.append("\"");
    }
    out.append(">");
  }

  /**
   * XML helper to append a Element end tag.
   */
  protected static void appendEndTag(StringBuilder out,
                                     String name) {
    out.append("</");
    out.append(name);
    out.append(">");
  }

  /**
   * XML helper to append an Element and it's child text value. Optionally a
   * number of attributeNvps can be passed, which will be inserted as XML
   * atrribute names and values, alternating between names and values.
   */
  protected static void appendElement(StringBuilder out,
                                      String name, Object value,
                                      Object... attributeNvps) {
    appendStartTag(out, name, attributeNvps);
    if (value != null)
      out.append(StringEscapeUtils.escapeXml(value.toString()));
    appendEndTag(out, name);
  }

}
