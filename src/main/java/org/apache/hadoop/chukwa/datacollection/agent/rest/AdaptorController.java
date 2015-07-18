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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.PathParam;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;
import javax.servlet.http.HttpServletResponse;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JAX-RS controller to handle all HTTP request to the Agent that deal with adaptors.
 */
@Path("/adaptor")
public class AdaptorController {

  private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat();
  private static final Log LOG = LogFactory.getLog(AdaptorController.class);

  static {
    DECIMAL_FORMAT.setMinimumFractionDigits(2);
    DECIMAL_FORMAT.setMaximumFractionDigits(2);
    DECIMAL_FORMAT.setGroupingUsed(false);
  }

  /**
   * Adds an adaptor to the agent and returns the adaptor info
   * 
   * @request.representation.example {@link Examples#CREATE_ADAPTOR_SAMPLE}
   * @response.representation.200.doc Adaptor has been registered
   * @response.representation.200.mediaType application/json
   * @response.representation.200.example {@link Examples#ADAPTOR_STATUS_SAMPLE}
   * @response.representation.400.doc Error in register adaptor
   * @response.representation.400.mediaType text/plain
   * @response.representation.400.example Bad Request
   */
  @POST
  @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public Response addAdaptor(AdaptorConfig ac) {
    ChukwaAgent agent = ChukwaAgent.getAgent();
    if (ac.getAdaptorClass() == null || 
        ac.getDataType() == null) {
      return badRequestResponse("Bad adaptor config.");
    }
  
    StringBuilder addCommand = new StringBuilder("add ");
    addCommand.append(ac.getAdaptorClass()).append(' ');
    addCommand.append(ac.getDataType());
    if (ac.getAdaptorParams() != null)
      addCommand.append(' ').append(ac.getAdaptorParams());
    addCommand.append(' ').append(ac.getOffset());

    // add the adaptor
    try {
      String adaptorId = agent.processAddCommandE(addCommand.toString());
      return doGetAdaptor(adaptorId);
    } catch (AdaptorException e) {
      LOG.warn("Could not add adaptor for data type: '" + ac.getDataType() +
          "', error: " + e.getMessage());
      return badRequestResponse("Could not add adaptor for data type: '" + ac.getDataType() +
              "', error: " + e.getMessage());
    }
  }

  /**
   * Remove an adaptor from the agent
   *
   * @param adaptorId id of adaptor to remove.
   * @response.representation.200.doc Delete adaptor by id
   * @response.representation.200.mediaType text/plain
   */
  @DELETE
  @Path("/{adaptorId}")
  @Produces({MediaType.TEXT_PLAIN})
  public Response removeAdaptor(@PathParam("adaptorId") String adaptorId) {
    ChukwaAgent agent = ChukwaAgent.getAgent();

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
   * 
   * @response.representation.200.doc List all configured adaptors
   * @response.representation.200.mediaType application/json
   * @response.representation.200.example {@link Examples#ADAPTOR_LIST_SAMPLE}
   */
  @GET
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response getAdaptors() {
    return doGetAdaptors();
  }

  /**
   * Get a single adaptor
   * 
   * @param adaptorId id of the adaptor to return
   * @response.representation.200.doc Adaptor status and data transfer rate in 1, 5, 10 minutes averages
   * @response.representation.200.mediaType application/json
   * @response.representation.200.example {@link Examples#ADAPTOR_STATUS_SAMPLE}
   */
  @GET
  @Path("/{adaptorId}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response getAdaptor(@PathParam("adaptorId") String adaptorId) {
    return doGetAdaptor(adaptorId);
  }

  /**
   * Handles a single adaptor request for rendering data model output.
   * 
   * @return Response object
   */
  private Response doGetAdaptor(String adaptorId) {
    return Response.ok(buildAdaptor(adaptorId)).build();
  }

  /**
   * Rendering data model output for all adaptors
   * 
   * @return Response object
   */
  private Response doGetAdaptors() {
    return Response.ok(buildAdaptors()).build();
  }
  
  /**
   * Renders info for one adaptor.
   */
  protected AdaptorInfo buildAdaptor(String adaptorId) {
    ChukwaAgent agent = ChukwaAgent.getAgent();
    Adaptor adaptor = agent.getAdaptor(adaptorId);
    OffsetStatsManager<Adaptor> adaptorStats = agent.getAdaptorStatsManager();

    AdaptorInfo info = new AdaptorInfo();
    info.setId(adaptorId);
    info.setDataType(adaptor.getType());
    info.setAdaptorClass(adaptor.getClass().getName());
    String[] status = adaptor.getCurrentStatus().split(" ",2);
    info.setAdaptorParams(status[1]);
    List<AdaptorAveragedRate> rates = new ArrayList<AdaptorAveragedRate>();
    rates.add(new AdaptorAveragedRate(60, adaptorStats.calcAverageRate(adaptor,  60)));
    rates.add(new AdaptorAveragedRate(300, adaptorStats.calcAverageRate(adaptor,  300)));
    rates.add(new AdaptorAveragedRate(600, adaptorStats.calcAverageRate(adaptor,  600)));
    info.setAdaptorRates(rates);
    return info;
  }
  
  /**
   * Renders info for all adaptors.
   */
  protected AdaptorList buildAdaptors() {
    ChukwaAgent agent = ChukwaAgent.getAgent();
    Map<String, String> adaptorMap = agent.getAdaptorList();
    AdaptorList list = new AdaptorList();
    for(String name : adaptorMap.keySet()) {
      Adaptor adaptor = agent.getAdaptor(name);
      OffsetStatsManager<Adaptor> adaptorStats = agent.getAdaptorStatsManager();

      AdaptorInfo info = new AdaptorInfo();
      info.setId(name);
      info.setDataType(adaptor.getType());
      info.setAdaptorClass(adaptor.getClass().getName());
      String[] status = adaptor.getCurrentStatus().split(" ",2);
      info.setAdaptorParams(status[1]);
      List<AdaptorAveragedRate> rates = new ArrayList<AdaptorAveragedRate>();
      rates.add(new AdaptorAveragedRate(60, adaptorStats.calcAverageRate(adaptor,  60)));
      rates.add(new AdaptorAveragedRate(300, adaptorStats.calcAverageRate(adaptor,  300)));
      rates.add(new AdaptorAveragedRate(600, adaptorStats.calcAverageRate(adaptor,  600)));
      info.setAdaptorRates(rates);
      list.add(info);
    }
    return list;
  }

  /**
   * Renders bad request response.
   */
  private static Response badRequestResponse(String content) {
    return Response.status(HttpServletResponse.SC_BAD_REQUEST)
                     .entity(content).build();
  }

}
