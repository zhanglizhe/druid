/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.http;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.RE;
import com.metamx.common.logger.Logger;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.common.utils.ServletResourceUtils;
import io.druid.server.namespace.cache.LookupCoordinatorManager;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * Contains information about lookups exposed through the coordinator
 */
@Path("/druid/coordinator/v1/lookups")
public class LookupsResource
{
  private static final Logger log = new Logger(LookupsResource.class);
  private final LookupCoordinatorManager lookupCoordinatorManager;

  @Inject
  public LookupsResource(
      final LookupCoordinatorManager lookupCoordinatorManager
  )
  {
    this.lookupCoordinatorManager = lookupCoordinatorManager;
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response getNamespaces()
  {
    try {
      final Map<String, Map<String, Object>> knownNamespaces = lookupCoordinatorManager.getKnownLookups();
      if (knownNamespaces == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok().entity(knownNamespaces).build();
      }
    }
    catch (Exception e) {
      log.error(e, "Error getting list of lookups");
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response updateAllNamespaces(
      final Map<String, Map<String, Object>> map,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context HttpServletRequest req
  )
  {
    try {
      lookupCoordinatorManager.updateLookups(map, new AuditInfo(author, comment, req.getRemoteAddr()));
      return Response.status(Response.Status.ACCEPTED).entity(map).build();
    }
    catch (Exception e) {
      log.error(e, "Error creating new lookup [%s]", map);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @DELETE
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/{lookup}")
  public Response deleteNamespace(
      @PathParam("lookup") String lookup,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context HttpServletRequest req
  )
  {
    try {
      if (Strings.isNullOrEmpty(lookup)) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(new IAE("Must specify `lookup`")))
                       .build();
      }

      if (lookupCoordinatorManager.deleteLookup(lookup, new AuditInfo(author, comment, req.getRemoteAddr()))) {
        return Response.status(Response.Status.ACCEPTED).build();
      } else {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
    }
    catch (Exception e) {
      log.error(e, "Error deleting namespace [%s]", lookup);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }


  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/{lookup}")
  public Response getSpecificNamespace(@PathParam("lookup") String lookup)
  {
    try {
      if (Strings.isNullOrEmpty(lookup)) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(new NullPointerException("`lookup` required")))
                       .build();
      }
      final Map<String, Object> map = lookupCoordinatorManager.getLookup(lookup);
      if (map == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(ServletResourceUtils.sanitizeException(new RE("lookup [%s] not found", lookup)))
                       .build();
      }
      return Response.ok().entity(map).build();
    }
    catch (Exception e) {
      log.error(e, "Error getting lookup [%s]", lookup);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }
}
