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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.RE;
import com.metamx.common.logger.Logger;
import io.druid.metadata.MetadataNamespaceManager;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * Contains information about namespaces
 */
@Path("/druid/coordinator/v1/namespaces")
public class NamespacesResource
{
  private static final Logger log = new Logger(NamespacesResource.class);
  private final MetadataNamespaceManager metadataNamespaceManager;

  @Inject
  public NamespacesResource(
      final MetadataNamespaceManager metadataNamespaceManager
  )
  {
    this.metadataNamespaceManager = metadataNamespaceManager;
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response getNamespaces()
  {
    try {
      return Response.ok().entity(metadataNamespaceManager.knownNamespaces()).build();
    }
    catch (Exception e) {
      log.error(e, "Error getting list of namespaces");
      return Response.serverError().entity(cleanError(e)).build();
    }
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response newNamespace(
      final Map<String, Object> map
  )
  {
    final String namespace = map.get("namespace").toString();
    if (Strings.isNullOrEmpty(namespace)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(cleanError(new IllegalArgumentException("Missing `namespace`")))
                     .build();
    }
    Response.ResponseBuilder builder = Response.status(Response.Status.ACCEPTED);
    metadataNamespaceManager.addOrUpdateNamespace(namespace, map);
    builder.entity(map);
    return builder.build();
  }

  @DELETE
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/{namespace}")
  public Response deleteNamespace(@PathParam("namespace") String namespace)
  {
    if (Strings.isNullOrEmpty(namespace)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(cleanError(new IAE("Must specify `namespace`")))
                     .build();
    }
    metadataNamespaceManager.disableNamespace(namespace);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  private static Map<String, String> cleanError(Exception e)
  {
    return ImmutableMap.of("error", Strings.nullToEmpty(e.getMessage()));
  }


  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/{namespace}")
  public Response getSpecificNamespace(@PathParam("namespace") String namespace)
  {
    if (Strings.isNullOrEmpty(namespace)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(cleanError(new NullPointerException("`namespace` required")))
                     .build();
    }
    final Map<String, Object> map = metadataNamespaceManager.getNamespace(namespace);
    if (map == null) {
      return Response.status(Response.Status.NOT_FOUND)
                     .entity(cleanError(new RE("namespace [%s] not found", namespace)))
                     .build();
    }
    return Response.ok().entity(map).build();
  }
}
