/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.http.security;

import com.google.common.base.Preconditions;
import com.sun.jersey.spi.container.ContainerRequest;
import io.druid.server.security.Access;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceType;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public class StateResourceFilter extends AbstractResourceFilter
{
  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    if (getAuthConfig().isEnabled()) {
      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424

      final String resourceName;
      if (request.getPath().startsWith("druid/broker/v1")) {
        resourceName = "BROKER";
      } else if (request.getPath().startsWith("druid/coordinator/v1")) {
        resourceName = "COORDINATOR";
      } else if (request.getPath().startsWith("druid/historical/v1")) {
        resourceName = "HISTORICAL";
      } else if (request.getPath().startsWith("druid/indexer/v1")) {
        resourceName = "OVERLORD";
      } else if (request.getPath().startsWith("druid/coordinator/v1/rules")) {
        resourceName = "RULES";
      } else if (request.getPath().startsWith("druid/coordinator/v1/tiers")) {
        resourceName = "TIER";
      } else if (request.getPath().startsWith("druid/worker/v1")) {
        resourceName = "WORKER";
      } else if (request.getPath().startsWith("druid/coordinator/v1/servers")) {
        resourceName = "SERVER";
      } else if (request.getPath().startsWith("status")) {
        resourceName = "STATUS";
      } else {
        throw new WebApplicationException(
            Response.status(Response.Status.BAD_REQUEST).entity(
                String.format(
                    "Do not know how to authorize this request path [%s] with StateResourceFilter",
                    request.getPath()
                )
            ).build()
        );
      }

      final AuthorizationInfo authorizationInfo = (AuthorizationInfo) getReq().getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
      Preconditions.checkNotNull(
          authorizationInfo,
          "Security is enabled but no authorization info found in the request"
      );

      final Access authResult = authorizationInfo.isAuthorized(
          new Resource(resourceName, ResourceType.STATE),
          getAction(request)
      );
      if (!authResult.isAllowed()) {
        throw new WebApplicationException(
            Response.status(Response.Status.FORBIDDEN)
                    .entity(String.format("Access-Check-Result: %s", authResult.toString()))
                    .build()
        );
      }
    }

    return request;
  }
}
