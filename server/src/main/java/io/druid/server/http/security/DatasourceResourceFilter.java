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
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.sun.jersey.spi.container.ContainerRequest;
import io.druid.server.security.Access;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceType;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;

public class DatasourceResourceFilter extends AbstractResourceFilter
{
  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    if (getAuthConfig().isEnabled()) {
      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424

      /* dataSourceName appears after the "/datasources" fragment in paths
      *  - druid/coordinator/v1/datasources/{dataSourceName}/...
      *  - druid/coordinator/v1/metadata/datasources/{dataSourceName}/...
      *  - druid/v2/datasources/{dataSourceName}/...
      *  OR
      *  the 5th fragment in path
      *  - druid/coordinator/v1/rules/{dataSourceName}
      */
      final String dataSourceName;

      if (request.getPath().startsWith("druid/coordinator/v1/rules/")) {
        dataSourceName = request.getPathSegments().get(4).getPath();
      } else if (
          request.getPath().startsWith("druid/coordinator/v1/datasources/") ||
          request.getPath().startsWith("druid/coordinator/v1/metadata/datasources/") ||
          request.getPath().startsWith("druid/v2/datasources/")
          ) {

        dataSourceName = request.getPathSegments()
                                .get(
                                    Iterables.indexOf(
                                        request.getPathSegments(),
                                        new Predicate<PathSegment>()
                                        {
                                          @Override
                                          public boolean apply(PathSegment input)
                                          {
                                            return input.getPath().equals("datasources");
                                          }
                                        }
                                    ) + 1
                                ).getPath();
      } else {
        throw new WebApplicationException(
            Response.serverError().entity(
                String.format(
                    "Do not know how to extract dataSource information "
                    + "for authorization check for request path: [%s]",
                    request.getPath()
                )
            ).build()
        );
      }

      Preconditions.checkNotNull(dataSourceName);
      final AuthorizationInfo authorizationInfo = (AuthorizationInfo) getReq().getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
      Preconditions.checkNotNull(
          authorizationInfo,
          "Security is enabled but no authorization info found in the request"
      );
      final Access authResult = authorizationInfo.isAuthorized(
          new Resource(dataSourceName, ResourceType.DATASOURCE),
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
