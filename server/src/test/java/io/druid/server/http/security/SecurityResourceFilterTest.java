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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.metamx.emitter.service.ServiceEmitter;
import com.sun.jersey.spi.container.ResourceFilter;
import io.druid.audit.AuditManager;
import io.druid.client.BrokerServerView;
import io.druid.client.CoordinatorServerView;
import io.druid.client.FilteredServerInventoryView;
import io.druid.client.InventoryView;
import io.druid.client.TimelineServerView;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.common.config.JacksonConfigManager;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.metadata.MetadataRuleManager;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.server.ClientInfoResource;
import io.druid.server.QueryManager;
import io.druid.server.QueryResource;
import io.druid.server.StatusResource;
import io.druid.server.coordination.ZkCoordinator;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.http.BrokerResource;
import io.druid.server.http.CoordinatorDynamicConfigsResource;
import io.druid.server.http.CoordinatorResource;
import io.druid.server.http.DatasourcesResource;
import io.druid.server.http.HistoricalResource;
import io.druid.server.http.IntervalsResource;
import io.druid.server.http.MetadataResource;
import io.druid.server.http.RulesResource;
import io.druid.server.http.ServersResource;
import io.druid.server.http.TiersResource;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.RequestLogger;
import java.util.Collection;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SecurityResourceFilterTest extends ResourceFilterTestHelper
{
  @Parameterized.Parameters
  public static Collection<Object[]> data()
  {
    return ImmutableList.copyOf(
        Iterables.concat(
            getRequestPaths(
                CoordinatorResource.class,
                ImmutableList.<Class<?>>of(DruidCoordinator.class)
            ),
            getRequestPaths(
                DatasourcesResource.class,
                ImmutableList.of(
                    CoordinatorServerView.class,
                    MetadataSegmentManager.class,
                    IndexingServiceClient.class
                )
            ),
            getRequestPaths(
                BrokerResource.class,
                ImmutableList.<Class<?>>of(BrokerServerView.class)
            ),
            getRequestPaths(
                HistoricalResource.class,
                ImmutableList.<Class<?>>of(ZkCoordinator.class)
            ),
            getRequestPaths(
                IntervalsResource.class,
                ImmutableList.<Class<?>>of(InventoryView.class)
            ),
            getRequestPaths(MetadataResource.class, ImmutableList.of(
                MetadataSegmentManager.class,
                IndexerMetadataStorageCoordinator.class
            )),
            getRequestPaths(
                RulesResource.class,
                ImmutableList.<Class<?>>of(MetadataRuleManager.class, AuditManager.class)
            ),
            getRequestPaths(ServersResource.class, ImmutableList.<Class<?>>of(InventoryView.class)),
            getRequestPaths(TiersResource.class, ImmutableList.<Class<?>>of(InventoryView.class)),
            getRequestPaths(ClientInfoResource.class, ImmutableList.of(
                FilteredServerInventoryView.class,
                TimelineServerView.class,
                SegmentMetadataQueryConfig.class
            )),
            getRequestPaths(
                CoordinatorDynamicConfigsResource.class,
                ImmutableList.<Class<?>>of(JacksonConfigManager.class, AuditManager.class)
            ),
            getRequestPaths(
                QueryResource.class,
                ImmutableList.of(
                    ServerConfig.class,
                    ObjectMapper.class,
                    QuerySegmentWalker.class,
                    ServiceEmitter.class,
                    RequestLogger.class,
                    QueryManager.class
                ),
                ImmutableList.<Key<?>>of(
                    Key.get(ObjectMapper.class, Json.class),
                    Key.get(ObjectMapper.class, Smile.class)
                )
            ),
            getRequestPaths(StatusResource.class, ImmutableList.<Class<?>>of())
        )
    );
  }

  private final String requestPath;
  private final String requestMethod;
  private final ResourceFilter resourceFilter;
  private final Injector injector;

  public SecurityResourceFilterTest(
      String requestPath,
      String requestMethod,
      ResourceFilter resourceFilter,
      Injector injector
  )
  {
    this.requestPath = requestPath;
    this.requestMethod = requestMethod;
    this.resourceFilter = resourceFilter;
    this.injector = injector;
  }

  @Before
  public void setUp() throws Exception
  {
    setUp(resourceFilter);
  }

  @Test
  public void testDatasourcesResourcesFilteringAccess()
  {
    setUpMockExpectations(requestPath, true, requestMethod);
    EasyMock.replay(req, request, authorizationInfo);
    resourceFilter.getRequestFilter().filter(request);
    Assert.assertTrue(((AbstractResourceFilter) resourceFilter.getRequestFilter()).isApplicable(requestPath));
  }

  @Test(expected = WebApplicationException.class)
  public void testDatasourcesResourcesFilteringNoAccess()
  {
    setUpMockExpectations(requestPath, false, requestMethod);
    EasyMock.replay(req, request, authorizationInfo);
    Assert.assertTrue(((AbstractResourceFilter) resourceFilter.getRequestFilter()).isApplicable(requestPath));
    try {
      resourceFilter.getRequestFilter().filter(request);
    }
    catch (WebApplicationException e) {
      Assert.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test
  public void testDatasourcesResourcesFilteringBadPath()
  {
    EasyMock.replay(req, request, authorizationInfo);
    final String badRequestPath = requestPath.replaceAll("\\w+", "droid");
    Assert.assertFalse(((AbstractResourceFilter) resourceFilter.getRequestFilter()).isApplicable(badRequestPath));
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(req, request, authorizationInfo);
  }
}
