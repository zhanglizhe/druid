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

package io.druid.server.namespace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.StringUtils;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import io.druid.server.listener.announcer.ListenerResourceDiscoveryModule;
import io.druid.server.listener.resource.ListenerResource;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.util.Map;

public class NamespacedExtractionModuleTest
{

  @Before
  public void setUp() throws Exception
  {

  }

  @After
  public void tearDown() throws Exception
  {

  }

  @Test
  public void testInjection()
  {
    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bind(Key.get(String.class, Names.named("serviceName"))).toInstance("serviceName");
                binder.bind(Key.get(Integer.class, Names.named("servicePort"))).toInstance(0);
              }
            },
            new ListenerResourceDiscoveryModule()
        )
    );
    ListenerResource resource = injector.getInstance(NamespaceResource.class);
    HttpServletRequest request = EasyMock.createNiceMock(HttpServletRequest.class);
    EasyMock.expect(request.getContentType()).andReturn(MediaType.APPLICATION_JSON).once();
    EasyMock.reset(request);
    Response response = resource.serviceAnnouncementPOSTAll(
        new ByteArrayInputStream(StringUtils.toUtf8("")),
        request
    );
    Assert.assertNotNull(response);
    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableSet.of("error"), ((Map) response.getEntity()).keySet());
  }
}
