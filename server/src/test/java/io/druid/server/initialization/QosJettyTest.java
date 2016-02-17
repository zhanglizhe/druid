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

package io.druid.server.initialization;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.concurrent.Execs;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.server.DruidNode;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.initialization.jetty.ServletFilterHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlets.QoSFilter;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class QosJettyTest extends BaseJettyTest
{
  protected Lifecycle lifecycle;
  protected HttpClient client;
  protected int port = -1;

  public static void setProperties()
  {
    System.setProperty("druid.server.http.numThreads", "10");
  }

  @Before
  public void setup() throws Exception
  {
    setProperties();
    Injector injector = setupInjector();
    final DruidNode node = injector.getInstance(Key.get(DruidNode.class, Self.class));
    port = node.getPort();
    lifecycle = injector.getInstance(Lifecycle.class);
    lifecycle.start();
    ClientHolder holder = injector.getInstance(ClientHolder.class);
    client = holder.getClient();
  }

  protected Injector setupInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test", "localhost", null)
                );
                binder.bind(JettyServerInitializer.class).to(JettyServerInit.class).in(LazySingleton.class);

                Multibinder<ServletFilterHolder> multibinder = Multibinder.newSetBinder(
                    binder,
                    ServletFilterHolder.class
                );

                multibinder.addBinding().toInstance(
                    new ServletFilterHolder()
                    {

                      @Override
                      public String getPath()
                      {
                        return "/slow/hello";
                      }

                      @Override
                      public Map<String, String> getInitParameters()
                      {
                        return ImmutableMap.of("maxRequests", "2");
                      }

                      @Override
                      public Class<? extends Filter> getFilterClass()
                      {
                        return QoSFilter.class;
                      }

                      @Override
                      public Filter getFilter()
                      {
                        return new QoSFilter();
                      }

                      @Override
                      public EnumSet<DispatcherType> getDispatcherType()
                      {
                        return null;
                      }
                    });

                Jerseys.addResource(binder, SlowResource.class);
                Jerseys.addResource(binder, DefaultResource.class);
                LifecycleModule.register(binder, Server.class);
              }
            }
        )
    );
  }

  @After
  public void teardown()
  {
    lifecycle.stop();
  }

  public static class ClientHolder
  {
    HttpClient client;

    ClientHolder()
    {
      try {
        this.client = HttpClientInit.createClient(
            new HttpClientConfig(50, SSLContext.getDefault(), Duration.ZERO),
            new Lifecycle()
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    public HttpClient getClient()
    {
      return client;
    }
  }

  @Path("/slow")
  public static class SlowResource
  {
    @GET
    @Path("/hello")
    @Produces(MediaType.APPLICATION_JSON)
    public Response hello()
    {
      try {
        TimeUnit.MILLISECONDS.sleep(1000);
      }
      catch (InterruptedException e) {
        //
      }
      return Response.ok("hello").build();
    }
  }

  @Path("/default")
  public static class DefaultResource
  {
    @Path("/hello")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get()
    {
      return Response.ok("hello").build();
    }
  }


  @Test
  public void testQos() throws InterruptedException
  {
    int fastthreads = 10;
    int slowThreads = 10;
    final int fastRequests = 200;
    final int slowRequests = 10;
    ExecutorService fastPool = Execs.multiThreaded(fastthreads, "fast-%d");
    ExecutorService slowPool = Execs.multiThreaded(slowThreads, "slow-%d");
    final CountDownLatch latch = new CountDownLatch(fastthreads + slowThreads);

    for (int i = 0; i < slowThreads; i++) {
      slowPool.submit(new Runnable()
      {
        @Override
        public void run()
        {
          for (int i = 0; i < slowRequests; i++) {
            long startTime = System.currentTimeMillis();
            try {
              ListenableFuture<StatusResponseHolder> go =
                  client.go(
                      new Request(HttpMethod.GET, new URL("http://localhost:" + port + "/slow/hello")),
                      new StatusResponseHandler(Charset.defaultCharset())
                  );
              go.get();
            }
            catch (Exception e) {
              e.printStackTrace();
            }
            finally {
              System.out
                  .println(
                      "Response time slow client "
                      + (System.currentTimeMillis() - startTime)
                  );
            }
          }
          latch.countDown();
        }
      });
    }

    // sleep for 5 secs for the slow requests to pile up.
    Thread.sleep(5000);

    for (int i = 0; i < fastthreads; i++) {
      fastPool.submit(new Runnable()
      {
        @Override
        public void run()
        {
          for (int i = 0; i < fastRequests; i++) {
            long startTime = System.currentTimeMillis();
            try {
              ListenableFuture<StatusResponseHolder> go =
                  client.go(
                      new Request(HttpMethod.GET, new URL("http://localhost:" + port + "/hello")),
                      new StatusResponseHandler(Charset.defaultCharset())
                  );
              go.get();
            }
            catch (Exception e) {
              e.printStackTrace();
            }
            finally {
              System.out
                  .println(
                      "Response time fast client "
                      + (System.currentTimeMillis() - startTime)
                  );
            }
          }
          latch.countDown();
        }
      });
    }

    latch.await();
  }


}
