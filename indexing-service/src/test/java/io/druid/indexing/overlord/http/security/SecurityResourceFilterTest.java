package io.druid.indexing.overlord.http.security;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.sun.jersey.spi.container.ResourceFilter;
import io.druid.audit.AuditManager;
import io.druid.common.config.JacksonConfigManager;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskStorageQueryAdapter;
import io.druid.indexing.overlord.http.OverlordResource;
import io.druid.indexing.worker.Worker;
import io.druid.indexing.worker.WorkerCuratorCoordinator;
import io.druid.indexing.worker.http.WorkerResource;
import io.druid.server.http.security.AbstractResourceFilter;
import io.druid.server.http.security.ResourceFilterTestHelper;
import io.druid.tasklogs.TaskLogStreamer;
import java.lang.reflect.Field;
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
            getRequestPaths(OverlordResource.class, ImmutableList.<Class<?>>of(
                TaskMaster.class,
                TaskStorageQueryAdapter.class,
                TaskLogStreamer.class,
                JacksonConfigManager.class,
                AuditManager.class
            )),
            getRequestPaths(WorkerResource.class, ImmutableList.<Class<?>>of(
                Worker.class,
                WorkerCuratorCoordinator.class,
                TaskRunner.class
            ))
        )
    );
  }

  private final String requestPath;
  private final String requestMethod;
  private final ResourceFilter resourceFilter;
  private final TaskStorageQueryAdapter tsqa;
  private final Task noopTask = new NoopTask(null, 0, 0, null, null, null);

  public SecurityResourceFilterTest(
      String requestPath,
      String requestMethod,
      ResourceFilter resourceFilter
  )
  {
    this.requestPath = requestPath;
    this.requestMethod = requestMethod;
    this.resourceFilter = resourceFilter;
    tsqa = EasyMock.createStrictMock(TaskStorageQueryAdapter.class);
  }

  @Before
  public void setUp() throws Exception
  {
    if (resourceFilter instanceof TaskResourceFilter) {
      EasyMock.expect(tsqa.getTask(EasyMock.anyString())).andReturn(Optional.of(noopTask)).anyTimes();
      Field tsqaField = TaskResourceFilter.class.getDeclaredField("taskStorageQueryAdapter");
      tsqaField.setAccessible(true);
      tsqaField.set(resourceFilter, tsqa);
    }
    setUp(resourceFilter);
  }

  @Test
  public void testDatasourcesResourcesFilteringAccess()
  {
    setUpMockExpectations(requestPath, true, requestMethod);
    EasyMock.expect(request.getEntity(Task.class)).andReturn(noopTask).anyTimes();
    // As request object is a strict mock the ordering of expected calls matters
    // therefore adding the expectation below again as getEntity is called before getMethod
    EasyMock.expect(request.getMethod()).andReturn(requestMethod).anyTimes();
    EasyMock.replay(req, request, authorizationInfo, tsqa);
    resourceFilter.getRequestFilter().filter(request);
    Assert.assertTrue(((AbstractResourceFilter) resourceFilter.getRequestFilter()).isApplicable(requestPath));
  }

  @Test(expected = WebApplicationException.class)
  public void testDatasourcesResourcesFilteringNoAccess()
  {
    setUpMockExpectations(requestPath, false, requestMethod);
    EasyMock.expect(request.getEntity(Task.class)).andReturn(noopTask).anyTimes();
    EasyMock.expect(request.getMethod()).andReturn(requestMethod).anyTimes();
    EasyMock.replay(req, request, authorizationInfo, tsqa);
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
    final String badRequestPath = requestPath.replaceAll("\\w+", "droid");
    EasyMock.expect(request.getPath()).andReturn(badRequestPath).anyTimes();
    EasyMock.replay(req, request, authorizationInfo, tsqa);
    Assert.assertFalse(((AbstractResourceFilter) resourceFilter.getRequestFilter()).isApplicable(badRequestPath));
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(req, request, authorizationInfo, tsqa);
  }

}
