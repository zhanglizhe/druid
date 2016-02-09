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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.metamx.common.StringUtils;
import io.druid.audit.AuditInfo;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.namespace.cache.LookupCoordinatorManager;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class LookupCoordinatorResourceTest
{
  private static final ObjectMapper mapper = new DefaultObjectMapper();
  private static final Map<String, Map<String, Object>> SINGLE_LOOKUP_MAP = ImmutableMap.<String, Map<String, Object>>of(
      "lookupName",
      ImmutableMap.<String, Object>of()
  );
  private static final Map<String, Map<String, Map<String, Object>>> SINGLE_TIER_MAP = ImmutableMap.<String, Map<String, Map<String, Object>>>of(
      "lookupTier",
      SINGLE_LOOKUP_MAP
  );
  private static final ByteSource SINGLE_TIER_MAP_SOURCE = new ByteSource()
  {
    @Override
    public InputStream openStream() throws IOException
    {
      return new ByteArrayInputStream(StringUtils.toUtf8(mapper.writeValueAsString(SINGLE_TIER_MAP)));
    }
  };

  @Test
  public void testSimpleGet()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    final Map<String, Map<String, Map<String, Object>>> retVal = new HashMap<>();
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(retVal).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    final Response response = lookupCoordinatorResource.getTiers();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(retVal.keySet(), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testMissingGet()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(null).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    final Response response = lookupCoordinatorResource.getTiers();
    Assert.assertEquals(404, response.getStatus());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testExceptionalGet()
  {
    final String errMsg = "some error";
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andThrow(new RuntimeException(errMsg)).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    final Response response = lookupCoordinatorResource.getTiers();
    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testSimpleGetNamespace()
  {
    final String tier = "some tier";
    final String namespace = "some namespace";
    final Map<String, Object> map = new HashMap<>();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getLookup(EasyMock.eq(tier), EasyMock.eq(namespace))).andReturn(map).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    final Response response = lookupCoordinatorResource.getSpecificNamespace(tier, namespace);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(map, response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testMissingGetNamespace()
  {
    final String tier = "some tier";
    final String namespace = "some namespace";
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getLookup(EasyMock.eq(tier), EasyMock.eq(namespace))).andReturn(null).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    final Response response = lookupCoordinatorResource.getSpecificNamespace(tier, namespace);
    Assert.assertEquals(404, response.getStatus());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testInvalidGetNamespace()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    Assert.assertEquals(400, lookupCoordinatorResource.getSpecificNamespace("foo", null).getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.getSpecificNamespace("foo", "").getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.getSpecificNamespace("", "foo").getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.getSpecificNamespace(null, "foo").getStatus());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testExceptionalGetNamespace()
  {
    final String errMsg = "some message";
    final String tier = "some tier";
    final String namespace = "some namespace";
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getLookup(EasyMock.eq(tier), EasyMock.eq(namespace)))
            .andThrow(new RuntimeException(errMsg))
            .once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    final Response response = lookupCoordinatorResource.getSpecificNamespace(tier, namespace);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testSimpleDelete()
  {
    final String tier = "some tier";
    final String namespace = "some namespace";
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.deleteLookup(
        EasyMock.eq(tier),
        EasyMock.eq(namespace),
        EasyMock.capture(auditInfoCapture)
    )).andReturn(true).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    final Response response = lookupCoordinatorResource.deleteNamespace(tier, namespace, author, comment, request);

    Assert.assertEquals(202, response.getStatus());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }


  @Test
  public void testMissingDelete()
  {
    final String tier = "some tier";
    final String namespace = "some namespace";
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.deleteLookup(
        EasyMock.eq(tier),
        EasyMock.eq(namespace),
        EasyMock.capture(auditInfoCapture)
    )).andReturn(false).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    final Response response = lookupCoordinatorResource.deleteNamespace(tier, namespace, author, comment, request);

    Assert.assertEquals(404, response.getStatus());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }


  @Test
  public void testExceptionalDelete()
  {
    final String tier = "some tier";
    final String namespace = "some namespace";
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";
    final String errMsg = "some error";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.deleteLookup(
        EasyMock.eq(tier),
        EasyMock.eq(namespace),
        EasyMock.capture(auditInfoCapture)
    )).andThrow(new RuntimeException(errMsg)).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    final Response response = lookupCoordinatorResource.deleteNamespace(tier, namespace, author, comment, request);

    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testInvalidDelete()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    Assert.assertEquals(400, lookupCoordinatorResource.deleteNamespace("foo", null, null, null, null).getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.deleteNamespace(null, null, null, null, null).getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.deleteNamespace(null, "foo", null, null, null).getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.deleteNamespace("foo", "", null, null, null).getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.deleteNamespace("", "foo", null, null, null).getStatus());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testSimpleNew() throws Exception
  {
    final String namespace = "some namespace";
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getContentType()).andReturn(MediaType.APPLICATION_JSON).once();
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();
    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    lookupCoordinatorManager.updateLookups(
        EasyMock.eq(SINGLE_TIER_MAP),
        EasyMock.capture(auditInfoCapture)
    );
    EasyMock.expectLastCall().once();

    EasyMock.replay(lookupCoordinatorManager, request);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    final Response response = lookupCoordinatorResource.updateAllNamespaces(SINGLE_TIER_MAP_SOURCE.openStream(), author, comment, request);

    Assert.assertEquals(202, response.getStatus());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testExceptionalNew() throws Exception
  {
    final String namespace = "some namespace";
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";
    final String errMsg = "some error";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getContentType()).andReturn(MediaType.APPLICATION_JSON).once();
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    lookupCoordinatorManager.updateLookups(
        EasyMock.eq(SINGLE_TIER_MAP),
        EasyMock.capture(auditInfoCapture)
    );
    EasyMock.expectLastCall().andThrow(new RuntimeException(errMsg)).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(lookupCoordinatorManager, mapper, mapper);
    final Response response = lookupCoordinatorResource.updateAllNamespaces(SINGLE_TIER_MAP_SOURCE.openStream(), author, comment, request);

    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }
}
