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

package io.druid.server.namespace.announcer.listener;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.api.client.util.Lists;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.multibindings.MapBinder;
import com.metamx.common.IAE;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;

/**
 * This is a simple announcement resource that handles simple items that have a POST to an announcement endpoint, a
 * GET of something in that endpoint with an ID, and a DELETE to that endpoint with an ID.
 * <p/>
 * The idea of this resource is simply to have a simple endpoint for basic POJO handling assuming the POJO has an ID
 * which distinguishes it from others of its kind.
 * <p/>
 * This resource is expected to NOT block for new items, and is instead expected to make a best effort at returning
 * as quickly as possible.
 */
@Path("/druid/announcement/v1/listen")
public class AnnouncementListenerResource
{

  public static MapBinder<String, AnnouncementPOSTHandler> getPOSTHandlerMap(
      final Binder binder
  )
  {
    return MapBinder.newMapBinder(
        binder,
        String.class,
        AnnouncementPOSTHandler.class,
        POSTHandler.class
    );
  }

  public static MapBinder<String, AnnouncementIDHandler> getGETHandlerMap(
      final Binder binder
  )
  {
    return MapBinder.newMapBinder(
        binder,
        String.class,
        AnnouncementIDHandler.class,
        GETHandler.class
    );
  }

  public static MapBinder<String, AnnouncementIDHandler> getDELETEHandlerMap(
      final Binder binder
  )
  {
    return MapBinder.newMapBinder(
        binder,
        String.class,
        AnnouncementIDHandler.class,
        DELETEHandler.class
    );
  }


  /**
   * Users are highly encouraged to use AbstractAnnouncementPOSTHandler instead.
   */
  public interface AnnouncementPOSTHandler
  {
    Response handle(InputStream inputStream, ObjectMapper mapper);
  }

  public static abstract class AbstractAnnouncementPOSTHandler<InObjType, ReturnObjType> implements
      AnnouncementPOSTHandler
  {
    private final TypeReference<? extends InObjType> inObjTypeRef;

    public AbstractAnnouncementPOSTHandler(TypeReference<? extends InObjType> inObjTypeRef)
    {
      this.inObjTypeRef = inObjTypeRef;
    }

    public final Response handle(final InputStream inputStream, final ObjectMapper mapper)
    {
      final List<InObjType> inObjList;
      try {
        inObjList =
            Lists.newArrayList(
                Iterables.transform(
                    mapper.<List<Map<String, Object>>>readValue(
                        inputStream, new TypeReference<List<Map<String, Object>>>()
                        {
                        }
                    ),
                    new Function<Map<String, Object>, InObjType>()
                    {
                      @Nullable
                      @Override
                      public InObjType apply(Map<String, Object> input)
                      {
                        return mapper.convertValue(input, inObjTypeRef);
                      }
                    }
                )
            );
      }
      catch (final IllegalArgumentException iae) {
        return Response.status(Response.Status.BAD_REQUEST).entity(sanitizeException(iae)).build();
      }
      catch (final IOException e) {
        return Response.status(Response.Status.BAD_REQUEST).entity(sanitizeException(e)).build();
      }
      final ReturnObjType returnObj;
      try {
        returnObj = handle(inObjList);
      }
      catch (Exception e) {
        return Response.serverError().entity(sanitizeException(e)).build();
      }
      return Response.ok().entity(returnObj).build();
    }

    public abstract ReturnObjType handle(List<InObjType> inputObject) throws Exception;
  }

  public interface AnnouncementIDHandler
  {
    Response handle(String id);
  }

  @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  public @interface POSTHandler
  {
  }

  @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  public @interface GETHandler
  {
  }

  @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  public @interface DELETEHandler
  {
  }

  private static Map<String, Object> sanitizeException(Throwable t)
  {
    return ImmutableMap.<String, Object>of("error", t.getMessage());
  }

  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final Map<String, AnnouncementPOSTHandler> postHandlers;
  private final Map<String, AnnouncementIDHandler> getHandlers;
  private final Map<String, AnnouncementIDHandler> deleteHandler;

  @Inject
  public AnnouncementListenerResource(
      final @Json ObjectMapper jsonMapper,
      final @Smile ObjectMapper smileMapper,
      final @POSTHandler Map<String, AnnouncementPOSTHandler> postHandlers,
      final @GETHandler Map<String, AnnouncementIDHandler> getHandlers,
      final @DELETEHandler Map<String, AnnouncementIDHandler> deleteHandlers
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.postHandlers = postHandlers;
    this.getHandlers = getHandlers;
    this.deleteHandler = deleteHandlers;
  }

  @Path("/{announce}")
  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response serviceAnnouncementPOST(
      final @PathParam("announce") String announce,
      final InputStream inputStream,
      final @Context HttpServletRequest req // used only to get request content-type
  )
  {
    final String reqContentType = req.getContentType();
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(reqContentType);
    final ObjectMapper mapper = isSmile ? smileMapper : jsonMapper;
    if (Strings.isNullOrEmpty(announce)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(
                         sanitizeException(
                             new IllegalArgumentException("Cannot have null or empty announcement path")
                         )
                     )
                     .build();
    }
    final AnnouncementPOSTHandler handler = postHandlers.get(announce);
    if (handler == null) {
      return Response.status(Response.Status.BAD_REQUEST).entity(
          sanitizeException(
              new IAE(
                  "Could not find POST announcer for [%s]",
                  announce
              )
          )
      ).build();
    }
    try {
      return handler.handle(inputStream, mapper);
    }
    catch (Exception e) {
      return Response.serverError().entity(sanitizeException(e)).build();
    }
  }


  @Path("/{announce}/{id}")
  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response serviceAnnouncementGET(
      final @PathParam("announce") String announce,
      final @PathParam("id") String id
  )
  {
    if (Strings.isNullOrEmpty(announce)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(
                         sanitizeException(
                             new IllegalArgumentException("Cannot have null or empty announcement path")
                         )
                     )
                     .build();
    }

    if (Strings.isNullOrEmpty(id)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(sanitizeException(new IllegalArgumentException("Cannot have null or empty id")))
                     .build();
    }
    final AnnouncementIDHandler handler = getHandlers.get(announce);
    if (handler == null) {
      return Response.status(Response.Status.BAD_REQUEST).entity(
          sanitizeException(
              new IAE(
                  "Could not find GET announcer for [%s]",
                  announce
              )
          )
      ).build();
    }
    return handler.handle(id);
  }

  @Path("/{announce}/{id}")
  @DELETE
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response serviceAnnouncementDELETE(
      final @PathParam("announce") String announce,
      final @PathParam("id") String id
  )
  {
    if (Strings.isNullOrEmpty(announce)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(
                         sanitizeException(
                             new IllegalArgumentException("Cannot have null or empty announcement path")
                         )
                     )
                     .build();
    }

    if (Strings.isNullOrEmpty(id)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(sanitizeException(new IllegalArgumentException("Cannot have null or empty id")))
                     .build();
    }
    final AnnouncementIDHandler handler = deleteHandler.get(announce);
    if (handler == null) {
      return Response.status(Response.Status.BAD_REQUEST).entity(
          sanitizeException(
              new IAE(
                  "Could not find GET announcer for [%s]",
                  announce
              )
          )
      ).build();
    }
    return handler.handle(id);
  }
}
