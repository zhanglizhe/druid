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

package io.druid.server;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.IAE;
import com.metamx.emitter.EmittingLogger;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryWatcher;
import io.druid.query.TableDataSource;
import io.druid.query.UnionDataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class QueryManager implements QueryWatcher
{
  private static final EmittingLogger log = new EmittingLogger(QueryManager.class);

  final SetMultimap<String, ListenableFuture> queries;
  final Map<String, List<String>> queryDatasources;

  public QueryManager()
  {
    this.queries = Multimaps.synchronizedSetMultimap(
        HashMultimap.<String, ListenableFuture>create()
    );
    this.queryDatasources = new ConcurrentHashMap<>();
  }

  public boolean cancelQuery(String id) {
    queryDatasources.remove(id);
    Set<ListenableFuture> futures = queries.removeAll(id);
    boolean success = true;
    for (ListenableFuture future : futures) {
      success = success && future.cancel(true);
    }
    return success;
  }

  public void registerQuery(Query query, final ListenableFuture future)
  {
    final String id = query.getId();
    queries.put(id, future);
    if(queryDatasources.get(id) == null) {
      queryDatasources.put(id, getDataSources(query));
    }
    future.addListener(
        new Runnable()
        {
          @Override
          public void run()
          {
            queries.remove(id, future);
            queryDatasources.remove(id);
          }
        },
        MoreExecutors.sameThreadExecutor()
    );
  }

  public List<String> getQueryDatasources(final String queryId) {
    return queryDatasources.get(queryId);
  }

  public List<String> getDataSources(final Query query) {
    List<String> datasources = new ArrayList<>();
    getDataSourcesHelper(query, datasources);
    return datasources;
  }

  private void getDataSourcesHelper(final Query query, List<String> datasources) {
    if (query.getDataSource() instanceof TableDataSource) {
      // there will only be one datasource for TableDataSource
      datasources.addAll(query.getDataSource().getNames());
    } else if (query.getDataSource() instanceof UnionDataSource) {
      for (DataSource ds : ((UnionDataSource) query.getDataSource()).getDataSources()) {
        datasources.addAll(ds.getNames());
      }
    } else if (query.getDataSource() instanceof QueryDataSource) {
      getDataSourcesHelper(((QueryDataSource) query.getDataSource()).getQuery(), datasources);
    } else {
      throw new IAE("Do not know how to extract datasource information from this query type [%s]", query.getClass());
    }
  }
}
