/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.druid.indexing.common.task.Task;

import java.util.List;
import java.util.Map;

/**
 */
public class FillCapacityWithAffinityConfig
{
  public static enum AffinityType {
    DATASOURCE,
    TASK_TYPE
  }

  // key, value:[nodeHostNames]
  private Map<String, List<String>> affinity = Maps.newHashMap();

  private AffinityType affinityType;

  @JsonCreator
  public FillCapacityWithAffinityConfig(
      @JsonProperty("affinity") Map<String, List<String>> affinity,
      @JsonProperty("affinityType") AffinityType affinityType
  )
  {
    this.affinity = affinity;
    this.affinityType = affinityType == null ? AffinityType.DATASOURCE : affinityType;
  }

  @JsonProperty
  public Map<String, List<String>> getAffinity()
  {
    return affinity;
  }

  @JsonProperty
  public AffinityType getAffinityType()
  {
    return affinityType;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FillCapacityWithAffinityConfig that = (FillCapacityWithAffinityConfig) o;

    if (affinity != null
        ? !Maps.difference(affinity, that.affinity).entriesDiffering().isEmpty()
        : that.affinity != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return affinity != null ? affinity.hashCode() : 0;
  }

  public String extractKeyFromTask(Task task){
    if(affinityType.equals(AffinityType.DATASOURCE)){
      return task.getDataSource();
    } else {
      return task.getType();
    }
  }
}
