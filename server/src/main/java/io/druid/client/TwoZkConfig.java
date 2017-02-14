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

package io.druid.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.curator.CuratorConfig;

public class TwoZkConfig
{

  @JsonProperty
  private CuratorConfig service1;

  @JsonProperty
  private CuratorConfig service2;

  public CuratorConfig getService1()
  {
    return service1;
  }

  public void setService1(CuratorConfig service1)
  {
    this.service1 = service1;
  }

  public CuratorConfig getService2()
  {
    return service2;
  }

  public void setService2(CuratorConfig service2)
  {
    this.service2 = service2;
  }
}
