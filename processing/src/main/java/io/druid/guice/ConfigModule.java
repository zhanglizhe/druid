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

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.config.Config;
import org.skife.config.ConfigurationObjectFactory;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

/**
 */
public class ConfigModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    //被下面的JsonConfigurator用来做生成Java Object的校验
    binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
    //提供根据properties的配置转成一个java object的util方法
    binder.bind(JsonConfigurator.class).in(LazySingleton.class);
  }

  //把property的数据关联到ConfigurationObjectFactory,进而供ConfigProvider使用
  @Provides @LazySingleton
  public ConfigurationObjectFactory makeFactory(Properties props)
  {
    return Config.createFactory(props);
  }
}
