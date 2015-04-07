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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.metamx.common.StringUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class ExplicitDimRenameFn extends FunctionalExtraction
{
  private final Map<String, String> lookup;

  @JsonCreator
  public ExplicitDimRenameFn(
      @NotNull @JsonProperty("lookup")
      final Map<String, String> lookup,
      @JsonProperty("retainMissingValue")
      final boolean retainMissingValue,
      @Nullable @JsonProperty("replaceMissingValueWith")
      final String replaceMissingValueWith,
      @JsonProperty("injective")
      final boolean injective
  )
  {
    super(
        new Function<String, String>()
        {
          @Nullable
          @Override
          public String apply(@Nullable String input)
          {
            if (Strings.isNullOrEmpty(input)) {
              return null;
            }
            return lookup.get(input);
          }
        },
        retainMissingValue,
        replaceMissingValueWith,
        injective
    );
    this.lookup = Preconditions.checkNotNull(lookup, "lookup");
  }

  private static final byte CACHE_TYPE_ID = 0x5;

  @JsonProperty
  public Map<String, String> getLookup()
  {
    return lookup;
  }

  @Override
  @JsonProperty
  public boolean isRetainMissingValue() {return super.isRetainMissingValue();}

  @Override
  @JsonProperty
  public String getReplaceMissingValueWith() {return super.getReplaceMissingValueWith();}

  @Override
  @JsonProperty
  public boolean isInjective()
  {
    return super.isInjective();
  }

  @Override
  public byte[] getCacheKey()
  {
    try {
      final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      outputStream.write(CACHE_TYPE_ID);
      for(Map.Entry<String, String> entry : lookup.entrySet()){
        final String key = entry.getKey();
        final String val = entry.getValue();
        if(!Strings.isNullOrEmpty(key)){
          outputStream.write(StringUtils.toUtf8(key));
        }
        if(!Strings.isNullOrEmpty(val)){
          outputStream.write(StringUtils.toUtf8(val));
        }
      }
      if(getReplaceMissingValueWith() != null){
        outputStream.write(StringUtils.toUtf8(getReplaceMissingValueWith()));
      }
      outputStream.write(isInjective() ? 1 : 0);
      return outputStream.toByteArray();
    }catch(IOException ex){
      // If ByteArrayOutputStream.write has problems, that is a very bad thing
      throw Throwables.propagate(ex);
    }
  }
}
