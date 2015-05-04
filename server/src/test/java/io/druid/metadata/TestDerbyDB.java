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

package io.druid.metadata;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.common.utils.SocketUtil;
import io.druid.metadata.storage.derby.DerbyConnector;
import org.apache.derby.drda.NetworkServerControl;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;

public class TestDerbyDB extends ExternalResource
{
  private NetworkServerControl networkServerControl = null;
  private Path tmpPath = null;
  private int port;

  @Override
  protected void before() throws Throwable
  {
    port = SocketUtil.findOpenPort(1621);
    tmpPath = Files.createTempFile("derby", ".db");
    tmpPath.toFile().delete();
    networkServerControl = new NetworkServerControl(InetAddress.getByName("localhost"), port);
    networkServerControl.start(null);
    boolean done = false;
    final long start = System.currentTimeMillis();
    while(!done) {
      try {
        networkServerControl.ping();
        done = true;
      }catch(Exception e){
        if(System.currentTimeMillis() - start > 10_000){
          throw Throwables.propagate(e);
        }
      }
    }
    Class.forName("org.apache.derby.jdbc.ClientDriver"); // shakeout errors early
    try (final Connection connection = DriverManager.getConnection(getConnectionString() + ";create=true")) {
      // Try the connection
    }
  }

  @Override
  protected void after()
  {
    if (networkServerControl != null) {
      try {
        networkServerControl.shutdown();
      }
      catch (Exception e) {
        // ignore
      }
    }

    if (tmpPath != null) {
      final File tmpFile = tmpPath.toFile();
      if (tmpFile != null && tmpFile.exists()) {
        if (!tmpFile.delete()) {
          // NOOP
        }
      }
    }
    networkServerControl = null;
    tmpPath = null;
  }

  public String getConnectionString()
  {
    return String.format("jdbc:derby://localhost:%d/%s/testDB", port, tmpPath.toAbsolutePath().toString());
  }

  public MetadataStorageConnectorConfig getConnectorConfig()
  {
    return new MetadataStorageConnectorConfig(
        true,
        "localhost",
        port,
        getConnectionString(),
        "sb",
        new PasswordProvider()
        {
          @Override
          public String getPassword()
          {
            return "sb";
          }
        }
    );
  }

  public DerbyConnector getConnector(Supplier<MetadataStorageTablesConfig> dbTables)
  {
    return new DerbyConnector(
        Suppliers.ofInstance(
            getConnectorConfig()
        ), dbTables
    );
  }
}
