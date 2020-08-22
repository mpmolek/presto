/*
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
package io.prestosql.plugin.greenplum;

import io.airlift.log.Logger;
import org.testcontainers.containers.GenericContainer;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class TestingGreenplumServer
        implements Closeable
{
    private static final Logger LOG = Logger.get(TestingGreenplumServer.class);

    private static final String USER = "gpadmin";
    private static final String PASSWORD = "gpadmin";
    private static final String DATABASE = "tpch";
    private static final int PORT = 5432;

    private final GenericContainer dockerContainer;

    public TestingGreenplumServer()
    {
        LOG.info("Starting GPDB docker container");
        dockerContainer = new GenericContainer<>("gpdb:latest");
        dockerContainer.start();
        LOG.info("Started GPDB docker container");

        while (true) {
            try {
                // TODO Should just make the database configurable, but we will still want to wait for it to be ready
                // Or otherwise find out how test containers will wait on `start` to continue the flow
                execute(getJdbcUrl(), "CREATE DATABASE tpch");
                LOG.info("Created database tpch");
                break;
            }
            catch (SQLException e) {
                LOG.info(format("Caught exception trying to create database; will retry: %s", e));
                try {
                    Thread.sleep(5000);
                }
                catch (InterruptedException interruptedException) {
                    Thread.interrupted();
                }
            }
        }
    }

    public void execute(String sql)
            throws SQLException
    {
        execute(getJdbcUrl(), sql);
    }

    private static void execute(String url, String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(url);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    public String getJdbcUrl()
    {
        // TODO we should not encode user and password in JDBC url, instead connection-user and connection-password catalog properties should be used
        String postgresConnect = format("jdbc:postgresql://%s:%s/%s?user=%s&password=%s", dockerContainer.getContainerIpAddress(), dockerContainer.getMappedPort(PORT), "gpadmin", USER, PASSWORD);
        String gbpdConnect = format("jdbc:pivotal:greenplum://%s:%s/%s?user=%s&password=%s", dockerContainer.getContainerIpAddress(), dockerContainer.getMappedPort(PORT), "gpadmin", USER, PASSWORD);
        return postgresConnect;
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
