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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static java.lang.String.format;

public class TestingGreenplumServer
        implements Closeable
{
    private static final String USER = "gpadmin";
    private static final String PASSWORD = "gpadmin";
    private static final String DATABASE = "tpch";
    private static final int PORT = 5432;

    private final GenericContainer dockerContainer;

    public TestingGreenplumServer()
    {
        dockerContainer = new GenericContainer<>("prestodev/gpdb-6");
        dockerContainer.addEnv("DATABASE", "tpch");
        dockerContainer.waitingFor(Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(10)));
        dockerContainer.start();
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
        // TODO we should encode user and password in JDBC url, instead connection-user and connection-password catalog properties should be used
        return format("jdbc:postgresql://%s:%s/%s?user=%s&password=%s", dockerContainer.getContainerIpAddress(), dockerContainer.getMappedPort(PORT), DATABASE, USER, PASSWORD);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
