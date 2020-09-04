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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.postgresql.TestPostgreSqlCaseInsensitiveMapping;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestGreenplumCaseInsensitiveMapping
        extends TestPostgreSqlCaseInsensitiveMapping
{
    private TestingGreenplumServer greenplumServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.greenplumServer = new TestingGreenplumServer();
        return GreenplumQueryRunner.createGreenplumQueryRunner(
                greenplumServer,
                ImmutableMap.of(),
                ImmutableMap.of("case-insensitive-name-matching", "true"),
                ImmutableSet.of());
    }

    @AfterClass(alwaysRun = true)
    public void shutdownServer()
    {
        greenplumServer.close();
    }

    @Override
    protected String getJdbcUrl()
    {
        return greenplumServer.getJdbcUrl();
    }
}
