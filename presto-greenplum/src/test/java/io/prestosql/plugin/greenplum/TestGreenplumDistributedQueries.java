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
import io.prestosql.plugin.postgresql.TestPostgreSqlDistributedQueries;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.greenplum.GreenplumQueryRunner.createGreenplumQueryRunner;

@Test
public class TestGreenplumDistributedQueries
        extends TestPostgreSqlDistributedQueries
{
    private TestingGreenplumServer greenplumServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.greenplumServer = new TestingGreenplumServer();
        return createGreenplumQueryRunner(
                greenplumServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        // caching here speeds up tests highly, caching is not used in smoke tests
                        .put("metadata.cache-ttl", "10m")
                        .put("metadata.cache-missing", "true")
                        .build(),
                TpchTable.getTables());
    }

    @Override
    protected String getJdbcUrl()
    {
        return greenplumServer.getJdbcUrl();
    }

    @AfterClass(alwaysRun = true)
    public void shutdownServer()
    {
        greenplumServer.close();
    }
}
