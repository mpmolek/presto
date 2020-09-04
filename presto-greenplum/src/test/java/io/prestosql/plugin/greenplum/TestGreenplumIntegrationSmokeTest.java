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

import io.prestosql.plugin.postgresql.TestPostgreSqlIntegrationSmokeTest;
import io.prestosql.testing.QueryRunner;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;

@Test
public class TestGreenplumIntegrationSmokeTest
        extends TestPostgreSqlIntegrationSmokeTest
{
    protected TestingGreenplumServer greenplumServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.greenplumServer = new TestingGreenplumServer();
        return GreenplumQueryRunner.createGreenplumQueryRunner(greenplumServer, CUSTOMER, NATION, ORDERS, REGION);
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

    @Test
    public void testForeignTable()
    {
        throw new SkipException("Foreign tables not supported by GPDB");
    }
}
