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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.postgresql.TestPostgreSqlTypeMapping;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.datatype.DataType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.greenplum.GreenplumQueryRunner.createGreenplumQueryRunner;
import static io.prestosql.testing.datatype.DataType.dataType;
import static io.prestosql.testing.datatype.DataType.formatStringLiteral;
import static io.prestosql.type.JsonType.JSON;
import static java.util.function.Function.identity;

@Test
public class TestGreenplumTypeMapping
        extends TestPostgreSqlTypeMapping
{
    private TestingGreenplumServer greenplumServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        greenplumServer = new TestingGreenplumServer();
        return createGreenplumQueryRunner(
                greenplumServer,
                ImmutableMap.of(),
                ImmutableMap.of("jdbc-types-mapped-to-varchar", "Tsrange, Inet" /* make sure that types are compared case insensitively */),
                ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public void shutdownServer()
    {
        greenplumServer.close();
    }

    @Override
    protected DataType<String> jsonbDataType()
    {
        return dataType(
                "jsonb",
                JSON,
                value -> "JSONB " + formatStringLiteral(value),
                value -> "JSON " + formatStringLiteral(value),
                identity());
    }
}
