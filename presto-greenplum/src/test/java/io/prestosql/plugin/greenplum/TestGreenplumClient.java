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

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.postgresql.PostgreSqlClient;
import io.prestosql.plugin.postgresql.TestPostgreSqlClient;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.type.InternalTypeManager;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;

public class TestGreenplumClient
        extends TestPostgreSqlClient
{
    private static final TypeManager TYPE_MANAGER = new InternalTypeManager(createTestMetadataManager());

    @Override
    protected PostgreSqlClient getPostgreSqlClient()
    {
        return new GreenplumClient(
                new BaseJdbcConfig(),
                new GreenplumConfig(),
                identity -> { throw new UnsupportedOperationException(); },
                TYPE_MANAGER);
    }
}
