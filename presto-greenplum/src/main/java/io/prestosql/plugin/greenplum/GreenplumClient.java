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
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.plugin.postgresql.PostgreSqlClient;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import javax.inject.Inject;

import static io.prestosql.spi.type.StandardTypes.JSON;

public class GreenplumClient
        extends PostgreSqlClient
{
    private static final Logger log = Logger.get(GreenplumClient.class);

    private final Type jsonType;

    @Inject
    public GreenplumClient(
            BaseJdbcConfig config,
            GreenplumConfig greenplumConfig,
            ConnectionFactory connectionFactory,
            TypeManager typeManager)
    {
        super(config, greenplumConfig.toPostgreSqlConfig(), connectionFactory, typeManager);
        this.jsonType = typeManager.getType(new TypeSignature(JSON));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping("jsonb", typedVarcharWriteFunction("jsonb"));
        }

        return super.toWriteMapping(session, type);
    }
}
