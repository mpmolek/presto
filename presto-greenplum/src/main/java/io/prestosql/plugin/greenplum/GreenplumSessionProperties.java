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
import io.prestosql.plugin.jdbc.SessionPropertiesProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.prestosql.spi.session.PropertyMetadata.enumProperty;

public final class GreenplumSessionProperties
        implements SessionPropertiesProvider
{
    private static final String ARRAY_MAPPING = "array_mapping";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public GreenplumSessionProperties(GreenplumConfig greenplumConfig)
    {
        sessionProperties = ImmutableList.of(
                enumProperty(
                        ARRAY_MAPPING,
                        "Handling of PostgreSql arrays",
                        GreenplumConfig.ArrayMapping.class,
                        greenplumConfig.getArrayMapping(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static GreenplumConfig.ArrayMapping getArrayMapping(ConnectorSession session)
    {
        return session.getProperty(ARRAY_MAPPING, GreenplumConfig.ArrayMapping.class);
    }
}
