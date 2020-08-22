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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class GreenplumConfig
{
    private ArrayMapping arrayMapping = ArrayMapping.DISABLED;
    private boolean includeSystemTables;
    private boolean useGPDBDriver;

    public enum ArrayMapping
    {
        DISABLED,
        AS_ARRAY,
        AS_JSON,
    }

    @NotNull
    public ArrayMapping getArrayMapping()
    {
        return arrayMapping;
    }

    @Config("greenplum.array-mapping")
    public GreenplumConfig setArrayMapping(ArrayMapping arrayMapping)
    {
        this.arrayMapping = arrayMapping;
        return this;
    }

    public boolean isIncludeSystemTables()
    {
        return includeSystemTables;
    }

    @Config("greenplum.include-system-tables")
    public GreenplumConfig setIncludeSystemTables(boolean includeSystemTables)
    {
        this.includeSystemTables = includeSystemTables;
        return this;
    }

    public boolean isUseGPDBDriver()
    {
        return useGPDBDriver;
    }

    @Config("greenplum.use-gpdb-driver")
    public GreenplumConfig setUseGPDBDriver(boolean useGPDBDriver)
    {
        this.useGPDBDriver = useGPDBDriver;
        return this;
    }
}
