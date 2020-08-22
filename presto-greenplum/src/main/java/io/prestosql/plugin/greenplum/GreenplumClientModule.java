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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DecimalModule;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import org.postgresql.Driver;

import java.lang.reflect.InvocationTargetException;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;

public class GreenplumClientModule
        implements Module
{
    private static final Logger log = Logger.get(GreenplumClientModule.class);

    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(GreenplumClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(GreenplumConfig.class);
        bindSessionPropertiesProvider(binder, GreenplumSessionProperties.class);
        binder.install(new DecimalModule());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, GreenplumConfig greenplumConfig, CredentialProvider credentialProvider)
    {
        if (greenplumConfig.isUseGPDBDriver()) {
            java.sql.Driver greenplumDriver;
            try {
                greenplumDriver = (java.sql.Driver) Class.forName("com.pivotal.jdbc.GreenplumDriver").getConstructor().newInstance();
            }
            catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
                String msg = "Failed to instantiate GreenplumDriver";
                log.error(e, msg);
                throw new RuntimeException(msg, e);
            }
            return new DriverConnectionFactory(greenplumDriver, config, credentialProvider);
        }
        else {
            return new DriverConnectionFactory(new Driver(), config, credentialProvider);
        }
    }
}
