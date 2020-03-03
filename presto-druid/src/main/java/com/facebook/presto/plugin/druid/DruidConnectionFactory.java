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
package com.facebook.presto.plugin.druid;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Created by Administrator on 2018/11/11.
 */
public class DruidConnectionFactory implements ConnectionFactory {

    private final String connectionUrl;

    private final Properties connectionProperties;

    public DruidConnectionFactory(BaseJdbcConfig config) {
        this(config.getConnectionUrl(), basicConnectionProperties(config));
    }

    public static Properties basicConnectionProperties(BaseJdbcConfig config) {
        Properties connectionProperties = new Properties();
        if (config.getConnectionUser() != null) {
            connectionProperties.setProperty("user", config.getConnectionUser());
        }
        if (config.getConnectionPassword() != null) {
            connectionProperties.setProperty("password", config.getConnectionPassword());
        }
        return connectionProperties;
    }

    public DruidConnectionFactory(String connectionUrl, Properties connectionProperties) {
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "basicConnectionProperties is null"));
    }

    @Override
    public Connection openConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionUrl, connectionProperties);
        checkState(connection != null, "returned null connection");
        return connection;
    }
}
