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

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.remote.Driver;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.charReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.varcharReadMapping;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Locale.ENGLISH;

public class DruidClient extends BaseJdbcClient {

    private boolean metaNameUpperCase;

    public String jdbcUrl;

    public String jdbcUser;

    public String jdbcPassword;

    @Inject
    public DruidClient(JdbcConnectorId connectorId, BaseJdbcConfig config, DruidConfig druidConfig) throws Exception {
        super(connectorId, config, "\"", connectionFactory(config, druidConfig));
        this.metaNameUpperCase = druidConfig.isMetaNameUpperCase();
        this.jdbcUrl = druidConfig.getOptimizeStorageConnectionUrl();
        this.jdbcUser = druidConfig.getOptimizeStorageConnectionUser();
        this.jdbcPassword = druidConfig.getOptimizeStorageConnectionPassword();
    }

    @SuppressWarnings("Duplicates")
    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, DruidConfig druidConfig) throws Exception {
        Properties connectionProperties = basicConnectionProperties(config);
        Driver driver = (Driver) Class.forName("org.apache.calcite.avatica.remote.Driver").getConstructor().newInstance();
        return new DriverConnectionFactory(driver, config.getConnectionUrl(), connectionProperties);
    }

    @Override
    public void abortReadConnection(Connection connection) throws SQLException {
        connection.abort(directExecutor());
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        if (statement.isWrapperFor(Statement.class)) {
            statement.unwrap(Statement.class);
        }
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                null,
                schemaName,
                tableName,
                new String[]{"TABLE", "VIEW"});
    }

    @Override
    protected String toSqlType(Type type) {
        return super.toSqlType(type);
    }

    @SuppressWarnings("Duplicates")
    @Override
    public List<SchemaTableName> getTableNames(@Nullable String schema) {
        try (Connection connection = connectionFactory.openConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metaNameUpperCase && (schema != null)) {
                schema = schema.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, schema, null)) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    list.add(getSchemaTableName(resultSet));
                }
                return list.build();
            }
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @SuppressWarnings("Duplicates")
    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName) {
        try (Connection connection = connectionFactory.openConnection()) {
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            if (metaNameUpperCase) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase(ENGLISH);
                jdbcTableName = jdbcTableName.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName)) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new DruidJdbcTableHandle(
                            connectorId,
                            schemaTableName,
                            resultSet.getString("TABLE_CAT"),
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME"),
                            ""));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @SuppressWarnings("Duplicates")
    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle) {
        try (Connection connection = connectionFactory.openConnection()) {
            try (ResultSet resultSet = connection.getMetaData().getColumns(tableHandle.getCatalogName(), tableHandle.getSchemaName(), tableHandle.getTableName(), null)) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(resultSet.getInt("DATA_TYPE"), resultSet.getInt("COLUMN_SIZE"), resultSet.getInt("DECIMAL_DIGITS"));
                    Optional<ReadMapping> columnMapping = toPrestoType(session, typeHandle);
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle, columnMapping.get().getType()));
                    }
                }
                if (columns.isEmpty()) {
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle) {
        int columnSize = typeHandle.getColumnSize();
        switch (typeHandle.getJdbcType()) {
            case Types.CHAR:
            case Types.NCHAR:
                int charLength = Math.min(CharType.MAX_LENGTH, CharType.MAX_LENGTH);
                return Optional.of(charReadMapping(createCharType(charLength)));
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH) {
                    return Optional.of(varcharReadMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharReadMapping(createVarcharType(VarcharType.MAX_LENGTH)));
            default:
                return super.toPrestoType(session, typeHandle);
        }
    }

    @Override
    public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles, String queryId) throws SQLException {
        return new DruidQueryBuilder(identifierQuote).buildSql(
                this,
                connection,
                split.getConnectorId(),
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                split.getTableAliasName(),
                columnHandles,
                split.getTupleDomain(),
                queryId);
    }

    @Override
    public ConnectorSplitSource getSplits(JdbcTableLayoutHandle layoutHandle) {
        DruidJdbcTableHandle tableHandle = (DruidJdbcTableHandle) layoutHandle.getTable();
        JdbcSplit jdbcSplit = new JdbcSplit(
                connectorId,
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getAliasName(),
                layoutHandle.getTupleDomain());
        return new FixedSplitSource(ImmutableList.of(jdbcSplit));
    }
}
