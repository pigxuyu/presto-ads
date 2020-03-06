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
package com.facebook.presto.plugin.kylin;

import com.facebook.presto.optimize.OptimizeConstant;
import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.kylin.jdbc.Driver;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.sql.*;
import java.util.*;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Locale.ENGLISH;

public class KylinClient extends BaseJdbcClient {

    private boolean metaNameUpperCase;

    public String jdbcUrl;

    public String jdbcUser;

    public String jdbcPassword;

    @Inject
    public KylinClient(JdbcConnectorId connectorId, BaseJdbcConfig config, KylinConfig kylinConfig) throws Exception {
        super(connectorId, config, "", connectionFactory(config, kylinConfig));
        this.metaNameUpperCase = kylinConfig.isMetaNameUpperCase();
        this.jdbcUrl = kylinConfig.getOptimizeStorageConnectionUrl();
        this.jdbcUser = kylinConfig.getOptimizeStorageConnectionUser();
        this.jdbcPassword = kylinConfig.getOptimizeStorageConnectionPassword();
    }

    @SuppressWarnings("Duplicates")
    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, KylinConfig kylinConfig) throws Exception {
        Properties connectionProperties = basicConnectionProperties(config);
        Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").getConstructor().newInstance();
        return new DriverConnectionFactory(driver, config.getConnectionUrl(), connectionProperties);
    }

    @Override
    public Set<String> getSchemaNames() {
        try (Connection connection = connectionFactory.openConnection();
             ResultSet resultSet = connection.getMetaData().getSchemas(null, "%")) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM").toLowerCase(ENGLISH);
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName) {
        try (Connection connection = connectionFactory.openConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            if (metadata.storesUpperCaseIdentifiers() || metaNameUpperCase) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase(ENGLISH);
                jdbcTableName = jdbcTableName.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName)) {
                List<JdbcTableHandle> tableHandles = new ArrayList<JdbcTableHandle>();
                while (resultSet.next()) {
                    tableHandles.add(new KylinJdbcTableHandle(
                            connectorId,
                            schemaTableName,
                            null,
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
        JdbcTypeHandle countTypeHandle = new JdbcTypeHandle(-5, -1, -1);
        Optional<ReadMapping> countColumnMapping = toPrestoType(session, countTypeHandle);
        try (Connection connection = connectionFactory.openConnection()) {
            String escape = connection.getMetaData().getSearchStringEscape();
            try (ResultSet resultSet = connection.getMetaData().getColumns(tableHandle.getCatalogName(), escapeNamePattern(tableHandle.getSchemaName(), escape), escapeNamePattern(tableHandle.getTableName(), escape), null)) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"));
                    Optional<ReadMapping> columnMapping = toPrestoType(session, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle, columnMapping.get().getType()));

                        columns.add(new JdbcColumnHandle(connectorId, OptimizeConstant.COUNT + columnName, countTypeHandle, countColumnMapping.get().getType()));
                        columns.add(new JdbcColumnHandle(connectorId, OptimizeConstant.COUNT_DISTINCT + columnName, countTypeHandle, countColumnMapping.get().getType()));
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases (e.g. PostgreSQL) a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                columns.add(new JdbcColumnHandle(connectorId, OptimizeConstant.COUNT + OptimizeConstant.ALL, countTypeHandle, countColumnMapping.get().getType()));
                columns.add(new JdbcColumnHandle(connectorId, OptimizeConstant.COUNT_DISTINCT + OptimizeConstant.ALL, countTypeHandle, countColumnMapping.get().getType()));
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles, String queryId) throws SQLException {
        return new KylinQueryBuilder(identifierQuote).buildSql(
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
        KylinJdbcTableHandle tableHandle = (KylinJdbcTableHandle) layoutHandle.getTable();
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
