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

import com.facebook.presto.optimize.OptimizeUdfUtil;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.google.common.collect.ImmutableList;

import static java.util.Objects.requireNonNull;

public class DruidMetadata extends JdbcMetadata {

    private final JdbcClient jdbcClient;

    public DruidMetadata(JdbcClient jdbcClient, boolean allowDropTable) {
        super(jdbcClient, allowDropTable);
        this.jdbcClient = requireNonNull(jdbcClient, "client is null");
    }

    @SuppressWarnings("Duplicates")
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        JdbcTableHandle handle = (JdbcTableHandle) table;

        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (JdbcColumnHandle column : jdbcClient.getColumns(session, handle)) {
            String columnName = column.getColumnName();
            if (OptimizeUdfUtil.isOptimizeUdf(columnName)) {
                columnMetadata.add(new ColumnMetadata(columnName, column.getColumnType(), null, true));
            } else {
                columnMetadata.add(column.getColumnMetadata());
            }
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build());
    }
}