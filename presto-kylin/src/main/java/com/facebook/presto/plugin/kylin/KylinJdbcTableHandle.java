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

import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import javax.annotation.Nullable;
import java.util.Objects;

public class KylinJdbcTableHandle extends JdbcTableHandle {

    private final String tableAliasName;

    @JsonCreator
    public KylinJdbcTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableAliasName") String tableAliasName) {

        super(connectorId, schemaTableName, catalogName, schemaName, tableName);
        this.tableAliasName = tableAliasName;
    }

    @JsonProperty
    public String getAliasName() {
        return tableAliasName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        KylinJdbcTableHandle o = (KylinJdbcTableHandle) obj;
        return Objects.equals(this.connectorId, o.connectorId) &&
                Objects.equals(this.schemaTableName, o.schemaTableName) &&
                Objects.equals(this.tableAliasName, o.tableAliasName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, schemaTableName, tableAliasName);
    }

    @Override
    public String toString() {
        return Joiner.on(":").useForNull("null").join(connectorId, schemaTableName, catalogName, schemaName, tableName, tableAliasName);
    }
}
