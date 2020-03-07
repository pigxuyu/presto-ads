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

import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public class KylinJdbcSplit extends JdbcSplit {

    protected final String tableAliasName;

    @JsonCreator
    public KylinJdbcSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableAliasName") @Nullable String tableAliasName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain) {
        super(connectorId, catalogName, schemaName, tableName, tupleDomain);
        this.tableAliasName = tableAliasName;
    }

    @JsonProperty
    @Nullable
    public String getTableAliasName() {
        return tableAliasName;
    }
}
