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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class JdbcSplit
        implements ConnectorSplit
{
    protected final String connectorId;
    protected final String catalogName;
    protected final String schemaName;
    protected final String tableName;
    protected final String tableAliasName;
    protected final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public JdbcSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableAliasName") @Nullable String tableAliasName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "table name is null");
        this.tableAliasName = tableAliasName;
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
    }

    public JdbcSplit(String connectorId, String catalogName, String schemaName, String tableName, TupleDomain<ColumnHandle> tupleDomain)
    {
        this(connectorId, catalogName, schemaName, tableName, "", tupleDomain);
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @Nullable
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    @Nullable
    public String getTableAliasName()
    {
        return tableAliasName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
