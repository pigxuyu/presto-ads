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
package com.facebook.presto.plugin.hbase;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;

import javax.inject.Inject;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Manages the Hbase connector specific metadata information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns.
 * of additional columns.
 */
public class HbaseMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(HbaseMetadata.class);

    private final String connectorId;
    private final HbaseConnectorConfig hbaseConnectorConfig;
    private final HbaseClientManager hbaseCientManager;
    private final TypeManager typeManager;
    private final Set<String> defaultSchemaList;

    @Inject
    HbaseMetadata(
            HbaseConnectorId connectorId,
            HbaseConnectorConfig hbaseConnectorConfig,
            HbaseClientManager hbaseCientManager,
            TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.hbaseConnectorConfig = requireNonNull(hbaseConnectorConfig, "hbaseConnectorConfig is null");
        this.hbaseCientManager = requireNonNull(hbaseCientManager, "hbaseCientManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.defaultSchemaList = requireNonNull(hbaseConnectorConfig.getDefaultSchemas(), "hbase config default schema is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        List<String> schemas = Lists.newArrayList(defaultSchemaList.iterator());
        try {
            NamespaceDescriptor[] nsList = hbaseCientManager.getHBaseClient().listNamespaceDescriptors();
            for (NamespaceDescriptor ns : nsList) {
                schemas.add(ns.getName());
            }
        } catch (Exception e) {
            log.error(e, "listSchemaNames failed");
        }
        return ImmutableList.copyOf(schemas);
    }

    @Override
    public HbaseTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        HTableDescriptor table = null;
        try {
            table = hbaseCientManager.getHBaseClient().getTableDescriptor(TableName.valueOf(schemaTableName.getSchemaName(), schemaTableName.getTableName()));
        } catch (Exception e) {
            log.error(e, "listSchemaNames failed");
        }
        if (table == null) {
            return null;
        }
        return new HbaseTableHandle(table.getTableName().getNamespaceAsString(), table.getTableName().getQualifierAsString());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName schemaTableName = convertTableHandle(tableHandle).getSchemaTableName();
        return getTableMetadata(session, schemaTableName);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        HbaseTableHandle tableHandle = convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(new HbaseTableLayoutHandle(tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        HbaseTableLayoutHandle layout = convertLayout(handle);
        // tables in this connector have a single layout
        return getTableLayouts(session, layout.getTable(), Constraint.alwaysTrue(), Optional.empty())
                .get(0)
                .getTableLayout();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        try {
            for (TableName table : hbaseCientManager.getHBaseClient().listTableNamesByNamespace(schemaNameOrNull)) {
                builder.add(new SchemaTableName(table.getNamespaceAsString(), table.getQualifierAsString()));
            }
        } catch (Exception e) {
            log.error(e, "listTables failed");
        }
        return builder.build();
    }

    @SuppressWarnings("Duplicates")
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HbaseTableHandle hbaseTableHandle = convertTableHandle(tableHandle);
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        if (tableMetadata == null) {
            throw new TableNotFoundException(hbaseTableHandle.getSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        List<ColumnMetadata> columnMetadataList = tableMetadata.getColumns();
        if (columnMetadataList != null) {
            for (ColumnMetadata columnMetadata : columnMetadataList) {
                columnHandles.put(columnMetadata.getName(), new HbaseColumnHandle(columnMetadata.getName(), columnMetadata.getType()));
            }
        }
        return columnHandles.build();
    }

    @SuppressWarnings("Duplicates")
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames;
        if (prefix.getTableName() == null) {
            tableNames = listTables(session, prefix.getSchemaName());
        }
        else {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        HTableDescriptor table = null;
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        try {
            table = hbaseCientManager.getHBaseClient().getTableDescriptor(TableName.valueOf(tableName.getSchemaName(), tableName.getTableName()));
            columns.add(new ColumnMetadata(HbaseConstant.ROW_KEY_COLUMN_NAME, typeManager.getType(TypeSignature.parseTypeSignature("varchar"))));
            HColumnDescriptor[] columnFamilys = table.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilys) {
                columns.add(new ColumnMetadata(columnFamily.getNameAsString(), typeManager.getType(TypeSignature.parseTypeSignature("varchar"))));
            }
        } catch (Exception e) {
            log.error(e, "getTableMetadata failed");
        }
        if (table == null) {
            throw new TableNotFoundException(tableName);
        }
        return new ConnectorTableMetadata(tableName, columns.build());
    }

    static HbaseTableHandle convertTableHandle(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof HbaseTableHandle, "tableHandle is not an instance of HbaseTableHandle");
        return (HbaseTableHandle) tableHandle;
    }

    static HbaseColumnHandle convertColumnHandle(ColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof HbaseColumnHandle, "columnHandle is not an instance of HbaseColumnHandle");
        return (HbaseColumnHandle) columnHandle;
    }

    static HbaseSplit convertSplit(ConnectorSplit split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split instanceof HbaseSplit, "split is not an instance of HbaseSplit");
        return (HbaseSplit) split;
    }

    static HbaseTableLayoutHandle convertLayout(ConnectorTableLayoutHandle layout)
    {
        requireNonNull(layout, "layout is null");
        checkArgument(layout instanceof HbaseTableLayoutHandle, "layout is not an instance of HbaseTableLayoutHandle");
        return (HbaseTableLayoutHandle) layout;
    }
}
