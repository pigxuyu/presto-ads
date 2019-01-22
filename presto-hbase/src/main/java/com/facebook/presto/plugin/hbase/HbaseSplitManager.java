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
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Hbase specific implementation of {@link ConnectorSplitManager}.
 */
public class HbaseSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final HbaseConnectorConfig hbaseConnectorConfig;
    private final HbaseClientManager hbaseClientManager;

    @Inject
    public HbaseSplitManager(
            HbaseConnectorId connectorId,
            HbaseConnectorConfig hbaseConnectorConfig,
            HbaseClientManager hbaseClientManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.hbaseConnectorConfig = requireNonNull(hbaseConnectorConfig, "hbaseConfig is null");
        this.hbaseClientManager = requireNonNull(hbaseClientManager, "hbaseClientManager is null");
    }

    @SuppressWarnings("Duplicates")
    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        HbaseTableHandle hbaseTableHandle = HbaseMetadata.convertLayout(layout).getTable();

        List<String> nodes = Lists.newArrayList(hbaseConnectorConfig.getHbaseZkNodes());
        checkState(!nodes.isEmpty(), "No Hbase nodes available");
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        HbaseTableLayoutHandle hbaseTableLayoutHandle = (HbaseTableLayoutHandle) layout;

        HbaseSplit split = new HbaseSplit(connectorId,
                hbaseTableHandle.getSchemaName(),
                hbaseTableHandle.getTableName(),
                hbaseTableLayoutHandle.getTupleDomain());

        builder.add(split);
        return new FixedSplitSource(builder.build());
    }
}
