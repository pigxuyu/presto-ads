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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Factory for Hbase specific {@link RecordSet} instances.
 */
public class HbaseRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final HbaseClientManager hbaseClientManager;

    @Inject
    public HbaseRecordSetProvider(HbaseClientManager hbaseClientManager)
    {
        this.hbaseClientManager = requireNonNull(hbaseClientManager, "hbaseClientManager is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        HbaseSplit redisSplit = HbaseMetadata.convertSplit(split);

        List<HbaseColumnHandle> hbaseColumns = columns.stream()
                .map(HbaseMetadata::convertColumnHandle)
                .collect(ImmutableList.toImmutableList());

        return new HbaseRecordSet(redisSplit, hbaseClientManager, hbaseColumns);
    }
}
