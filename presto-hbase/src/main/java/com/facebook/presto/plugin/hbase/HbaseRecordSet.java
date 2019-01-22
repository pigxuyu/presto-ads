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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Hbase specific record set. Returns a cursor for a table which iterates over a Hbase values.
 */
public class HbaseRecordSet
        implements RecordSet
{
    private final HbaseSplit split;
    private final HbaseClientManager hbaseClientManager;
    private final List<HbaseColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    HbaseRecordSet(
            HbaseSplit split,
            HbaseClientManager hbaseClientManager,
            List<HbaseColumnHandle> columnHandles)
    {
        this.split = requireNonNull(split, "split is null");
        this.hbaseClientManager = requireNonNull(hbaseClientManager, "hbaseClientManager is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
        for (HbaseColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }
        this.columnTypes = typeBuilder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new HbaseRecordCursor(split, columnHandles, hbaseClientManager);
    }
}
