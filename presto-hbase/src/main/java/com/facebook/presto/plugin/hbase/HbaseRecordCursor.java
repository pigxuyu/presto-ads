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
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class HbaseRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(HbaseRecordCursor.class);

    private final HbaseSplit split;
    private final List<HbaseColumnHandle> columnHandles;
    private final HBaseClient hbaseClient;
    private final int maxScanCountOnce;
    private ResultScanner resultScanner;
    private Iterator<Result> rowIter;
    private Result currentRowResult;
    private boolean closed;

    HbaseRecordCursor(
            HbaseSplit split,
            List<HbaseColumnHandle> columnHandles,
            HbaseClientManager hbaseThriftManager)
    {
        this.split = split;
        this.columnHandles = columnHandles;
        this.hbaseClient = hbaseThriftManager.getHBaseClient();
        this.maxScanCountOnce = hbaseThriftManager.getHbaseConnectorConfig().getHbaseScanCount();
        faultTolerantFetchData(split.getTupleDomain(), 2);
    }

    private void faultTolerantFetchData(TupleDomain<ColumnHandle> tupleDomain, int retryNum) {
        int retryTimeNum = retryNum + 1;
        while (retryTimeNum-- > 0) {
            try {
                fetchData(tupleDomain);
                break;
            } catch (IOException e) {
                log.error(e, "fetch data throw exception, retry " + retryTimeNum);
                if (resultScanner != null) {
                    resultScanner.close();
                }
                hbaseClient.reConnect();
            }
        }
        if (retryTimeNum == 0) {
            log.error("fetch data failed after retry " + retryNum);
        }
    }

    private void fetchData(TupleDomain<ColumnHandle> tupleDomain) throws IOException {
        List<ByteBuffer> columnFamilys = new ArrayList<>();
        Scan scan = new Scan();
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (HbaseColumnHandle columnHandle : columnHandles) {
            if (!columnHandle.getName().equalsIgnoreCase(HbaseConstant.ROW_KEY_COLUMN_NAME)) {
                columnFamilys.add(ByteBuffer.wrap(columnHandle.getName().getBytes()));
            } else {
                Domain domain = tupleDomain.getDomains().get().get(columnHandle);
                if (domain != null) {
                    for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                        checkState(!range.isAll()); // Already checked
                        if (range.isSingleValue()) {
                            Filter eqFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(((Slice) range.getLow().getValue()).toStringUtf8().getBytes()));
                            filterList.addFilter(eqFilter);
                        }
                        else {
                            FilterList rangeFilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                            if (!range.getLow().isLowerUnbounded()) {
                                switch (range.getLow().getBound()) {
                                    case ABOVE:
                                        Filter greaterFilter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(((Slice) range.getLow().getValue()).toStringUtf8().getBytes()));
                                        rangeFilterList.addFilter(greaterFilter);
                                        break;
                                    case EXACTLY:
                                        Filter greaterOrEqualFilter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(((Slice) range.getLow().getValue()).toStringUtf8().getBytes()));
                                        rangeFilterList.addFilter(greaterOrEqualFilter);
                                        break;
                                    case BELOW:
                                        throw new IllegalArgumentException("Low marker should never use BELOW bound");
                                    default:
                                        throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                                }
                            }
                            if (!range.getHigh().isUpperUnbounded()) {
                                switch (range.getHigh().getBound()) {
                                    case ABOVE:
                                        throw new IllegalArgumentException("High marker should never use ABOVE bound");
                                    case EXACTLY:
                                        Filter lessOrEqualFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(((Slice) range.getHigh().getValue()).toStringUtf8().getBytes()));
                                        rangeFilterList.addFilter(lessOrEqualFilter);
                                        break;
                                    case BELOW:
                                        Filter lessFilter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(((Slice) range.getHigh().getValue()).toStringUtf8().getBytes()));
                                        rangeFilterList.addFilter(lessFilter);
                                        break;
                                    default:
                                        throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                                }
                            }
                            filterList.addFilter(rangeFilterList);
                        }
                    }
                } else {
                    throw new UnsupportedOperationException("hbase not support scan all data");
                }
            }
        }
        scan.setCaching(100);
        scan.setBatch(maxScanCountOnce);
        scan.setFilter(filterList);
        resultScanner = hbaseClient.getScanner(TableName.valueOf(split.getSchemaName(), split.getTableName()).getNameAsString(), scan);
        nextRow();
    }

    private boolean nextRow() throws IOException {
        boolean hasMoreData = false;
        try {
            List<Result> currentRowValues = Lists.newArrayList(resultScanner.next(maxScanCountOnce));
            this.rowIter = currentRowValues.iterator();
            if (currentRowValues != null && currentRowValues.size() != 0) {
                hasMoreData = true;
            }
        } catch (IOException e) {
            log.error(e, "fetch data for next row throw exception");
            throw e;
        }
        return hasMoreData;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            if (rowIter != null) {
                boolean hasNext = rowIter.hasNext();
                if (hasNext) {
                    currentRowResult = rowIter.next();
                    return hasNext;
                }
                if (nextRow()) {
                    return advanceNextPosition();
                }
            }
        } catch (IOException e) {
            log.error(e, "fetch data for next position throw exception");
        }
        return false;
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(!closed, "scanner is closed");
        checkArgument(field < columnHandles.size(), "Invalid field index");
        checkFieldType(field, Slice.class);
        return getFieldValueProvider(columnHandles.get(field).getName());
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(!closed, "scanner is closed");
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return currentRowResult == null;
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    private Slice getFieldValueProvider(String fieldName)
    {
        byte[] value = null;
        if (fieldName.equalsIgnoreCase(HbaseConstant.ROW_KEY_COLUMN_NAME)) {
            value = currentRowResult.getRow();
        } else {
            for (Cell cell : currentRowResult.rawCells()) {
                String columnFamily = Bytes.toStringBinary(CellUtil.cloneFamily(cell));
                if (columnFamily.equalsIgnoreCase(fieldName)) {
                    value = CellUtil.cloneValue(cell);
                }
            }
        }
        if (value == null) {
            throw new IllegalArgumentException("Not found field: " + fieldName);
        }
        return Slices.wrappedBuffer(value, 0, value.length);
    }

    private void checkFieldType(int field, Class<?> expected)
    {
        Class<?> actual = getType(field).getJavaType();
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            resultScanner.close();
        } catch (Exception e) {
            // ignore exception from close
        }
    }
}
