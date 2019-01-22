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

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.QueryBuilder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.*;
import com.google.common.base.Joiner;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.Float.intBitsToFloat;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.joining;

/**
 * Created by Administrator on 2018/11/5.
 */
public class DruidQueryBuilder extends QueryBuilder {

    private static final Logger log = Logger.get(DruidQueryBuilder.class);

    public DruidQueryBuilder(String quote) {
        super(quote);
    }

    public PreparedStatement buildSql(JdbcClient client, Connection connection, String connectorId, String catalog, String schema, String table, List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain, String queryId)
            throws SQLException {
        StringBuilder sql = new StringBuilder();

        String columnNames = columns.stream()
                .map(JdbcColumnHandle::getColumnName)
                .map(super::quote)
                .collect(joining(", "));

        sql.append("SELECT ");
        sql.append(columnNames);
        if (columns.isEmpty()) {
            sql.append("1");
        }

        sql.append(" FROM ");
        if (!isNullOrEmpty(catalog)) {
            sql.append(quote(catalog)).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        sql.append(quote(table));

        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> clauses = toConjuncts(columns, tupleDomain, accumulator);
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ").append(Joiner.on(" AND ").join(clauses));
        }

        String needReplaceSql = sql.toString();
        String sqlFromFile = getSourceSqlFromFile(connectorId, catalog, schema, table, queryId, columns, clauses);
        if (sqlFromFile != null) {
            needReplaceSql = sqlFromFile.toString();
        } else {
            log.info("not found sql from file");
        }
        for (int i = 0; i < accumulator.size(); i++) {
            TypeAndValue typeAndValue = accumulator.get(i);
            if (typeAndValue.getType().equals(BigintType.BIGINT)) {
                needReplaceSql = needReplaceSql.replaceFirst("\\?", String.valueOf((long) typeAndValue.getValue()));
            } else if (typeAndValue.getType().equals(IntegerType.INTEGER)) {
                needReplaceSql = needReplaceSql.replaceFirst("\\?", String.valueOf(((Number) typeAndValue.getValue()).intValue()));
            } else if (typeAndValue.getType().equals(SmallintType.SMALLINT)) {
                needReplaceSql = needReplaceSql.replaceFirst("\\?", String.valueOf(((Number) typeAndValue.getValue()).shortValue()));
            } else if (typeAndValue.getType().equals(TinyintType.TINYINT)) {
                needReplaceSql = needReplaceSql.replaceFirst("\\?", String.valueOf(((Number) typeAndValue.getValue()).intValue()));
            } else if (typeAndValue.getType().equals(DoubleType.DOUBLE)) {
                needReplaceSql = needReplaceSql.replaceFirst("\\?", String.valueOf((double) typeAndValue.getValue()));
            } else if (typeAndValue.getType().equals(RealType.REAL)) {
                needReplaceSql = needReplaceSql.replaceFirst("\\?", String.valueOf(intBitsToFloat(((Number) typeAndValue.getValue()).intValue())));
            } else if (typeAndValue.getType().equals(BooleanType.BOOLEAN)) {
                needReplaceSql = needReplaceSql.replaceFirst("\\?", String.valueOf((boolean) typeAndValue.getValue()));
            } else if (typeAndValue.getType().equals(DateType.DATE)) {
                long millis = DAYS.toMillis((long) typeAndValue.getValue());
                needReplaceSql = needReplaceSql.replaceFirst("\\?", "MILLIS_TO_TIMESTAMP(" + String.valueOf(millis) + ")");
            } else if (typeAndValue.getType().equals(TimeType.TIME)) {
                needReplaceSql = needReplaceSql.replaceFirst("\\?", "MILLIS_TO_TIMESTAMP(" + String.valueOf((long) typeAndValue.getValue()) + ")");
            } else if (typeAndValue.getType().equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE)) {
                needReplaceSql = needReplaceSql.replaceFirst("\\?", "MILLIS_TO_TIMESTAMP(" + String.valueOf((long) typeAndValue.getValue()) + ")");
            } else if (typeAndValue.getType().equals(TimestampType.TIMESTAMP)) {
                needReplaceSql = needReplaceSql.replaceFirst("\\?", "MILLIS_TO_TIMESTAMP(" + String.valueOf((long) typeAndValue.getValue()) + ")");
            } else if (typeAndValue.getType().equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
                needReplaceSql = needReplaceSql.replaceFirst("\\?", "MILLIS_TO_TIMESTAMP(" + String.valueOf((long) typeAndValue.getValue()) + ")");
            } else if (typeAndValue.getType() instanceof VarcharType) {
                needReplaceSql = needReplaceSql.replaceFirst("\\?", "'" + ((Slice) typeAndValue.getValue()).toStringUtf8() + "'");
            } else {
                throw new UnsupportedOperationException("Can't handle type: " + typeAndValue.getType());
            }
        }
        log.info(table + ": " + needReplaceSql);
        PreparedStatement statement = client.getPreparedStatement(connection, needReplaceSql);
        return statement;
    }

    @SuppressWarnings("Duplicates")
    private String getSourceSqlFromFile(String connectorId, String catalog, String schema, String table, String queryId, List<JdbcColumnHandle> jdbcColumnHandles, List<String> clauses) {

        java.io.ObjectInputStream objinput = null;
        try {
            java.io.File directory = new java.io.File(System.getProperty("java.io.tmpdir") + java.io.File.separatorChar + "prestoPlanTree");
            if (!directory.exists()) {
                return null;
            }

            StringBuilder tableInfo = new StringBuilder();
            if (!isNullOrEmpty(catalog)) {
                tableInfo.append(quote(catalog)).append('.');
            }
            if (!isNullOrEmpty(schema)) {
                tableInfo.append(quote(schema)).append('.');
            }
            tableInfo.append(quote(table));

            String key = (connectorId + "." + schema + "." + table).toLowerCase(java.util.Locale.getDefault());
            objinput = new java.io.ObjectInputStream(new java.io.FileInputStream(new java.io.File(directory, queryId)));
            java.util.Map<String, Pair<String, List<String>>> allSourceSqls = (Map<String, Pair<String, List<String>>>) objinput.readObject();
            java.util.Map<String, String> allWhereCondition = (Map<String, String>) objinput.readObject();
            if (allSourceSqls.containsKey(key)) {
                List<String> finalColumns = new java.util.ArrayList<>();
                String baseSql = allSourceSqls.get(key).getKey();
                List<String> analysisColumns = allSourceSqls.get(key).getValue();
                for (JdbcColumnHandle jdbcColumnHandle : jdbcColumnHandles) {
                    boolean isMatch = false;
                    String jdbcColumnName = jdbcColumnHandle.getColumnName();
                    for (String col : analysisColumns) {
                        if (ArrayUtils.contains(StringUtils.split(col.trim().toUpperCase(), "(), "), jdbcColumnName.toUpperCase())) {
                            finalColumns.add(col);
                            isMatch = true;
                        }
                    }
                    if (!isMatch) {
                        finalColumns.add(jdbcColumnName + " " + jdbcColumnName);
                    }
                }
                List<String> conditions = new ArrayList<>(clauses);
                if(allWhereCondition.containsKey(key)) {
                    conditions.add(allWhereCondition.get(key));
                }
                boolean hasWhere = baseSql.contains("where");
                String wherePushDown = !conditions.isEmpty() ? Joiner.on(" AND ").join(conditions) + (hasWhere ? " AND " : "") : "";
                return baseSql.replace("{tableName}", tableInfo.toString() + (!hasWhere && !conditions.isEmpty() ? " where " : ""))
                        .replace("where", "where "+ wherePushDown)
                        .replace("{columns}", StringUtils.join(finalColumns, ","))
                        .replace("format_datetime", "time_format");
            }
        }
        catch (Exception e) {
            throw new RuntimeException("read external planTree fail", e);
        } finally {
            try {
                if (objinput != null) {
                    objinput.close();
                }
            } catch (Exception ex) {}
        }
        return null;
    }
}
