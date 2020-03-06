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

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcRecordSet;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;

import java.util.List;

public class KylinRecordSet extends JdbcRecordSet {

    private final JdbcClient jdbcClient;
    private final List<JdbcColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final JdbcSplit split;
    private final ConnectorSession session;

    public KylinRecordSet(JdbcClient jdbcClient, ConnectorSession session, JdbcSplit split, List<JdbcColumnHandle> columnHandles) {
        super(jdbcClient, session, split, columnHandles);
        this.jdbcClient = jdbcClient;
        this.split = split;
        this.columnHandles = columnHandles;
        this.columnTypes = getColumnTypes();
        this.session = session;
    }

    @Override
    public RecordCursor cursor() {
        return new KylinRecordCursor(jdbcClient, session, split, columnHandles);
    }
}
