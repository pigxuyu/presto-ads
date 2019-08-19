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

import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2019/1/4.
 */
public class HBaseClient {

    private static final Logger log = Logger.get(HBaseClient.class);

    private String zkHosts = "";
    private String zkPort = "";
    private Connection connection = null;

    private HBaseClient(String zkHosts, int zkPort) {
        this.zkHosts = zkHosts;
        this.zkPort = String.valueOf(zkPort);
        createConnection();
    }

    private void createConnection() {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, zkHosts);
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);
        conf.setInt("hbase.client.scanner.timeout.period", 180000);
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            log.error(e, "hbase create connection failed");
        }
    }

    public static HBaseClient getHbaseClient(String zkHosts, int zkPort) {
        return new HBaseClient(zkHosts, zkPort);
    }

    public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
        try (Admin admin = connection.getAdmin()) {
            return admin.listNamespaceDescriptors();
        }
    }

    public TableName[] listTableNamesByNamespace(String ns) throws IOException {
        try (Admin admin = connection.getAdmin()) {

            return admin.listTableNamesByNamespace(ns);
        }
    }

    public HTableDescriptor getTableDescriptor(TableName name) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            return admin.getTableDescriptor(name);
        }
    }

    public HTableDescriptor[] listTables(Pattern p) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            return admin.listTables(p);
        }
    }

    public Result get(String tableName, Get get) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            return table.get(get);
        }
    }

    public ResultScanner getScanner(String tableName, Scan scan) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            return table.getScanner(scan);
        }
    }

    public void reConnect() {
        close();
        createConnection();
    }

    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
        }
    }
}
