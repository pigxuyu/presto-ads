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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Set;

import static com.google.common.collect.Iterables.filter;

public class HbaseConnectorConfig
{
    /**
     * Seed nodes for Hbase zookeeper cluster. At least one must exist.
     */
    private Set<String> hbaseZkNodes = ImmutableSet.of();

    /**
     * The hbase zookeeper port to use in the connector.
     */
    private int defaultHbaseZkPort = 2181;

    /**
     * The schema name list to use in the connector.
     */
    private Set<String> defaultSchemaList = ImmutableSet.of();

    /**
     * Count parameter for Hbase scan command.
     */
    private int hbaseScanCount = 1000;

    /**
     * Timeout to connect to Hbase.
     */
    private Duration hbaseConnectTimeout = Duration.valueOf("2000ms");

    @Config("hbase.zk.port")
    public HbaseConnectorConfig setDefaultHbaseZkPort(int port)
    {
        this.defaultHbaseZkPort = port;
        return this;
    }

    @NotNull
    public int getDefaultHbaseZkPort()
    {
        return defaultHbaseZkPort;
    }

    @Size(min = 1)
    public Set<String> getHbaseZkNodes()
    {
        return hbaseZkNodes;
    }

    @Config("hbase.zk.nodes")
    public HbaseConnectorConfig setHbaseZkNodes(String nodes)
    {
        this.hbaseZkNodes = (nodes == null) ? null : parseNodes(nodes);
        return this;
    }

    @Size(min = 1)
    public Set<String> getDefaultSchemas()
    {
        return defaultSchemaList;
    }

    @Config("hbase.default-schema")
    public HbaseConnectorConfig setDefaultSchemas(String defaultSchemas)
    {
        if (defaultSchemas == null) {
            this.defaultSchemaList = null;
        } else {
            Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
            this.defaultSchemaList = ImmutableSet.copyOf(filter(splitter.split(defaultSchemas), StringUtils::isNoneEmpty));
        }
        return this;
    }

    public int getHbaseScanCount()
    {
        return hbaseScanCount;
    }

    @Config("hbase.scan-count")
    public HbaseConnectorConfig setHbaseScanCount(int hbaseScanCount)
    {
        this.hbaseScanCount = hbaseScanCount;
        return this;
    }

    @MinDuration("1s")
    public Duration getHbaseConnectTimeout()
    {
        return hbaseConnectTimeout;
    }

    @Config("hbase.connect-timeout")
    public HbaseConnectorConfig setHbaseConnectTimeout(String hbaseConnectTimeout)
    {
        this.hbaseConnectTimeout = Duration.valueOf(hbaseConnectTimeout);
        return this;
    }

    public static ImmutableSet<String> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return ImmutableSet.copyOf(filter(splitter.split(nodes), StringUtils::isNoneEmpty));
    }
}
