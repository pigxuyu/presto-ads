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

import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Joiner;
import io.airlift.log.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Manages connections to the Hbase nodes
 */
public class HbaseClientManager
{
    private static final Logger log = Logger.get(HbaseClientManager.class);

    private final HBaseClient hbaseClient;

    private final HbaseConnectorConfig hbaseConnectorConfig;

    @Inject
    HbaseClientManager(
            HbaseConnectorConfig hbaseConnectorConfig,
            NodeManager nodeManager)
    {
        this.hbaseConnectorConfig = requireNonNull(hbaseConnectorConfig, "hbaseConfig is null");
        this.hbaseClient = createConsumer(hbaseConnectorConfig.getHbaseZkNodes(), hbaseConnectorConfig.getDefaultHbaseZkPort());
    }

    @PreDestroy
    public void tearDown()
    {
        if (hbaseClient != null) {
            try {
                hbaseClient.close();
            }
            catch (Exception e) {
                log.warn(e, "While destroying HbasePool");
            }
        }
    }

    public HbaseConnectorConfig getHbaseConnectorConfig()
    {
        return hbaseConnectorConfig;
    }

    public HBaseClient getHBaseClient()
    {
        return hbaseClient;
    }

    private HBaseClient createConsumer(Set<String> hosts, int defaultHbaseZkPort)
    {
        String url = Joiner.on(",").skipNulls().join(hosts);
        log.info("Creating new HbaseClient for url %s and port %s", url, defaultHbaseZkPort);
        return HBaseClient.getHbaseClient(url, defaultHbaseZkPort);
    }
}
