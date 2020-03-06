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

import com.facebook.presto.plugin.jdbc.JdbcConnector;
import com.facebook.presto.plugin.jdbc.JdbcPageSinkProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import io.airlift.bootstrap.LifeCycleManager;

import javax.inject.Inject;
import java.util.Optional;

public class KylinConnector extends JdbcConnector {

    @Inject
    public KylinConnector(
            LifeCycleManager lifeCycleManager,
            KylinMetadataFactory jdbcMetadataFactory,
            JdbcSplitManager jdbcSplitManager,
            KylinRecordSetProvider jdbcRecordSetProvider,
            JdbcPageSinkProvider jdbcPageSinkProvider,
            Optional<ConnectorAccessControl> accessControl) {

        super(lifeCycleManager, jdbcMetadataFactory, jdbcSplitManager, jdbcRecordSetProvider, jdbcPageSinkProvider, accessControl);
    }
}