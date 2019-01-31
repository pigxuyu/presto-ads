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

import io.airlift.configuration.Config;

public class KylinConfig {

    private boolean metaNameUpperCase = true;

    private String optimizeStorageConnectionUrl;

    private String optimizeStorageConnectionUser;

    private String optimizeStorageConnectionPassword;

    public boolean isMetaNameUpperCase() {
        return metaNameUpperCase;
    }

    public String getOptimizeStorageConnectionUrl() {
        return optimizeStorageConnectionUrl;
    }

    public String getOptimizeStorageConnectionUser() {
        return optimizeStorageConnectionUser;
    }

    public String getOptimizeStorageConnectionPassword() {
        return optimizeStorageConnectionPassword;
    }

    @Config("kylin.metaname-uppercase")
    public KylinConfig setMetaNameUpperCase(boolean metaNameUpperCase) {
        this.metaNameUpperCase = metaNameUpperCase;
        return this;
    }

    @Config("kylin.optimize.storage.connection-url")
    public KylinConfig setOptimizeStorageConnectionUrl(String optimizeStorageConnectionUrl) {
        this.optimizeStorageConnectionUrl = optimizeStorageConnectionUrl;
        return this;
    }

    @Config("kylin.optimize.storage.connection-user")
    public KylinConfig setOptimizeStorageConnectionUser(String optimizeStorageConnectionUser) {
        this.optimizeStorageConnectionUser = optimizeStorageConnectionUser;
        return this;
    }

    @Config("kylin.optimize.storage.connection-password")
    public KylinConfig setOptimizeStorageConnectionPassword(String optimizeStorageConnectionPassword) {
        this.optimizeStorageConnectionPassword = optimizeStorageConnectionPassword;
        return this;
    }
}
