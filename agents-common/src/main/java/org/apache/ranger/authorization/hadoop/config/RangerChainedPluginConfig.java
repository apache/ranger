/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.authorization.hadoop.config;

public class RangerChainedPluginConfig extends RangerPluginConfig {
    public RangerChainedPluginConfig(String serviceType, String serviceName, String appId, RangerPluginConfig sourcePluginConfig) {
        super(serviceType,  serviceName, appId, sourcePluginConfig.getClusterName(), sourcePluginConfig.getClusterType(), null);

        // add necessary config "overrides", so that RangerAdminClient implementations (like RangerAdminRESTClient)
        // will use configurations from ranger-<source-service-type>-security.xml (sourcePluginConfig) to connect to Ranger Admin

        set(getPropertyPrefix() + ".service.name", serviceName);
        copyProperty(sourcePluginConfig, ".policy.source.impl");
        copyProperty(sourcePluginConfig, ".policy.cache.dir");
        copyProperty(sourcePluginConfig, ".policy.rest.url");
        copyProperty(sourcePluginConfig, ".policy.rest.ssl.config.file");
        copyProperty(sourcePluginConfig, ".policy.pollIntervalMs", 30 * 1000);
        copyProperty(sourcePluginConfig, ".policy.rest.client.connection.timeoutMs", 120 * 1000);
        copyProperty(sourcePluginConfig, ".policy.rest.read.timeoutMs", 30 * 1000);
        copyProperty(sourcePluginConfig, ".policy.rest.supports.policy.deltas");
        copyProperty(sourcePluginConfig, ".tag.rest.supports.tag.deltas");
    }

    protected void copyProperty(RangerPluginConfig sourcePluginConfig, String propertySuffix) {
        String value = sourcePluginConfig.get("ranger.plugin." + sourcePluginConfig.getServiceType() + propertySuffix);
        if (value != null) {
            set(getPropertyPrefix() + propertySuffix, sourcePluginConfig.get("ranger.plugin." + sourcePluginConfig.getServiceType() + propertySuffix));
        }
    }

    protected void copyProperty(RangerPluginConfig sourcePluginConfig, String propertySuffix, int defaultValue) {
        setInt(getPropertyPrefix() + propertySuffix, sourcePluginConfig.getInt("ranger.plugin" + sourcePluginConfig.getServiceType() + propertySuffix, defaultValue));
    }
}
