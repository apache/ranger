/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.db.upgrade;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class LiquibasePropertiesFactory {
    private static final Logger LOG = LoggerFactory.getLogger(LiquibasePropertiesFactory.class);

    Map<String, IConfigProvider> configProviders;

    @Autowired
    public LiquibasePropertiesFactory(@Lazy Map<String, IConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    public String getUrl(String serviceName) {
        String url;
        IConfigProvider provider = configProviders.get(serviceName);
        if (provider == null) {
            LOG.error("ConfigProvider for service {} is null", serviceName);
            url = "";
        }
        else {
            url = provider.getUrl();
        }
        return url;
    }

    public String getUsername(String serviceName) {
        String username;
        IConfigProvider provider = configProviders.get(serviceName);
        if (provider == null) {
            LOG.error("ConfigProvider for service {} is null", serviceName);
            username = "";
        }
        else {
            username = provider.getUsername();
        }
        return username;
    }

    public String getPassword(String serviceName) {
        String pwd;
        IConfigProvider provider = configProviders.get(serviceName);
        if (provider == null) {
            LOG.error("ConfigProvider for service {} is null", serviceName);
            pwd = "";
        }
        else {
            pwd = provider.getPassword();
        }
        return pwd;
    }

    public String getDriver(String serviceName) {
        String driver;
        IConfigProvider provider = configProviders.get(serviceName);
        if (provider == null) {
            LOG.error("ConfigProvider for service {} is null", serviceName);
            LOG.error("Setting driver to empty string");
            driver = "";
        }
        else {
            driver = provider.getDriver();
        }
        return driver;
    }

    public String getMasterChangelog(String serviceName) {
        String masterChangeLog;
        IConfigProvider provider = configProviders.get(serviceName);
        if (provider == null) {
            LOG.error("ConfigProvider for service {} is null", serviceName);
            LOG.error("Setting masterChangeLog to empty string");
            masterChangeLog = "";
        }
        else {
            masterChangeLog = provider.getMasterChangelogRelativePath();
        }
        return masterChangeLog;
    }

    public String getFinalizeChangelog(String serviceName) {
        String finalizeChangelog;
        IConfigProvider provider = configProviders.get(serviceName);
        if (provider == null) {
            LOG.error("ConfigProvider for service {} is null", serviceName);
            LOG.error("Setting finalizeChangeLog to empty string");
            finalizeChangelog = "";
        }
        else {
            finalizeChangelog = provider.getFinalizeChangelogRelativePath();
        }
        return finalizeChangelog;
    }
}
