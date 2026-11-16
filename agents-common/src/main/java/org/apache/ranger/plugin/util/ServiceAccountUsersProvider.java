/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.plugin.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.admin.client.RangerAdminRESTClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class ServiceAccountUsersProvider {
    private static final String CONFIG_PREFIX             = "ranger.serviceaccount.refresher";
    private static final String POLLING_INTERVAL_PROPERTY = CONFIG_PREFIX + ".pollIntervalMs";

    private final ServiceAccountUsersRefresher refresher;

    private volatile Map<String, Set<String>> serviceAccountUsers = Collections.emptyMap();

    public ServiceAccountUsersProvider(boolean serviceAccountRefreshEnabled, Configuration config) {
        refresher = serviceAccountRefreshEnabled ? createRefresher(requireNonNull(config, "config")) : null;
    }

    public ServiceAccountUsersProvider(boolean serviceAccountRefreshEnabled, Properties properties) {
        this(serviceAccountRefreshEnabled, serviceAccountRefreshEnabled ? toConfiguration(properties) : null);
    }

    public void start() {
        if (refresher != null) {
            refresher.startRefresher();
        }
    }

    public void stop() {
        if (refresher != null) {
            refresher.stopRefresher();
        }
    }

    public boolean isServiceAccountUser(String serviceName, String userName) {
        Set<String> users = serviceAccountUsers.get(serviceName);

        return users != null && users.contains(userName);
    }

    private ServiceAccountUsersRefresher createRefresher(Configuration config) {
        RangerAdminRESTClient rangerAdmin = new RangerAdminRESTClient();

        rangerAdmin.init("service-account-users", "service-account-users-provider", CONFIG_PREFIX, config);

        return new ServiceAccountUsersRefresher(rangerAdmin::getServiceAccountUsers, this::setServiceAccountUsers,
                config.getLong(POLLING_INTERVAL_PROPERTY, -1L));
    }

    private static Configuration toConfiguration(Properties properties) {
        Configuration ret = new Configuration(false);

        Properties source = requireNonNull(properties, "properties");

        for (String name : source.stringPropertyNames()) {
            ret.set(name, source.getProperty(name));
        }

        return ret;
    }

    private void setServiceAccountUsers(Map<String, Set<String>> serviceAccountUsers) {
        Map<String, Set<String>> snapshot = new HashMap<>();

        serviceAccountUsers.forEach((serviceName, users) -> {
            if (serviceName != null && users != null && !users.isEmpty()) {
                snapshot.put(serviceName, Collections.unmodifiableSet(new HashSet<>(users)));
            }
        });

        this.serviceAccountUsers = Collections.unmodifiableMap(snapshot);
    }
}
