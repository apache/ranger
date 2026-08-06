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

package org.apache.ranger.audit.partition;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.model.RangerService;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Parses {@code policy.download.auth.users} service config for audit ingestor authorization. */
public final class PolicyDownloadAuthUsersUtil {
    public static final String CONFIG_NAME = "policy.download.auth.users";

    private PolicyDownloadAuthUsersUtil() {
    }

    public static List<String> parseUsers(RangerService service) {
        if (service == null || service.getConfigs() == null) {
            return Collections.emptyList();
        }
        return parseUsers(service.getConfigs().get(CONFIG_NAME));
    }

    public static List<String> parseUsers(String configValue) {
        if (StringUtils.isBlank(configValue)) {
            return Collections.emptyList();
        }
        return Arrays.stream(configValue.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .filter(user -> !"*".equals(user))
                .collect(Collectors.toList());
    }

    public static Map<String, List<String>> normalizeServiceAllowedUsers(Map<String, List<String>> serviceAllowedUsers) {
        if (serviceAllowedUsers == null || serviceAllowedUsers.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, List<String>> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : serviceAllowedUsers.entrySet()) {
            if (StringUtils.isBlank(entry.getKey())) {
                continue;
            }
            List<String> users = entry.getValue() == null ? Collections.emptyList() : parseUsers(String.join(",", entry.getValue()));
            if (users.isEmpty()) {
                continue;
            }
            normalized.put(entry.getKey().trim(), List.copyOf(users));
        }
        return Collections.unmodifiableMap(normalized);
    }
}
