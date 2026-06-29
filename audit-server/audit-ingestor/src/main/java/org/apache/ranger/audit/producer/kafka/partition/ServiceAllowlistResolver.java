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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.producer.kafka.partition;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;

/**
 * Per-repo audit POST authorization after {@code auth_to_local} mapping.
 *
 * <p>Distinct from the global allowlist union in {@link AuthToLocalRuleComposer#collectAllowedUserShortNames}
 * (which selects Kerberos mapping rules). Checks whether the mapped short name is allowed for the
 * specific {@code serviceName} on {@code POST /api/audit/access?serviceName=...}.
 *
 * <p>Example (dynamic mode, plan has dev_hdfs with hdfs and nn only):
 * <ul>
 *   <li>isAllowed(dev_hdfs, hdfs) returns true (repo in plan)</li>
 *   <li>isAllowed(dev_hive, hive) returns false unless static XML lists hive for dev_hive
 *       (repo not in plan -> registry returns null -> XML fallback)</li>
 * </ul>
 */
public class ServiceAllowlistResolver {
    private ServiceAllowlistResolver() {
    }

    /**
     * @param serviceName Ranger repo from {@code serviceName} query param (e.g. {@code dev_hdfs})
     * @param userName short name after {@code KerberosName.getShortName()} (e.g. {@code hdfs} from {@code nn/...})
     * @param holder partition-plan registry; per-repo {@code services[serviceName].allowedUsers}
     */
    public static boolean isAllowedServiceUser(String serviceName, String userName, boolean dynamicPartitionPlanEnabled, PartitionPlanHolder holder, Map<String, Set<String>> staticAllowedUsersByService) {
        boolean ret = false;

        if (!StringUtils.isBlank(serviceName) && !StringUtils.isBlank(userName)) {
            if (dynamicPartitionPlanEnabled && holder != null) {
                Set<String> registryUsers = holder.getAllowedUsersForService(serviceName);

                if (registryUsers != null) {
                    ret = registryUsers.contains(userName);
                } else {
                    Set<String> allowedUsers = staticAllowedUsersByService != null ? staticAllowedUsersByService.get(serviceName) : null;
                    ret = allowedUsers != null && allowedUsers.contains(userName);
                }
            } else {
                Set<String> allowedUsers = staticAllowedUsersByService != null ? staticAllowedUsersByService.get(serviceName) : null;
                ret = allowedUsers != null && allowedUsers.contains(userName);
            }
        }

        return ret;
    }
}
