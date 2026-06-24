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

package org.apache.ranger.audit.producer.kafka.partition;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;
import org.apache.ranger.audit.server.AuditServerConfig;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.ranger.audit.server.AuditServerConstants.PROP_PREFIX_AUDIT_SERVER_SERVICE;
import static org.apache.ranger.audit.server.AuditServerConstants.PROP_SUFFIX_ALLOWED_USERS;

/** Loads service allowlist entries from ingestor site XML for registry bootstrap and brownfield merge. */
public class ServiceAllowlistBootstrap {
    private static final String ALLOWLIST_SOURCE_SITE_XML = "xml-bootstrap";

    private ServiceAllowlistBootstrap() {
    }

    /** Scans {@code ranger.audit.ingestor.service.<repo>.allowed.users} properties. */
    public static Map<String, ServiceAllowlistEntry> loadAllowlistsFromProperties(Properties ingestorProperties) {
        Map<String, ServiceAllowlistEntry> allowlistEntriesByRepo = new LinkedHashMap<>();
        if (ingestorProperties == null) {
            return allowlistEntriesByRepo;
        }
        for (String propertyName : ingestorProperties.stringPropertyNames()) {
            if (!propertyName.startsWith(PROP_PREFIX_AUDIT_SERVER_SERVICE) || !propertyName.endsWith(PROP_SUFFIX_ALLOWED_USERS)) {
                continue;
            }
            String serviceRepoName = propertyName.substring(PROP_PREFIX_AUDIT_SERVER_SERVICE.length(), propertyName.length() - PROP_SUFFIX_ALLOWED_USERS.length());
            if (StringUtils.isBlank(serviceRepoName)) {
                continue;
            }
            String allowedUsersPropertyValue = ingestorProperties.getProperty(propertyName);
            if (StringUtils.isBlank(allowedUsersPropertyValue)) {
                continue;
            }
            List<String> allowedUserShortNames = parseAllowedUserShortNames(allowedUsersPropertyValue);
            if (allowedUserShortNames.isEmpty()) {
                continue;
            }
            allowlistEntriesByRepo.put(
                    serviceRepoName.trim(),
                    new ServiceAllowlistEntry(allowedUserShortNames, ALLOWLIST_SOURCE_SITE_XML, null, null));
        }
        return allowlistEntriesByRepo;
    }

    /** Loads allowlist entries from the running ingestor configuration singleton. */
    public static Map<String, ServiceAllowlistEntry> loadAllowlistsFromServerConfig() {
        return loadAllowlistsFromProperties(AuditServerConfig.getInstance().getProperties());
    }

    /**
     * Brownfield helper: when the Kafka plan has no {@code services} block, merge XML entries in memory only
     * (does not bump version or write to Kafka).
     */
    public static PartitionPlan mergeSiteXmlAllowlistsWhenPlanServicesMissing(
            PartitionPlan partitionPlan, Properties ingestorProperties) {
        PartitionPlan ret = partitionPlan;

        if (partitionPlan != null && (partitionPlan.getServices() == null || partitionPlan.getServices().isEmpty())) {
            Map<String, ServiceAllowlistEntry> siteXmlAllowlistEntries = loadAllowlistsFromProperties(ingestorProperties);

            if (!siteXmlAllowlistEntries.isEmpty()) {
                ret = partitionPlan.toBuilder().services(siteXmlAllowlistEntries).build();
            }
        }

        return ret;
    }

    private static List<String> parseAllowedUserShortNames(String allowedUsersPropertyValue) {
        List<String> allowedUserShortNames = new ArrayList<>();
        for (String userToken : allowedUsersPropertyValue.split(",")) {
            if (userToken != null) {
                String trimmedShortName = userToken.trim();
                if (StringUtils.isNotBlank(trimmedShortName)) {
                    allowedUserShortNames.add(trimmedShortName);
                }
            }
        }
        return allowedUserShortNames;
    }
}
