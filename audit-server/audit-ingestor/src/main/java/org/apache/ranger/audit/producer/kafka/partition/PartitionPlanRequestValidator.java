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
import org.apache.ranger.audit.producer.kafka.partition.constants.PartitionPlanConstants;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.OnboardPlugin;
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;
import org.apache.ranger.audit.producer.kafka.partition.model.UpdatePlugin;

import java.util.List;
import java.util.Map;

/** Validates partition-plan REST mutation request bodies before registry writes. */
public class PartitionPlanRequestValidator {
    private PartitionPlanRequestValidator() {
    }

    public static void validateOnboardPlugin(OnboardPlugin onboardPluginRequest) {
        if (onboardPluginRequest == null) {
            throw new PartitionPlanException("Onboard plugin request is required");
        }
        validateNonBlankPluginId(onboardPluginRequest.getPluginId());
        validatePositiveCount(onboardPluginRequest.getPartitionCount(), "partitionCount");
        validateExpectedVersion(onboardPluginRequest.getExpectedVersion());
        validateRequiredServiceEntries(onboardPluginRequest.getServices());
    }

    public static void validateUpdatePlugin(String pluginId, UpdatePlugin updatePluginRequest) {
        if (updatePluginRequest == null) {
            throw new PartitionPlanException("Update plugin request is required");
        }
        validateNonBlankPluginId(pluginId);
        validateExpectedVersion(updatePluginRequest.getExpectedVersion());
        if (!updatePluginRequest.hasMutationDelta()) {
            throw new PartitionPlanException("At least one of additionalPartitions, addServices, updateServices, or removeServices must be provided");
        }
        Integer additionalPartitions = updatePluginRequest.getAdditionalPartitions();
        if (additionalPartitions != null && additionalPartitions < 1) {
            throw new PartitionPlanException("additionalPartitions must be >= 1");
        }
        validateOptionalServiceEntries(updatePluginRequest.getAddServices());
        validateOptionalServiceEntries(updatePluginRequest.getUpdateServices());
        validateRemoveServiceNames(updatePluginRequest.getRemoveServices());
    }

    private static void validateRequiredServiceEntries(Map<String, ServiceAllowlistEntry> services) {
        if (services == null || services.isEmpty()) {
            throw new PartitionPlanException("services are required");
        }
        validateServiceEntries(services);
    }

    private static void validateOptionalServiceEntries(Map<String, ServiceAllowlistEntry> services) {
        if (services == null || services.isEmpty()) {
            return;
        }
        validateServiceEntries(services);
    }

    private static void validateServiceEntries(Map<String, ServiceAllowlistEntry> services) {
        for (Map.Entry<String, ServiceAllowlistEntry> entry : services.entrySet()) {
            validateNonBlankServiceName(entry.getKey());
            if (entry.getValue() == null) {
                throw new PartitionPlanException("Service allowlist entry is required for '" + entry.getKey() + "'");
            }
            validateNonEmptyAllowedUsers(entry.getValue().getAllowedUsers());
        }
    }

    private static void validateRemoveServiceNames(List<String> removeServices) {
        if (removeServices == null || removeServices.isEmpty()) {
            return;
        }
        for (String serviceName : removeServices) {
            validateNonBlankServiceName(serviceName);
        }
    }

    private static void validateExpectedVersion(int expectedVersion) {
        if (expectedVersion < PartitionPlanConstants.INITIAL_PLAN_VERSION) {
            throw new PartitionPlanException("expectedVersion must be >= " + PartitionPlanConstants.INITIAL_PLAN_VERSION);
        }
    }

    private static void validateNonBlankPluginId(String pluginId) {
        if (StringUtils.isBlank(pluginId)) {
            throw new PartitionPlanException("pluginId is required");
        }
    }

    private static void validateNonBlankServiceName(String serviceName) {
        if (StringUtils.isBlank(serviceName)) {
            throw new PartitionPlanException("serviceName is required");
        }
    }

    private static void validatePositiveCount(int count, String fieldName) {
        if (count < 1) {
            throw new PartitionPlanException(fieldName + " must be >= 1");
        }
    }

    private static void validateNonEmptyAllowedUsers(List<String> allowedUsers) {
        if (allowedUsers == null || allowedUsers.isEmpty()) {
            throw new PartitionPlanException("allowedUsers are required");
        }
        boolean hasNonBlankUser = false;
        for (String allowedUserShortName : allowedUsers) {
            if (StringUtils.isNotBlank(allowedUserShortName)) {
                hasNonBlankUser = true;
                break;
            }
        }
        if (!hasNonBlankUser) {
            throw new PartitionPlanException("allowedUsers must contain at least one non-blank username");
        }
    }
}
