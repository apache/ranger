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
import org.apache.ranger.audit.producer.kafka.partition.model.OnboardService;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlanReplacement;
import org.apache.ranger.audit.producer.kafka.partition.model.PromotePlugin;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginScale;

import java.util.List;

/** Validates partition-plan REST mutation request bodies before registry writes. */
public class PartitionPlanRequestValidator {
    private PartitionPlanRequestValidator() {
    }

    public static void validatePatchRequest(PartitionPlanReplacement partitionPlanUpdate) {
        if (partitionPlanUpdate == null) {
            throw new PartitionPlanException("Partition plan patch request is required");
        }
        validateExpectedVersion(partitionPlanUpdate.getExpectedVersion());
        if (!partitionPlanUpdate.hasMergeDelta()) {
            throw new PartitionPlanException(
                    "At least one of topicPartitionCount, plugins, buffer, or services must be provided");
        }
    }

    public static void validatePromotePlugin(PromotePlugin promotePluginRequest) {
        if (promotePluginRequest == null) {
            throw new PartitionPlanException("Promote plugin request is required");
        }
        validateNonBlankPluginId(promotePluginRequest.getPluginId());
        validatePositiveCount(promotePluginRequest.getPartitionCount(), "partitionCount");
        validateExpectedVersion(promotePluginRequest.getExpectedVersion());
        if (StringUtils.isNotBlank(promotePluginRequest.getRepo())
                && (promotePluginRequest.getAllowedUsers() == null
                || promotePluginRequest.getAllowedUsers().isEmpty())) {
            throw new PartitionPlanException("allowedUsers are required when repo is specified");
        }
    }

    public static void validateScalePlugin(String pluginId, PluginScale scalePlugin) {
        if (scalePlugin == null) {
            throw new PartitionPlanException("Plugin scale request is required");
        }
        validateNonBlankPluginId(pluginId);
        validatePositiveCount(scalePlugin.getAdditionalPartitions(), "additionalPartitions");
        validateExpectedVersion(scalePlugin.getExpectedVersion());
    }

    public static void validateOnboardService(OnboardService onboardServiceRequest) {
        if (onboardServiceRequest == null) {
            throw new PartitionPlanException("Onboard service request is required");
        }
        validateNonBlankServiceName(onboardServiceRequest.getServiceName());
        validateNonBlankPluginId(onboardServiceRequest.getPluginId());
        validatePositiveCount(onboardServiceRequest.getPartitionCount(), "partitionCount");
        validateNonEmptyAllowedUsers(onboardServiceRequest.getAllowedUsers());
        validateExpectedVersion(onboardServiceRequest.getExpectedVersion());
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
