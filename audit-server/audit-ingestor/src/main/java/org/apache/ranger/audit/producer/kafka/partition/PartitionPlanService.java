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
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanConflictException;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.OnboardService;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlanReplacement;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginScale;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConfig;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Dynamic partition plan reads, REST PATCH merges, and lazy plugin onboarding on audit POST. */
@Component
public class PartitionPlanService {
    public static final String INGESTOR_PROP_PREFIX = "ranger.audit.ingestor";

    private static final Logger LOG = LoggerFactory.getLogger(PartitionPlanService.class);
    private static final Object AUTO_ONBOARD_LOCK = new Object();
    private static final int AUTO_ONBOARD_MAX_ATTEMPTS = 3;

    private final Properties configProps;
    private final PartitionPlanHolder holder;
    private final PartitionPlanRegistryFactory registryFactory;
    private final KafkaAuditTopicPartitionGrower auditTopicPartitionGrower;

    public PartitionPlanService() {
        this(AuditServerConfig.getInstance().getProperties(), PartitionPlanHolder.getInstance(), new PartitionPlanRegistryFactory(), new KafkaAuditTopicPartitionGrower());
    }

    PartitionPlanService(Properties configProps, PartitionPlanHolder holder, PartitionPlanRegistryFactory registryFactory, KafkaAuditTopicPartitionGrower auditTopicPartitionGrower) {
        this.configProps               = configProps;
        this.holder                    = holder;
        this.registryFactory           = registryFactory;
        this.auditTopicPartitionGrower = auditTopicPartitionGrower;
    }

    /** Returns whether dynamic partition-plan mode is enabled in ingestor configuration. */
    public boolean isDynamicPartitionPlanEnabled() {
        return PartitionPlanKafkaConfig.isDynamicPartitionPlanEnabled(configProps, INGESTOR_PROP_PREFIX);
    }

    /** Returns the plan currently installed in memory on this ingestor pod. */
    public PartitionPlan getPartitionPlan() {
        PartitionPlan plan = holder.getPlan();
        if (plan == null) {
            throw new PartitionPlanException("Partition plan is not loaded in memory");
        }
        return plan;
    }

    /**
     * After Kerberos and service allowlist checks pass on {@code POST /access}, promote unknown plugins
     * from the buffer and upsert the repo allowlist without a separate onboard REST call.
     */
    public void ensurePluginOnboarded(String serviceName, String pluginId, String authenticatedUser) {
        if (!isDynamicPartitionPlanEnabled() || StringUtils.isAnyBlank(serviceName, pluginId, authenticatedUser)) {
            return;
        }
        PartitionPlan plan = holder.getPlan();
        if (plan != null && plan.getPlugins().containsKey(pluginId)) {
            return;
        }
        synchronized (AUTO_ONBOARD_LOCK) {
            plan = holder.getPlan();
            if (plan != null && plan.getPlugins().containsKey(pluginId)) {
                return;
            }
            int partitionCount = resolveDefaultPartitionCountForPlugin(pluginId);
            String updatedBy = "auto-onboard:" + authenticatedUser;
            OnboardService onboardRequest = new OnboardService(
                    serviceName,
                    pluginId,
                    partitionCount,
                    List.of(authenticatedUser),
                    plan != null ? plan.getVersion() : PartitionPlanConstants.INITIAL_PLAN_VERSION);
            for (int attempt = 1; attempt <= AUTO_ONBOARD_MAX_ATTEMPTS; attempt++) {
                try {
                    onboardService(onboardRequest, updatedBy);
                    LOG.info("Auto-onboarded plugin '{}' for service '{}' with {} partition(s)", pluginId, serviceName, partitionCount);
                    return;
                } catch (PartitionPlanConflictException conflict) {
                    PartitionPlan currentPlan = conflict.getCurrentPlan();
                    if (currentPlan != null && currentPlan.getPlugins().containsKey(pluginId)) {
                        holder.install(currentPlan, currentPlan.getTopicPartitionCount());
                        return;
                    }
                    plan = holder.getPlan();
                    if (plan == null) {
                        throw conflict;
                    }
                    onboardRequest = new OnboardService(
                            serviceName,
                            pluginId,
                            partitionCount,
                            List.of(authenticatedUser),
                            plan.getVersion());
                    if (attempt == AUTO_ONBOARD_MAX_ATTEMPTS) {
                        LOG.warn("Auto-onboard conflict for plugin '{}' on service '{}' after {} attempts; continuing with buffer routing",
                                pluginId, serviceName, AUTO_ONBOARD_MAX_ATTEMPTS, conflict);
                        return;
                    }
                }
            }
        }
    }

    /** Merges a partial plan delta via REST PATCH with optimistic locking. */
    public PartitionPlan mergePartitionPlan(PartitionPlanReplacement partitionPlanUpdate, String updatedBy) {
        PartitionPlanRequestValidator.validatePatchRequest(partitionPlanUpdate);
        requireDynamicEnabled();
        String auditTopic = resolveAuditTopicName();
        try (PartitionPlanRegistry registry = registryFactory.open(configProps, INGESTOR_PROP_PREFIX)) {
            PartitionPlan currentPlan = requirePlan(registry, auditTopic);
            requireExpectedVersion(currentPlan, partitionPlanUpdate.getExpectedVersion());
            PartitionPlan nextPlan = currentPlan;
            if (partitionPlanUpdate.hasMergeDelta()) {
                PartitionPlan mergedPlan = partitionPlanUpdate.toMergedPlan(currentPlan, updatedBy);
                if (!currentPlan.sameContentAs(mergedPlan)) {
                    nextPlan = PartitionPlanAllocator.replacePlan(currentPlan, mergedPlan);
                }
            }
            if (partitionPlanUpdate.hasPluginScalesDelta()) {
                for (Map.Entry<String, Integer> scaleEntry : partitionPlanUpdate.getPluginScales().entrySet()) {
                    PartitionPlanRequestValidator.validateScalePlugin(scaleEntry.getKey(),
                            new PluginScale(scaleEntry.getValue(), partitionPlanUpdate.getExpectedVersion()));
                    nextPlan = PartitionPlanAllocator.scalePlugin(nextPlan, scaleEntry.getKey(), scaleEntry.getValue(), updatedBy);
                }
            }
            if (currentPlan.sameContentAs(nextPlan)) {
                return returnCurrentPlanNoOp(currentPlan);
            }
            return publishMutation(registry, auditTopic, partitionPlanUpdate.getExpectedVersion(), currentPlan, nextPlan);
        } catch (PartitionPlanException e) {
            throw e;
        } catch (Exception e) {
            throw new PartitionPlanException("Failed to update partition plan for audit topic '" + auditTopic + "'", e);
        }
    }

    /** Onboards a service repo: upsert allowlist and promote plugin partitions in one plan version. */
    public PartitionPlan onboardService(OnboardService onboardServiceRequest, String updatedBy) {
        PartitionPlanRequestValidator.validateOnboardService(onboardServiceRequest);
        requireDynamicEnabled();
        String auditTopic = resolveAuditTopicName();
        try (PartitionPlanRegistry registry = registryFactory.open(configProps, INGESTOR_PROP_PREFIX)) {
            PartitionPlan currentPlan = requirePlan(registry, auditTopic);
            requireExpectedVersion(currentPlan, onboardServiceRequest.getExpectedVersion());
            if (PartitionPlanAllocator.isOnboardAlreadyApplied(currentPlan, onboardServiceRequest.getServiceName(), onboardServiceRequest.getPluginId(), onboardServiceRequest.getPartitionCount(), onboardServiceRequest.getAllowedUsers())) {
                return returnCurrentPlanNoOp(currentPlan);
            }
            PartitionPlan nextPlan = PartitionPlanAllocator.onboardRepo(currentPlan, onboardServiceRequest.getServiceName(), onboardServiceRequest.getPluginId(), onboardServiceRequest.getPartitionCount(), onboardServiceRequest.getAllowedUsers(), updatedBy);
            return publishMutation(registry, auditTopic, onboardServiceRequest.getExpectedVersion(), currentPlan, nextPlan);
        } catch (PartitionPlanException e) {
            throw e;
        } catch (Exception e) {
            throw new PartitionPlanException("Failed to onboard repo in partition plan for audit topic '" + auditTopic + "'", e);
        }
    }

    /** Validates version, grows the audit topic if needed, writes the plan, and reloads memory. */
    private PartitionPlan publishMutation(PartitionPlanRegistry registry, String auditTopic, int expectedVersion, PartitionPlan current, PartitionPlan next) {
        requireExpectedVersion(current, expectedVersion);
        growAuditTopicIfNeeded(next.getTopicPartitionCount());
        verifyVersionUnchanged(registry, expectedVersion);
        registry.writePlan(auditTopic, next);
        verifyReadback(registry, auditTopic, next.getVersion());
        return holder.getPlan();
    }

    /** Returns the current plan without a registry write when the desired state is already satisfied. */
    private PartitionPlan returnCurrentPlanNoOp(PartitionPlan current) {
        holder.install(current, current.getTopicPartitionCount());
        return holder.getPlan();
    }

    private static void requireExpectedVersion(PartitionPlan current, int expectedVersion) {
        if (current.getVersion() != expectedVersion) {
            throw new PartitionPlanConflictException(current);
        }
    }

    /** Grows the audit topic before the plan references new partition IDs. */
    private void growAuditTopicIfNeeded(int requiredPartitions) {
        try {
            auditTopicPartitionGrower.growAuditTopicToRequiredPartitionCount(configProps, INGESTOR_PROP_PREFIX, resolveAuditTopicName(), requiredPartitions);
        } catch (RuntimeException e) {
            throw new PartitionPlanException("Failed to grow audit topic partition count", e);
        }
    }

    /** Confirms the registry still holds the expected version before writing. */
    private void verifyVersionUnchanged(PartitionPlanRegistry registry, int expectedVersion) {
        PartitionPlan latest = registry.readPlan(resolveAuditTopicName());
        if (latest == null) {
            throw new PartitionPlanException("Partition plan disappeared during update");
        }
        if (latest.getVersion() != expectedVersion) {
            throw new PartitionPlanConflictException(latest);
        }
    }

    /** Mandatory read-back after publish so every pod converges on the same plan version. */
    private void verifyReadback(PartitionPlanRegistry registry, String auditTopic, int expectedVersion) {
        PartitionPlan readback = registry.readPlan(auditTopic);
        if (readback == null || readback.getVersion() != expectedVersion) {
            throw new PartitionPlanConflictException(readback != null ? readback : holder.getPlan());
        }
        holder.install(readback, readback.getTopicPartitionCount());
    }

    /** Loads the current plan from Kafka or fails when the registry is empty. */
    private static PartitionPlan requirePlan(PartitionPlanRegistry registry, String auditTopic) {
        PartitionPlan plan = registry.readPlan(auditTopic);
        if (plan == null) {
            throw new PartitionPlanException("No partition plan found in Kafka for audit topic '" + auditTopic + "'");
        }
        return plan;
    }

    /** Rejects REST calls when dynamic mode is disabled. */
    private void requireDynamicEnabled() {
        if (!isDynamicPartitionPlanEnabled()) {
            throw new PartitionPlanException("Dynamic partition plan is not enabled");
        }
    }

    /** Resolves the audit data topic name from ingestor configuration. */
    private String resolveAuditTopicName() {
        return MiscUtil.getStringProperty(configProps, INGESTOR_PROP_PREFIX + "." + AuditServerConstants.PROP_TOPIC_NAME, AuditServerConstants.DEFAULT_TOPIC);
    }

    private int resolveDefaultPartitionCountForPlugin(String pluginId) {
        Map<String, Object> configMap = new HashMap<>();
        for (String key : configProps.stringPropertyNames()) {
            configMap.put(key, configProps.getProperty(key));
        }
        PartitionPlanBootstrapConfig bootstrapConfig = PartitionPlanBootstrapConfig.fromProducerConfigMap(configMap, resolveAuditTopicName());
        return bootstrapConfig.getPartitionsForPlugin(pluginId);
    }
}
