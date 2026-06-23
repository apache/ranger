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

import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanConflictException;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.OnboardService;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlanReplacement;
import org.apache.ranger.audit.producer.kafka.partition.model.PromotePlugin;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginScale;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConfig;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.Set;

/** REST mutations and reads for the dynamic Kafka partition plan. */
@Component
public class PartitionPlanService {
    public static final String INGESTOR_PROP_PREFIX = "ranger.audit.ingestor";

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

    /** Merges a partial plan delta via REST PATCH with optimistic locking. */
    public PartitionPlan mergePartitionPlan(PartitionPlanReplacement partitionPlanUpdate, String updatedBy) {
        PartitionPlanRequestValidator.validatePatchRequest(partitionPlanUpdate);
        requireDynamicEnabled();
        String auditTopic = resolveAuditTopicName();
        try (PartitionPlanRegistry registry = registryFactory.open(configProps, INGESTOR_PROP_PREFIX)) {
            PartitionPlan currentPlan = requirePlan(registry, auditTopic);
            requireExpectedVersion(currentPlan, partitionPlanUpdate.getExpectedVersion());
            PartitionPlan mergedPlan = partitionPlanUpdate.toMergedPlan(currentPlan, updatedBy);
            if (currentPlan.sameContentAs(mergedPlan)) {
                return returnCurrentPlanNoOp(currentPlan);
            }
            PartitionPlan nextPlan = PartitionPlanAllocator.replacePlan(currentPlan, mergedPlan);
            return publishMutation(registry, auditTopic, partitionPlanUpdate.getExpectedVersion(), currentPlan, nextPlan);
        } catch (PartitionPlanException e) {
            throw e;
        } catch (Exception e) {
            throw new PartitionPlanException("Failed to update partition plan for audit topic '" + auditTopic + "'", e);
        }
    }

    /** Promotes a plugin from the buffer to dedicated partitions. */
    public PartitionPlan promotePlugin(PromotePlugin promotePluginRequest, String updatedBy) {
        PartitionPlanRequestValidator.validatePromotePlugin(promotePluginRequest);
        requireDynamicEnabled();
        String auditTopic = resolveAuditTopicName();
        try (PartitionPlanRegistry registry = registryFactory.open(configProps, INGESTOR_PROP_PREFIX)) {
            PartitionPlan currentPlan = requirePlan(registry, auditTopic);
            requireExpectedVersion(currentPlan, promotePluginRequest.getExpectedVersion());
            if (PartitionPlanAllocator.isPromoteAlreadyApplied(currentPlan, promotePluginRequest.getPluginId(), promotePluginRequest.getPartitionCount(), promotePluginRequest.getRepo(), promotePluginRequest.getAllowedUsers())) {
                return returnCurrentPlanNoOp(currentPlan);
            }
            PartitionPlan nextPlan = PartitionPlanAllocator.promotePlugin(currentPlan, promotePluginRequest.getPluginId(), promotePluginRequest.getPartitionCount(), updatedBy, promotePluginRequest.getRepo(), promotePluginRequest.getAllowedUsers());
            return publishMutation(registry, auditTopic, promotePluginRequest.getExpectedVersion(), currentPlan, nextPlan);
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

    /** Returns configured admin short usernames for partition-plan REST (empty = not restricted beyond authentication). */
    public Set<String> getPartitionPlanAdminUsers() {
        return PartitionPlanKafkaConfig.resolvePartitionPlanAdminUsers(configProps, INGESTOR_PROP_PREFIX);
    }

    /** Appends tail partitions to a plugin already present in the plan. */
    public PartitionPlan scalePlugin(String pluginId, PluginScale scalePlugin, String updatedBy) {
        PartitionPlanRequestValidator.validateScalePlugin(pluginId, scalePlugin);
        requireDynamicEnabled();
        String auditTopic = resolveAuditTopicName();
        try (PartitionPlanRegistry registry = registryFactory.open(configProps, INGESTOR_PROP_PREFIX)) {
            PartitionPlan currentPlan = requirePlan(registry, auditTopic);
            PartitionPlan nextPlan = PartitionPlanAllocator.scalePlugin(currentPlan, pluginId, scalePlugin.getAdditionalPartitions(), updatedBy);
            return publishMutation(registry, auditTopic, scalePlugin.getExpectedVersion(), currentPlan, nextPlan);
        } catch (PartitionPlanException e) {
            throw e;
        } catch (Exception e) {
            throw new PartitionPlanException("Failed to update partition plan for audit topic '" + auditTopic + "'", e);
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
}
