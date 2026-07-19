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

package org.apache.ranger.audit.producer.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.ranger.audit.producer.kafka.partition.PartitionPlanHolder;
import org.apache.ranger.audit.producer.kafka.partition.PartitionPlanKafkaConfig;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginPartitionAssignment;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Plugin-based Kafka partitioner for Ranger audit events.
 * <p>
 * Configured plugins (hdfs, hiveServer, hiveMetastore, kafka, hbaseRegion, hbaseMaster, solr)
 * each get a fixed number of partitions. Unconfigured plugins use buffer partitions.
 * Within each plugin's partition set, messages are round-robin distributed for load balancing.
 */
public class AuditPartitioner implements Partitioner {
    private static final Logger LOG = LoggerFactory.getLogger(AuditPartitioner.class);

    private String[] configuredPlugins;
    private int      defaultPartitionsPerPlugin;
    private int      totalPartitions;
    private int[]    pluginPartitionCounts;
    private int[]    configuredPluginPartitionStart;
    private int[]    configuredPluginPartitionEnd;
    private int      bufferPartitionStart;
    private int      bufferPartitionCount;
    private boolean  dynamicPlanEnabled;
    private final    ConcurrentHashMap<String, AtomicInteger> appIdCounters = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        String propPrefix = AuditServerConstants.PROP_PREFIX_AUDIT_SERVER;
        String ingestorPropPrefix = propPrefix.substring(0, propPrefix.length() - 1);
        dynamicPlanEnabled = PartitionPlanKafkaConfig.isDynamicPartitionPlanEnabled(configs, ingestorPropPrefix);
        if (dynamicPlanEnabled) {
            LOG.info("Dynamic partition plan enabled — routing from in-memory plan (PartitionPlanHolder)");
            logDynamicPlanConfiguration(configs, propPrefix);
            return;
        }

        String pluginsStr = getConfig(configs, propPrefix + AuditServerConstants.PROP_CONFIGURED_PLUGINS,  AuditServerConstants.DEFAULT_CONFIGURED_PLUGINS);
        configuredPlugins = pluginsStr.split(",");
        for (int i = 0; i < configuredPlugins.length; i++) {
            configuredPlugins[i] = configuredPlugins[i].trim();
        }

        defaultPartitionsPerPlugin = getIntConfig(configs, propPrefix + AuditServerConstants.PROP_TOPIC_PARTITIONS_PER_CONFIGURED_PLUGIN, AuditServerConstants.DEFAULT_PARTITIONS_PER_CONFIGURED_PLUGIN);
        if (defaultPartitionsPerPlugin < 1) {
            defaultPartitionsPerPlugin = 1;
        }

        totalPartitions = getIntConfig(configs, propPrefix + AuditServerConstants.PROP_TOPIC_PARTITIONS, AuditServerConstants.DEFAULT_TOPIC_PARTITIONS);
        if (totalPartitions < 1) {
            totalPartitions = AuditServerConstants.DEFAULT_TOPIC_PARTITIONS;
        }

        // Build per-plugin partition counts - check for individual overrides per plugin
        pluginPartitionCounts = new int[configuredPlugins.length];
        for (int i = 0; i < configuredPlugins.length; i++) {
            String plugin = configuredPlugins[i];
            String overrideKey = propPrefix + AuditServerConstants.PROP_PLUGIN_PARTITION_OVERRIDE_PREFIX + plugin;
            int partitionCount = getIntConfig(configs, overrideKey, defaultPartitionsPerPlugin);
            pluginPartitionCounts[i] = partitionCount;
            if (partitionCount != defaultPartitionsPerPlugin) {
                LOG.info("Plugin '{}' partition override: {} partitions (default: {})", plugin, partitionCount, defaultPartitionsPerPlugin);
            }
        }

        // Calculate partition ranges for each plugin
        configuredPluginPartitionStart = new int[configuredPlugins.length];
        configuredPluginPartitionEnd   = new int[configuredPlugins.length];
        int currentPartition = 0;
        int configuredPartitionCount = 0;

        for (int i = 0; i < configuredPlugins.length; i++) {
            configuredPluginPartitionStart[i] = currentPartition;
            configuredPluginPartitionEnd[i] = currentPartition + pluginPartitionCounts[i] - 1;
            currentPartition += pluginPartitionCounts[i];
            configuredPartitionCount += pluginPartitionCounts[i];
        }

        // Buffer partitions start after configured plugins
        bufferPartitionStart = configuredPartitionCount;
        bufferPartitionCount = Math.max(1, totalPartitions - configuredPartitionCount);

        // Log allocated partition
        AuditServerLogFormatter.LogBuilder logBuilder = AuditServerLogFormatter.builder("****** AuditPartitioner Configuration ******");
        logBuilder.add("Total partitions: ", totalPartitions);
        logBuilder.add("Configured plugins: ", configuredPlugins.length);
        logBuilder.add("Default partitions per plugin: ", defaultPartitionsPerPlugin);
        for (int i = 0; i < configuredPlugins.length; i++) {
            String rangeInfo = String.format("%d partitions (range: %d-%d): ", pluginPartitionCounts[i], configuredPluginPartitionStart[i], configuredPluginPartitionEnd[i]);
            logBuilder.add("Plugin '" + configuredPlugins[i] + "'", rangeInfo);
        }
        logBuilder.add("Buffer partitions (unconfigured): ", String.format("%d partitions (range: %d-%d)", bufferPartitionCount, bufferPartitionStart, totalPartitions - 1));
        logBuilder.logInfo(LOG);
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String appId = key != null ? key.toString() : null;
        if (dynamicPlanEnabled) {
            int numPartitions = resolveTopicPartitionCount(cluster, topic);
            if (appId == null || appId.isEmpty()) {
                return Math.abs(System.identityHashCode(key) % numPartitions);
            }
            return partitionFromPlan(appId, numPartitions);
        }

        int numPartitions = totalPartitions;
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        if (partitions != null && !partitions.isEmpty()) {
            numPartitions = partitions.size();
        }

        if (appId == null || appId.isEmpty()) {
            return Math.abs(System.identityHashCode(key) % numPartitions);
        }

        int pluginIndex = indexOfConfiguredPlugin(appId);
        if (pluginIndex >= 0) {
            int start = configuredPluginPartitionStart[pluginIndex];
            int end = Math.min(configuredPluginPartitionEnd[pluginIndex], numPartitions - 1);
            if (end < start) {
                end = start;
            }
            int rangeSize = end - start + 1;
            int roundRobinIndex = nextRoundRobinIndex(appId, rangeSize);
            return start + roundRobinIndex;
        } else {
            // Unconfigured plugin - use buffer partitions
            int start = Math.min(bufferPartitionStart, numPartitions - 1);
            int count = Math.min(bufferPartitionCount, numPartitions - start);
            if (count <= 0) {
                return start;
            }
            int p = Math.abs(appId.hashCode() % count) + start;
            return Math.min(p, numPartitions - 1);
        }
    }

    @Override
    public void close() {
        appIdCounters.clear();
    }

    /**
     * Routes one audit event to a Kafka partition using the in-memory dynamic partition plan.
     *
     * <p>The plan splits the audit topic into <em>dedicated plugin lanes</em> (configured plugins
     * such as {@code hdfs}, {@code hiveServer}) and a <em>shared buffer pool</em> (everything else).
     * Routing for a given {@code appId} follows this order:
     * <ol>
     *   <li><b>Known plugin with dedicated partitions</b> — round-robin across that plugin's
     *       assignment list so load is spread evenly while preserving per-plugin ordering lanes.
     *       Example: {@code hdfs} → [0, 1, 2] sends three successive events to 0, then 1,
     *       then 2, then wraps.</li>
     *   <li><b>Unknown or unconfigured plugin</b> — sticky hash into the shared buffer pool so
     *       the same {@code appId} always lands on the same buffer partition.
     *       Example: buffer → [10, 11]; {@code myCustomApp} consistently maps to 10 or 11.</li>
     *   <li><b>No buffer partitions in the plan</b> — sticky hash across the full topic when every
     *       partition is assigned to configured plugins.</li>
     * </ol>
     *
     * <p>After topic scale-up, the Kafka producer's cluster metadata can lag behind
     * {@link PartitionPlan#getTopicPartitionCount()}. We use the larger of the two counts as
     * {@code effectiveTopicPartitionCount} so a newly planned tail id (e.g. 12) is not folded
     * into the stale metadata ceiling (e.g. 11).
     *
     * @param appId plugin key from the audit event (Kafka record key)
     * @param kafkaClusterPartitionCount partition count reported by live Kafka cluster metadata
     * @return Kafka partition id to produce to
     */
    private int partitionFromPlan(String appId, int kafkaClusterPartitionCount) {
        PartitionPlan plan = PartitionPlanHolder.getInstance().getPlan();
        if (plan == null) {
            LOG.error("Dynamic partition plan is not loaded; falling back to hash routing for appId '{}'", appId);
            return hashAppIdToPartitionIndex(appId, kafkaClusterPartitionCount);
        }

        int effectiveTopicPartitionCount = Math.max(kafkaClusterPartitionCount, plan.getTopicPartitionCount());

        PluginPartitionAssignment pluginAssignment = findPluginAssignment(plan, appId);
        if (pluginAssignment != null && !pluginAssignment.getPartitions().isEmpty()) {
            List<Integer> dedicatedPluginPartitions = pluginAssignment.getPartitions();
            int dedicatedLaneIndex = nextRoundRobinIndex(appId, dedicatedPluginPartitions.size());
            int plannedPartitionId = dedicatedPluginPartitions.get(dedicatedLaneIndex);
            return resolvePlannedPartitionId(plannedPartitionId, effectiveTopicPartitionCount);
        }

        List<Integer> sharedBufferPartitions = plan.getBuffer().getPartitions();
        if (sharedBufferPartitions.isEmpty()) {
            return hashAppIdToPartitionIndex(appId, effectiveTopicPartitionCount);
        }
        int bufferPoolIndex = hashAppIdToPartitionIndex(appId, sharedBufferPartitions.size());
        int plannedPartitionId = sharedBufferPartitions.get(bufferPoolIndex);
        return resolvePlannedPartitionId(plannedPartitionId, effectiveTopicPartitionCount);
    }

    /**
     * Sticky hash: same {@code appId} always picks the same slot in {@code [0, slotCount)}.
     * Used for buffer-pool routing and for plan-not-loaded fallback.
     */
    private static int hashAppIdToPartitionIndex(String appId, int slotCount) {
        return Math.abs(appId.hashCode() % slotCount);
    }

    /**
     * Returns the next dedicated-lane index for round-robin routing within one plugin's partition set.
     * Each {@code appId} keeps its own counter (0, 1, 2, …); {@code % dedicatedLaneCount} cycles
     * through that plugin's lanes only.
     */
    private int nextRoundRobinIndex(String appId, int dedicatedLaneCount) {
        AtomicInteger messageCounter = appIdCounters.computeIfAbsent(appId, k -> new AtomicInteger(0));
        return messageCounter.getAndIncrement() % dedicatedLaneCount;
    }

    /** Looks up a plugin assignment using case-insensitive plugin id matching. */
    private static PluginPartitionAssignment findPluginAssignment(PartitionPlan plan, String appId) {
        for (Map.Entry<String, PluginPartitionAssignment> entry : plan.getPlugins().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(appId)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /** Returns the live partition count for the audit topic from the Kafka cluster metadata. */
    private static int resolveTopicPartitionCount(Cluster cluster, String topic) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        if (partitions != null && !partitions.isEmpty()) {
            return partitions.size();
        }
        return 1;
    }

    /**
     * Converts a partition id from the plan into the id passed to the Kafka producer.
     *
     * <p>When cluster metadata is stale after scale-up, {@code effectiveTopicPartitionCount} may
     * exceed what the broker metadata reports. Planned tail ids must still be returned as-is —
     * never clamp partition 12 down to 11 just because metadata has not caught up yet.
     *
     * @param plannedPartitionId partition id from the dynamic plan assignment or buffer pool
     * @param effectiveTopicPartitionCount {@code max(kafkaClusterPartitionCount, plan.topicPartitionCount)}
     */
    private static int resolvePlannedPartitionId(int plannedPartitionId, int effectiveTopicPartitionCount) {
        if (effectiveTopicPartitionCount <= 0) {
            return 0;
        }
        if (plannedPartitionId < 0) {
            return 0;
        }
        if (plannedPartitionId < effectiveTopicPartitionCount) {
            return plannedPartitionId;
        }
        return plannedPartitionId;
    }

    /** Logs the dynamic plan snapshot installed in {@link PartitionPlanHolder} at startup. */
    private void logDynamicPlanConfiguration(Map<String, ?> configs, String propPrefix) {
        int defaultPerPlugin = getIntConfig(configs, propPrefix + AuditServerConstants.PROP_TOPIC_PARTITIONS_PER_CONFIGURED_PLUGIN, AuditServerConstants.DEFAULT_PARTITIONS_PER_CONFIGURED_PLUGIN);
        if (defaultPerPlugin < 1) {
            defaultPerPlugin = 1;
        }

        PartitionPlan plan = PartitionPlanHolder.getInstance().getPlan();
        AuditServerLogFormatter.LogBuilder logBuilder = AuditServerLogFormatter.builder("****** AuditPartitioner Configuration ******");
        if (plan == null) {
            logBuilder.add("Mode: ", "dynamic (plan not loaded yet)");
            logBuilder.logInfo(LOG);
            return;
        }

        logBuilder.add("Mode: ", "dynamic (PartitionPlanHolder)");
        logBuilder.add("Plan version: ", plan.getVersion());
        logBuilder.add("Total partitions: ", plan.getTopicPartitionCount());
        logBuilder.add("Configured plugins: ", plan.getPlugins().size());
        logBuilder.add("Default partitions per plugin: ", defaultPerPlugin);
        for (Map.Entry<String, PluginPartitionAssignment> entry : plan.getPlugins().entrySet()) {
            List<Integer> partitionIds = entry.getValue().getPartitions();
            String rangeInfo = formatPartitionRangeInfo(partitionIds);
            logBuilder.add("Plugin '" + entry.getKey() + "'", rangeInfo);
        }
        logBuilder.add("Buffer partitions (unconfigured): ", formatBufferPartitionInfo(plan.getBuffer().getPartitions()));
        logBuilder.logInfo(LOG);
    }

    private static String formatPartitionRangeInfo(List<Integer> partitionIds) {
        if (partitionIds.isEmpty()) {
            return "no partitions assigned in plan";
        }
        return String.format("%d partitions (range: %d-%d)",
                partitionIds.size(), partitionIds.get(0), partitionIds.get(partitionIds.size() - 1));
    }

    private static String formatBufferPartitionInfo(List<Integer> bufferPartitionIds) {
        if (bufferPartitionIds.isEmpty()) {
            return "none (all topic partitions assigned to configured plugins)";
        }
        return String.format("%d partitions (range: %d-%d)",
                bufferPartitionIds.size(), bufferPartitionIds.get(0), bufferPartitionIds.get(bufferPartitionIds.size() - 1));
    }

    private int indexOfConfiguredPlugin(String appId) {
        for (int i = 0; i < configuredPlugins.length; i++) {
            if (configuredPlugins[i].equalsIgnoreCase(appId)) {
                return i;
            }
        }
        return -1;
    }

    private static String getConfig(Map<String, ?> configs, String key, String defaultValue) {
        Object val = configs.get(key);
        return val != null ? val.toString() : defaultValue;
    }

    private static int getIntConfig(Map<String, ?> configs, String key, int defaultValue) {
        Object val = configs.get(key);
        if (val == null) {
            return defaultValue;
        }
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }
        try {
            return Integer.parseInt(val.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
