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

package org.apache.ranger.audit.destination.kafka;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.ranger.audit.server.AuditServerConstants.DEFAULT_CONFIGURED_APP_IDS;
import static org.apache.ranger.audit.server.AuditServerConstants.DEFAULT_PARTITIONS_PER_APP_ID;
import static org.apache.ranger.audit.server.AuditServerConstants.PROP_CONFIGURED_APP_IDS;
import static org.apache.ranger.audit.server.AuditServerConstants.PROP_PARTITION_OVERRIDE_PREFIX;

/***************************************************************************
 SourceBasedPartitioner is a custom Kafka partitioner for audit messages
 - Pre-calculated partition maps for performance
 - Direct partition count assignment per appId
 - Each appId explicitly can define how many partitions it needs via override config
 - Default: 2 partitions per appId if not overridden
 - Total partitions = sum of all partition overrides
 - Dynamic partition assignment updates
 ***************************************************************************/
public class AuditSourceBasedPartitioner implements Partitioner {
    private static final Logger LOG = LoggerFactory.getLogger(AuditSourceBasedPartitioner.class);

    // Pre-calculated partition assignment maps
    private final Map<String, Map<String, List<Integer>>> partitionAssignmentCache = new ConcurrentHashMap<>();
    private final Map<String, Integer>                    topicPartitionCounts     = new ConcurrentHashMap<>();
    private final Map<String, List<Integer>>              topicReservedPartitions  = new ConcurrentHashMap<>();
    private       Map<String, Integer>                    sourcePartitionCounts    = new HashMap<>();
    private volatile boolean                              isConfigured;

    public AuditSourceBasedPartitioner() {
        LOG.info("AuditSourceBasedPartitioner engaged...");
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        LOG.debug("==> AuditSourceBasedPartitioner.partition()");

        if (key == null || !isConfigured) {
            LOG.warn("Key is null or AuditSourceBasedPartitioner not configured yet, using default partition 0");
            return 0;
        }

        String source        = key.toString();
        int    numPartitions = cluster.partitionCountForTopic(topic);

        // Check if partition count has changed and update cache if needed
        updatePartitionAssignmentIfNeeded(topic, numPartitions);

        // Get partition assignment map for this topic
        Map<String, List<Integer>> sourcePartitionMap = partitionAssignmentCache.get(topic);
        if (sourcePartitionMap == null) {
            LOG.warn("No partition assignment found for topic '{}', using hash-based assignment", topic);
            return (source.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }

        // Get partitions for this source, fallback to reserved partitions for unknown sources
        List<Integer> partitions = sourcePartitionMap.get(source);
        int partition;
        if (partitions == null || partitions.isEmpty()) {
            // For unconfigured sources, use reserved partitions
            List<Integer> reservedPartitions = topicReservedPartitions.get(topic);
            if (reservedPartitions != null && !reservedPartitions.isEmpty()) {
                // Hash to one of the reserved partitions
                partition = reservedPartitions.get((source.hashCode() & Integer.MAX_VALUE) % reservedPartitions.size());
                LOG.debug("AuditSourceBasedPartitioner.partition(): Unconfigured source '{}' routed to reserved partition: {} from pool: {}", source, partition, reservedPartitions);
            } else {
                // Fallback to hash-based assignment if no reserved partitions.
                // NOTE: This will result is usage of an assigned partition
                partition = (source.hashCode() & Integer.MAX_VALUE) % numPartitions;
                LOG.warn("AuditSourceBasedPartitioner.partition(): Unconfigured source '{}' with no reserved partitions available, using hash-based assignment: {}", source, partition);
            }
        } else {
            // Use round-robin selection within assigned partitions based on hash
            partition = partitions.get((source.hashCode() & Integer.MAX_VALUE) % partitions.size());
            LOG.debug("AuditSourceBasedPartitioner.partition(): Source '{}' assigned to partition: {} from available partitions: {}", source, partition, partitions);
        }

        LOG.debug("<== AuditSourceBasedPartitioner.partition()");

        return partition;
    }

    /**
     * Update partition assignment if partition count has changed
     */
    private void updatePartitionAssignmentIfNeeded(String topic, int currentPartitions) {
        LOG.debug("==> AuditSourceBasedPartitioner.updatePartitionAssignmentIfNeeded()");

        Integer cachedPartitions = topicPartitionCounts.get(topic);

        if (cachedPartitions == null || !cachedPartitions.equals(currentPartitions)) {
            // Update partition count cache
            topicPartitionCounts.put(topic, currentPartitions);

            // Recalculate and update partition assignments
            Map<String, List<Integer>> newAssignments = calculatePartitionAssignments(topic, currentPartitions);
            partitionAssignmentCache.put(topic, newAssignments);

            AuditServerLogFormatter.builder("Partition Assignment Updated - " + topic)
                    .add("Previous partition count", cachedPartitions)
                    .add("New partition count", currentPartitions)
                    .add("New assignments", newAssignments)
                    .add("Reserved partitions", topicReservedPartitions.get(topic))
                    .logInfo(LOG);
        }

        LOG.debug("<== AuditSourceBasedPartitioner.updatePartitionAssignmentIfNeeded()");
    }

    /**
     * - Default: 2 partitions per appId if not overridden
     * - Total partitions = sum of all partition counts
     * - Reserved partitions for unconfigured appIds
     */
    private Map<String, List<Integer>> calculatePartitionAssignments(String topic, int numPartitions) {
        LOG.info("==> AuditSourceBasedPartitioner.calculatePartitionAssignments() for the audit sources");

        Map<String, List<Integer>> ret = new HashMap<>();
        List<String> sources = new ArrayList<>(sourcePartitionCounts.keySet());
        int numSources = sources.size();

        LOG.info("Calculating partition assignments - total partitions: {}, sources: {}", numPartitions, numSources);

        // Calculate total partitions needed
        int totalPartitionsNeeded = sourcePartitionCounts.values().stream().mapToInt(Integer::intValue).sum();

        LOG.info("Total partitions needed: {} (sum of all overrides)", totalPartitionsNeeded);

        // checks to guard from manual update of partition happened via kafka api.
        if (numPartitions < totalPartitionsNeeded) {
            String errorMsg = String.format(
                    "INSUFFICIENT PARTITIONS: Cannot assign requested partitions to all sources. " +
                            "Required: %d partitions (sum of all overrides), Available: %d partitions. " +
                            "This usually indicates partition auto-update failed. Check logs for partition update errors. " +
                            "Configured sources and their partition counts: %s",
                    totalPartitionsNeeded, numPartitions, sourcePartitionCounts);
            LOG.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }

        // Assign partitions directly based on counts
        int currentPartition = 0;
        for (String source : sources) {
            int partitionsForSource = sourcePartitionCounts.get(source);

            // Initialize list for this source
            ret.put(source, new ArrayList<>());

            // Assign consecutive partitions to this source
            for (int j = 0; j < partitionsForSource && currentPartition < numPartitions; j++) {
                ret.get(source).add(currentPartition);
                currentPartition++;
            }

            // Validate: Every source MUST have at least one partition
            if (ret.get(source).isEmpty()) {
                String errorMsg = String.format("PARTITION ASSIGNMENT FAILED: Source '%s' has no partitions assigned.", source);
                LOG.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }

            LOG.info("Source '{}' assigned to partition(s): {} ({} partitions requested)", source, ret.get(source), partitionsForSource);
        }

        // Calculate reserved partitions for unconfigured appIds
        List<Integer> reservedPartitions = new ArrayList<>();
        for (int i = currentPartition; i < numPartitions; i++) {
            reservedPartitions.add(i);
        }
        topicReservedPartitions.put(topic, reservedPartitions);

        AuditServerLogFormatter.builder("Partition Assignments Calculated")
                .add("Total partitions available", numPartitions)
                .add("Total partitions assigned to configured appIds", currentPartition)
                .add("Reserved partitions for unconfigured appIds", reservedPartitions.size())
                .add("Reserved partition IDs", reservedPartitions)
                .add("Number of configured sources", sources.size())
                .add("Partition counts", sourcePartitionCounts)
                .add("Assignments", ret)
                .logInfo(LOG);

        LOG.info("<== AuditSourceBasedPartitioner.calculatePartitionAssignments()");

        return ret;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.debug("==> AuditSourceBasedPartitioner.configure(): Configure Source Partition with Configs {}", configs);

        if (MapUtils.isNotEmpty(configs)) {
            // Load default partition counts (2 per appId by default)
            sourcePartitionCounts.putAll(getDefaultSourcePartitionCounts(configs));

            String propPrefix = AuditProviderFactory.AUDIT_DEST_BASE + "." + AuditServerConstants.DEFAULT_SERVICE_NAME;
            String partitionOverridePropPrefix = propPrefix + "." + PROP_PARTITION_OVERRIDE_PREFIX + ".";

            // Load custom partition overrides from configuration
            for (Map.Entry<String, ?> entry : configs.entrySet()) {
                String key = entry.getKey();

                if (key.startsWith(partitionOverridePropPrefix)) {
                    String source = key.substring(partitionOverridePropPrefix.length());
                    try {
                        int partitionCount = Integer.parseInt(entry.getValue().toString());
                        if (partitionCount > 0) {
                            sourcePartitionCounts.put(source, partitionCount);
                            LOG.info("Configured partition override: {} = {} partitions", source, partitionCount);
                        } else {
                            LOG.warn("Invalid partition count for source '{}': {}. Count must be positive. Using default.",
                                    source, partitionCount);
                        }
                    } catch (NumberFormatException e) {
                        LOG.warn("Invalid partition count value for source '{}': {}. Using default.",
                                source, entry.getValue(), e);
                    }
                }
            }

            LOG.info("Configured source partition counts: {}", sourcePartitionCounts);

            // Clear existing partition assignments when configuration changes
            partitionAssignmentCache.clear();
            topicPartitionCounts.clear();

            // Mark as configured
            isConfigured = true;
        } else {
            LOG.error("FATAL: No appIds configured for partitioner. Cannot proceed.");
            String message = "Partitioner configuration failed: No config related to partition found";
            throw new IllegalStateException(message);
        }

        LOG.debug("<== AuditSourceBasedPartitioner.configure()");
    }

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {}

    /**
     * Get current source partition counts
     */
    public Map<String, Integer> getSourcePartitionCounts() {
        return new HashMap<>(sourcePartitionCounts);
    }

    /**
     * Check if partitioner is configured
     */
    public boolean isConfigured() {
        return isConfigured;
    }

    /**
     * Get default partition counts for all configured appIds
     * Default: 2 partitions per appId
     */
    private Map<String, Integer> getDefaultSourcePartitionCounts(Map<String, ?> configs) {
        LOG.debug("==> AuditSourceBasedPartitioner.getDefaultSourcePartitionCounts() configs: {}", configs);

        Map<String, Integer> ret              = new HashMap<>();
        String               prefix           = AuditProviderFactory.AUDIT_DEST_BASE + "." + AuditServerConstants.DEFAULT_SERVICE_NAME;
        String               configuredAppIds = (String) configs.get(prefix + "." + PROP_CONFIGURED_APP_IDS);

        // Default appIds that are configured in this instance of audit Server
        if (StringUtils.isEmpty(configuredAppIds)) {
            configuredAppIds = DEFAULT_CONFIGURED_APP_IDS;
        }

        String[] appIds = configuredAppIds.split(",");
        if (!ArrayUtils.isEmpty(appIds)) {
            // Each appId gets DEFAULT_PARTITIONS_PER_APPID (2) partitions by default
            // This can be overridden with partitioner.override.<appId> config
            // e.g xasecure.audit.destination.kafka.partitioner.override.hdfs = 5,
            //     xasecure.audit.destination.kafka.partitioner.override.hive = 3 etc.
            Arrays.stream(appIds).forEach(appId -> ret.put(appId.trim(), DEFAULT_PARTITIONS_PER_APP_ID));

            LOG.info("Default partition counts: {} appIds × {} partitions = {} total partitions needed", appIds.length, DEFAULT_PARTITIONS_PER_APP_ID, appIds.length * DEFAULT_PARTITIONS_PER_APP_ID);
        }

        LOG.debug("<== AuditSourceBasedPartitioner.getDefaultSourcePartitionCounts(): {}", ret);

        return ret;
    }
}
