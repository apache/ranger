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
    private final    ConcurrentHashMap<String, AtomicInteger> appIdCounters = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        String propPrefix = AuditServerConstants.PROP_KAFKA_PROP_PREFIX;

        String pluginsStr = getConfig(configs, propPrefix + "." + AuditServerConstants.PROP_CONFIGURED_PLUGINS,  AuditServerConstants.DEFAULT_CONFIGURED_PLUGINS);
        configuredPlugins = pluginsStr.split(",");
        for (int i = 0; i < configuredPlugins.length; i++) {
            configuredPlugins[i] = configuredPlugins[i].trim();
        }

        defaultPartitionsPerPlugin = getIntConfig(configs, propPrefix + "." + AuditServerConstants.PROP_TOPIC_PARTITIONS_PER_CONFIGURED_PLUGIN, AuditServerConstants.DEFAULT_PARTITIONS_PER_CONFIGURED_PLUGIN);
        if (defaultPartitionsPerPlugin < 1) {
            defaultPartitionsPerPlugin = 1;
        }

        totalPartitions = getIntConfig(configs, propPrefix + "." + AuditServerConstants.PROP_TOPIC_PARTITIONS, AuditServerConstants.DEFAULT_TOPIC_PARTITIONS);
        if (totalPartitions < 1) {
            totalPartitions = AuditServerConstants.DEFAULT_TOPIC_PARTITIONS;
        }

        // Build per-plugin partition counts - check for individual overrides per plugin
        pluginPartitionCounts = new int[configuredPlugins.length];
        for (int i = 0; i < configuredPlugins.length; i++) {
            String plugin = configuredPlugins[i];
            String overrideKey = propPrefix + "." + AuditServerConstants.PROP_PLUGIN_PARTITION_OVERRIDE_PREFIX + plugin;
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
        int numPartitions = totalPartitions;
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        if (partitions != null && !partitions.isEmpty()) {
            numPartitions = partitions.size();
        }

        String appId = key != null ? key.toString() : null;
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
            int subPartition = appIdCounters
                    .computeIfAbsent(appId, k -> new AtomicInteger(0))
                    .getAndIncrement() % rangeSize;
            return start + subPartition;
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
