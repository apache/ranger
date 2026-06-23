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

import org.apache.ranger.audit.server.AuditServerConstants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Inputs for building the first partition plan from legacy ingestor XML / producer config. */
public class PartitionPlanBootstrapConfig {
    private final String auditTopic;
    private final String[] configuredPlugins;
    private final int defaultPartitionsPerPlugin;
    private final int bufferPartitionCount;
    private final int hashBasedTopicPartitionCount;
    private final Map<String, Integer> pluginPartitionOverrides;

    public PartitionPlanBootstrapConfig(String auditTopic, String[] configuredPlugins, int defaultPartitionsPerPlugin, int bufferPartitionCount, int hashBasedTopicPartitionCount, Map<String, Integer> pluginPartitionOverrides) {
        this.auditTopic                 = auditTopic;
        this.configuredPlugins          = configuredPlugins != null ? configuredPlugins : new String[0];
        this.defaultPartitionsPerPlugin = defaultPartitionsPerPlugin;
        this.bufferPartitionCount       = bufferPartitionCount;
        this.hashBasedTopicPartitionCount = Math.max(1, hashBasedTopicPartitionCount);
        this.pluginPartitionOverrides   = pluginPartitionOverrides != null ? new LinkedHashMap<>(pluginPartitionOverrides) : Collections.emptyMap();
    }

    public static PartitionPlanBootstrapConfig create(String auditTopic, String[] configuredPlugins, int defaultPartitionsPerPlugin, int bufferPartitionCount) {
        return new PartitionPlanBootstrapConfig(auditTopic, configuredPlugins, defaultPartitionsPerPlugin, bufferPartitionCount, AuditServerConstants.DEFAULT_TOPIC_PARTITIONS, Collections.emptyMap());
    }

    public PartitionPlanBootstrapConfig withPluginOverride(String pluginId, int partitionCount) {
        Map<String, Integer> overrides = new LinkedHashMap<>(pluginPartitionOverrides);
        overrides.put(pluginId, partitionCount);
        return new PartitionPlanBootstrapConfig(auditTopic, configuredPlugins, defaultPartitionsPerPlugin, bufferPartitionCount, hashBasedTopicPartitionCount, overrides);
    }

    public String getAuditTopic() {
        return auditTopic;
    }

    public String[] getConfiguredPlugins() {
        return configuredPlugins;
    }

    public int getBufferPartitionCount() {
        return bufferPartitionCount;
    }

    /** Used when {@link #getConfiguredPlugins()} is empty: matches {@code kafka.topic.partitions} / hash-based mode. */
    public int getHashBasedTopicPartitionCount() {
        return hashBasedTopicPartitionCount;
    }

    public int getPartitionsForPlugin(String pluginId) {
        Integer override = pluginPartitionOverrides.get(pluginId);
        int count = override != null ? override : defaultPartitionsPerPlugin;
        return Math.max(1, count);
    }

    public static PartitionPlanBootstrapConfig fromProducerConfigMap(Map<String, ?> configs, String auditTopic) {
        String propPrefix = AuditServerConstants.PROP_PREFIX_AUDIT_SERVER;
        String pluginsStr = getString(configs, propPrefix + AuditServerConstants.PROP_CONFIGURED_PLUGINS, AuditServerConstants.DEFAULT_CONFIGURED_PLUGINS);
        String[] plugins = parsePluginIds(pluginsStr);
        int defaultPerPlugin = getInt(configs, propPrefix + AuditServerConstants.PROP_TOPIC_PARTITIONS_PER_CONFIGURED_PLUGIN, AuditServerConstants.DEFAULT_PARTITIONS_PER_CONFIGURED_PLUGIN);
        int bufferCount = getInt(configs, propPrefix + AuditServerConstants.PROP_BUFFER_PARTITIONS, AuditServerConstants.DEFAULT_BUFFER_PARTITIONS);
        int hashBasedTopicPartitions = getInt(configs, propPrefix + AuditServerConstants.PROP_TOPIC_PARTITIONS, AuditServerConstants.DEFAULT_TOPIC_PARTITIONS);

        PartitionPlanBootstrapConfig config = new PartitionPlanBootstrapConfig(auditTopic, plugins, defaultPerPlugin, bufferCount, hashBasedTopicPartitions, Collections.emptyMap());
        for (String plugin : plugins) {
            String overrideKey = propPrefix + AuditServerConstants.PROP_PLUGIN_PARTITION_OVERRIDE_PREFIX + plugin;
            if (configs.containsKey(overrideKey)) {
                config = config.withPluginOverride(plugin, getInt(configs, overrideKey, defaultPerPlugin));
            }
        }
        return config;
    }

    private static String[] parsePluginIds(String pluginsStr) {
        if (pluginsStr == null || pluginsStr.isBlank()) {
            return new String[0];
        }
        List<String> plugins = new ArrayList<>();
        for (String plugin : pluginsStr.split(",")) {
            if (plugin != null && !plugin.isBlank()) {
                plugins.add(plugin.trim());
            }
        }
        return plugins.toArray(new String[0]);
    }

    private static String getString(Map<String, ?> configs, String key, String defaultValue) {
        Object val = configs.get(key);
        return val != null ? val.toString() : defaultValue;
    }

    private static int getInt(Map<String, ?> configs, String key, int defaultValue) {
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
