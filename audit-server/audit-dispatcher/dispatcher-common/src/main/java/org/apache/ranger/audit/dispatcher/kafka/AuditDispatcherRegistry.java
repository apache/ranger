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

package org.apache.ranger.audit.dispatcher.kafka;

import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for managing audit dispatcher factories and instances.
 * Supports dynamic dispatcher registration and creation based on configuration.
 */
public class AuditDispatcherRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(AuditDispatcherRegistry.class);

    private static final AuditDispatcherRegistry      auditDispatcherRegistry = new AuditDispatcherRegistry();
    private final Map<String, AuditDispatcherFactory> dispatcherFactories     = new ConcurrentHashMap<>();
    private final Map<String, AuditDispatcher>        activeDispatchers       = new ConcurrentHashMap<>();

    private AuditDispatcherRegistry() {
        LOG.debug("AuditDispatcherRegistry instance created by classloader: {}", this.getClass().getClassLoader());
    }

    public static AuditDispatcherRegistry getInstance() {
        return auditDispatcherRegistry;
    }

    /**
     * Register a dispatcher factory for a specific destination type.
     * This method can be called early in the application lifecycle (static initializers)
     * to register dispatchers before the AuditMessageQueue is initialized.
     *
     * @param destinationType The destination type identifier (e.g., "solr", "hdfs", "elasticsearch")
     * @param factory The factory that creates dispatcher instances
     */
    public void registerFactory(String destinationType, AuditDispatcherFactory factory) {
        if (destinationType == null || destinationType.trim().isEmpty()) {
            LOG.warn("Attempted to register factory with null or empty destination type");
            return;
        }

        if (factory == null) {
            LOG.warn("Attempted to register null factory for destination type: {}", destinationType);
            return;
        }

        AuditDispatcherFactory existing = dispatcherFactories.put(destinationType, factory);
        if (existing != null) {
            LOG.warn("Replaced existing factory for destination type: {}", destinationType);
        } else {
            LOG.info("Registered dispatcher factory for destination type: {}", destinationType);
        }
    }

    /**
     * Create dispatcher instances for all enabled destinations based on configuration.
     *
     * @param props Configuration properties
     * @param propPrefix Property prefix for Kafka configuration
     * @return List of created dispatcher instances
     */
    public List<AuditDispatcher> createDispatchers(Properties props, String propPrefix) {
        LOG.info("==> AuditDispatcherRegistry.createDispatchers()");

        List<AuditDispatcher> dispatchers = new ArrayList<>();

        for (Map.Entry<String, AuditDispatcherFactory> entry : dispatcherFactories.entrySet()) {
            String               destinationType = entry.getKey();
            AuditDispatcherFactory factory         = entry.getValue();
            String               destPropPrefix  = AuditProviderFactory.AUDIT_DEST_BASE + "." + destinationType;
            boolean              isEnabled       = MiscUtil.getBooleanProperty(props, destPropPrefix, false);

            if (isEnabled) {
                try {
                    LOG.info("Creating dispatcher for enabled destination: {}", destinationType);
                    AuditDispatcher dispatcher = factory.createDispatcher(props, propPrefix);

                    if (dispatcher != null) {
                        dispatchers.add(dispatcher);
                        activeDispatchers.put(destinationType, dispatcher);
                        LOG.info("Successfully created dispatcher for destination: {}", destinationType);
                    } else {
                        LOG.warn("Factory returned null dispatcher for destination: {}", destinationType);
                    }
                } catch (Exception e) {
                    LOG.error("Failed to create dispatcher for destination: {}", destinationType, e);
                }
            } else {
                LOG.debug("Destination '{}' is disabled (property: {} = false)", destinationType, destPropPrefix);
            }
        }

        LOG.info("<== AuditDispatcherRegistry.createDispatchers(): Created {} dispatchers out of {} registered factories", dispatchers.size(), dispatcherFactories.size());

        return dispatchers;
    }

    public Collection<AuditDispatcher> getActiveDispatchers() {
        return activeDispatchers.values();
    }

    public Collection<String> getRegisteredDestinationTypes() {
        return dispatcherFactories.keySet();
    }

    /**
     * Clear all active dispatcher references.
     * Called during shutdown after dispatchers have been stopped.
     */
    public void clearActiveDispatchers() {
        LOG.debug("Clearing {} active dispatcher references", activeDispatchers.size());
        activeDispatchers.clear();
    }

    public int getFactoryCount() {
        return dispatcherFactories.size();
    }
}
