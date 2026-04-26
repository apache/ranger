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

package org.apache.ranger.audit.dispatcher;

import org.apache.ranger.audit.dispatcher.kafka.AuditDispatcher;
import org.apache.ranger.audit.dispatcher.kafka.AuditDispatcherTracker;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;

import java.util.Properties;

/**
 * Spring component that manages the lifecycle of Solr dispatcher threads.
 * Manager that manages the lifecycle of Solr dispatcher threads.
 * - Initializes the dispatcher tracker
 * - Creates Solr dispatcher instances
 * - Starts dispatcher threads
 * - Handles graceful shutdown
 */
public class SolrDispatcherManager {
    private static final Logger LOG                    = LoggerFactory.getLogger(SolrDispatcherManager.class);
    private static final String CONFIG_DISPATCHER_TYPE = "ranger.audit.dispatcher.type";

    private final AuditDispatcherTracker tracker = AuditDispatcherTracker.getInstance();
    private       AuditDispatcher        dispatcher;
    private       Thread                 dispatcherThread;

    @PostConstruct
    public void init(Properties props) {
        LOG.info("==> SolrDispatcherManager.init()");

        String dispatcherType = System.getProperty(CONFIG_DISPATCHER_TYPE);
        if (dispatcherType != null && !dispatcherType.equalsIgnoreCase("solr")) {
            LOG.info("Skipping SolrDispatcherManager initialization since dispatcher type is {}", dispatcherType);
            return;
        }

        try {
            if (props == null) {
                LOG.error("Configuration properties are null");
                throw new RuntimeException("Failed to load configuration");
            }

            boolean isEnabled = MiscUtil.getBooleanProperty(props, "xasecure.audit.destination.solr", false);
            if (!isEnabled) {
                LOG.warn("Solr destination is disabled (xasecure.audit.destination.solr=false). No dispatchers will be created.");
                return;
            }

            // Initialize and register Solr Dispatcher
            initializeDispatcher(props, AuditServerConstants.PROP_KAFKA_PROP_PREFIX);

            if (dispatcher == null) {
                LOG.warn("No dispatcher was created! Verify that xasecure.audit.destination.solr=true and classes are configured correctly.");
            } else {
                LOG.info("Created Solr dispatcher");

                // Register shutdown hook
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    LOG.info("JVM shutdown detected, stopping SolrDispatcherManager...");
                    shutdown();
                }, "SolrDispatcherManager-ShutdownHook"));

                // Start dispatcher thread
                startDispatcher();
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize SolrDispatcherManager", e);
            throw new RuntimeException("Failed to initialize SolrDispatcherManager", e);
        }

        LOG.info("<== SolrDispatcherManager.init()");
    }

    private void initializeDispatcher(Properties props, String propPrefix) {
        LOG.info("==> SolrDispatcherManager.initializeDispatcher()");

        // Get dispatcher classes from configuration
        String clsStr = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_CLASSES,
                "org.apache.ranger.audit.dispatcher.kafka.AuditSolrDispatcher");

        String solrDispatcherClassName = clsStr.split(",")[0].trim();
        if (solrDispatcherClassName.isEmpty()) {
            LOG.error("Dispatcher class name is empty");
            return;
        }

        try {
            Class<?> dispatcherClass = Class.forName(solrDispatcherClassName);
            dispatcher = (AuditDispatcher) dispatcherClass
                    .getConstructor(Properties.class, String.class)
                    .newInstance(props, propPrefix);
            tracker.addActiveDispatcher("solr", dispatcher);
            LOG.info("Successfully initialized dispatcher class: {}", dispatcherClass.getName());
        } catch (ClassNotFoundException e) {
            LOG.error("Dispatcher class not found: {}. Ensure the class is on the classpath.", solrDispatcherClassName, e);
        } catch (Exception e) {
            LOG.error("Error initializing dispatcher class: {}", solrDispatcherClassName, e);
        }

        LOG.info("<== SolrDispatcherManager.initializeDispatcher()");
    }

    /**
     * Start dispatcher thread
     */
    private void startDispatcher() {
        LOG.info("==> SolrDispatcherManager.startDispatcher()");

        logSolrDispatcherStartup();

        if (dispatcher != null) {
            try {
                String dispatcherName = dispatcher.getClass().getSimpleName();
                dispatcherThread = new Thread(dispatcher, dispatcherName);
                dispatcherThread.setDaemon(true);
                dispatcherThread.start();

                LOG.info("Started {} thread [Thread-ID: {}, Thread-Name: '{}']",
                        dispatcherName, dispatcherThread.getId(), dispatcherThread.getName());
            } catch (Exception e) {
                LOG.error("Error starting dispatcher: {}", dispatcher.getClass().getSimpleName(), e);
            }
        }

        LOG.info("<== SolrDispatcherManager.startDispatcher()");
    }

    /**
     * Log startup banner with dispatcher information
     */
    private void logSolrDispatcherStartup() {
        LOG.info("################## SOLR DISPATCHER SERVICE STARTUP ######################");

        if (dispatcher == null) {
            LOG.warn("WARNING: No Solr dispatchers are enabled!");
            LOG.warn("Verify: xasecure.audit.destination.solr=true in configuration");
        } else {
            AuditServerLogFormatter.LogBuilder builder = AuditServerLogFormatter.builder("Solr Dispatcher Status");
            String dispatcherType = dispatcher.getClass().getSimpleName();
            builder.add(dispatcherType, "ENABLED");
            builder.add("Topic", dispatcher.getTopicName());
            builder.logInfo(LOG);

            LOG.info("Starting Solr dispatcher thread...");
        }
        LOG.info("########################################################################");
    }

    public void shutdown() {
        LOG.info("==> SolrDispatcherManager.shutdown()");

        if (dispatcher != null) {
            try {
                LOG.info("Shutting down dispatcher: {}", dispatcher.getClass().getSimpleName());
                dispatcher.shutdown();
                LOG.info("Dispatcher shutdown completed: {}", dispatcher.getClass().getSimpleName());
            } catch (Exception e) {
                LOG.error("Error shutting down dispatcher: {}", dispatcher.getClass().getSimpleName(), e);
            }
        }

        // Wait for thread to terminate
        if (dispatcherThread != null && dispatcherThread.isAlive()) {
            try {
                LOG.info("Waiting for thread to terminate: {}", dispatcherThread.getName());
                dispatcherThread.join(10000); // Wait up to 10 seconds
                if (dispatcherThread.isAlive()) {
                    LOG.warn("Thread did not terminate within 10 seconds: {}", dispatcherThread.getName());
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for thread to terminate: {}", dispatcherThread.getName(), e);
                Thread.currentThread().interrupt();
            }
        }

        dispatcher = null;
        dispatcherThread = null;
        tracker.clearActiveDispatchers();

        LOG.info("<== SolrDispatcherManager.shutdown() - Solr dispatcher stopped");
    }
}
