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
import org.apache.ranger.audit.dispatcher.kafka.AuditDispatcherRegistry;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.server.HdfsDispatcherConfig;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Spring component that manages the lifecycle of HDFS dispatcher threads.
 * This manager:
 * - Initializes the dispatcher registry
 * - Creates HDFS dispatcher instances
 * - Starts dispatcher threads
 * - Handles graceful shutdown
 */
@Component
public class HdfsDispatcherManager {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsDispatcherManager.class);

    private final AuditDispatcherRegistry dispatcherRegistry = AuditDispatcherRegistry.getInstance();
    private final List<AuditDispatcher>   dispatchers        = new ArrayList<>();
    private final List<Thread>            dispatcherThreads  = new ArrayList<>();

    @PostConstruct
    public void init() {
        LOG.info("==> HdfsDispatcherManager.init()");

        String dispatcherType = System.getProperty("ranger.audit.dispatcher.type");
        if (dispatcherType != null && !dispatcherType.equalsIgnoreCase("hdfs")) {
            LOG.info("Skipping HdfsDispatcherManager initialization since dispatcher type is {}", dispatcherType);
            return;
        }

        try {
            HdfsDispatcherConfig config = HdfsDispatcherConfig.getInstance();
            Properties props = config.getProperties();

            if (props == null) {
                LOG.error("Configuration properties are null");
                throw new RuntimeException("Failed to load configuration");
            }

            // Initialize and register HDFS Dispatcher
            initializeDispatcherClasses(props, AuditServerConstants.PROP_KAFKA_PROP_PREFIX);

            // Create dispatchers from registry
            List<AuditDispatcher> createdDispatchers = dispatcherRegistry.createDispatchers(props, AuditServerConstants.PROP_KAFKA_PROP_PREFIX);
            dispatchers.addAll(createdDispatchers);

            if (dispatchers.isEmpty()) {
                LOG.warn("No dispatchers were created! Verify that xasecure.audit.destination.hdfs=true");
            } else {
                LOG.info("Created {} HDFS dispatcher(s)", dispatchers.size());

                // Start dispatcher threads
                startDispatchers();
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize HdfsDispatcherManager", e);
            throw new RuntimeException("Failed to initialize HdfsDispatcherManager", e);
        }

        LOG.info("<== HdfsDispatcherManager.init() - {} dispatcher thread(s) started", dispatcherThreads.size());
    }

    private void initializeDispatcherClasses(Properties props, String propPrefix) {
        LOG.info("==> HdfsDispatcherManager.initializeDispatcherClasses()");

        String clsStr = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_CLASSES, "org.apache.ranger.audit.dispatcher.kafka.AuditHDFSDispatcher");

        String[] hdfsDispatcherClasses = clsStr.split(",");

        LOG.info("Initializing {} dispatcher class(es)", hdfsDispatcherClasses.length);

        for (String hdfsDispatcherClassName : hdfsDispatcherClasses) {
            hdfsDispatcherClassName = hdfsDispatcherClassName.trim();

            if (hdfsDispatcherClassName.isEmpty()) {
                continue;
            }

            try {
                Class<?> dispatcherClass = Class.forName(hdfsDispatcherClassName);
                LOG.info("Successfully initialized dispatcher class: {}", dispatcherClass.getName());
            } catch (ClassNotFoundException e) {
                LOG.error("Dispatcher class not found: {}. Ensure the class is on the classpath.", hdfsDispatcherClassName, e);
            } catch (Exception e) {
                LOG.error("Error initializing dispatcher class: {}", hdfsDispatcherClassName, e);
            }
        }

        LOG.info("Registered dispatcher factories: {}", dispatcherRegistry.getRegisteredDestinationTypes());
        LOG.info("<== HdfsDispatcherManager.initializeDispatcherClasses()");
    }

    /**
     * Start all dispatcher threads
     */
    private void startDispatchers() {
        LOG.info("==> HdfsDispatcherManager.startDispatchers()");

        logDispatcherStartup();

        for (AuditDispatcher dispatcher : dispatchers) {
            try {
                String dispatcherName = dispatcher.getClass().getSimpleName();
                Thread dispatcherThread = new Thread(dispatcher, dispatcherName);
                dispatcherThread.setDaemon(true);
                dispatcherThread.start();
                dispatcherThreads.add(dispatcherThread);

                LOG.info("Started {} thread [Thread-ID: {}, Thread-Name: '{}']",
                        dispatcherName, dispatcherThread.getId(), dispatcherThread.getName());
            } catch (Exception e) {
                LOG.error("Error starting dispatcher: {}", dispatcher.getClass().getSimpleName(), e);
            }
        }

        LOG.info("<== HdfsDispatcherManager.startDispatchers() - {} thread(s) started", dispatcherThreads.size());
    }

    private void logDispatcherStartup() {
        LOG.info("################## HDFS DISPATCHER SERVICE STARTUP ######################");

        if (dispatchers.isEmpty()) {
            LOG.warn("WARNING: No HDFS dispatchers are enabled!");
            LOG.warn("Verify: xasecure.audit.destination.hdfs=true in configuration");
        } else {
            AuditServerLogFormatter.LogBuilder builder = AuditServerLogFormatter.builder("HDFS Dispatcher Status");

            for (AuditDispatcher dispatcher : dispatchers) {
                String dispatcherType = dispatcher.getClass().getSimpleName();
                builder.add(dispatcherType, "ENABLED");
                builder.add("Topic", dispatcher.getTopicName());
            }

            builder.logInfo(LOG);
            LOG.info("Starting {} HDFS dispatcher thread(s)...", dispatchers.size());
        }
        LOG.info("########################################################################");
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("==> HdfsDispatcherManager.shutdown()");

        // Shutdown all dispatchers
        for (AuditDispatcher dispatcher : dispatchers) {
            try {
                LOG.info("Shutting down dispatcher: {}", dispatcher.getClass().getSimpleName());
                dispatcher.shutdown();
                LOG.info("Dispatcher shutdown completed: {}", dispatcher.getClass().getSimpleName());
            } catch (Exception e) {
                LOG.error("Error shutting down dispatcher: {}", dispatcher.getClass().getSimpleName(), e);
            }
        }

        // Wait for threads to terminate
        for (Thread thread : dispatcherThreads) {
            if (thread.isAlive()) {
                try {
                    LOG.info("Waiting for thread to terminate: {}", thread.getName());
                    thread.join(10000); // Wait up to 10 seconds
                    if (thread.isAlive()) {
                        LOG.warn("Thread did not terminate within 10 seconds: {}", thread.getName());
                    }
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting for thread to terminate: {}", thread.getName(), e);
                    Thread.currentThread().interrupt();
                }
            }
        }

        dispatchers.clear();
        dispatcherThreads.clear();
        dispatcherRegistry.clearActiveDispatchers();

        LOG.info("<== HdfsDispatcherManager.shutdown() - All HDFS dispatchers stopped");
    }
}
