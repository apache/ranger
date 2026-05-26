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
import org.apache.ranger.audit.dispatcher.kafka.AuditOpenSearchDispatcher;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public final class OpenSearchDispatcherManager {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchDispatcherManager.class);
    private static final String CONFIG_DISPATCHER_TYPE = AuditServerConstants.PROP_DISPATCHER_TYPE;
    private static final String TYPE_OPENSEARCH = "opensearch";
    private static final String OPENSEARCH_DEST_PROP = "xasecure.audit.destination.opensearch";
    private static final int    MAX_INIT_ATTEMPTS = 5;
    private static final long   INIT_RETRY_MS     = 5000L;
    private static final long   SHUTDOWN_WAIT_MS  = 10000L;

    private final AuditDispatcherTracker tracker = AuditDispatcherTracker.getInstance();
    private AuditDispatcher dispatcher;
    private Thread          dispatcherThread;

    public void init(final Properties props) {
        LOG.info("==> OpenSearchDispatcherManager.init()");

        String dispatcherType = System.getProperty(CONFIG_DISPATCHER_TYPE);
        if (dispatcherType != null && !dispatcherType.equalsIgnoreCase(TYPE_OPENSEARCH)) {
            LOG.info("Skipping OpenSearchDispatcherManager initialization since dispatcher type is {}", dispatcherType);
            return;
        }

        try {
            if (props == null) {
                LOG.error("Configuration properties are null");
                throw new RuntimeException("Failed to load configuration");
            }

            boolean isEnabled = MiscUtil.getBooleanProperty(props, OPENSEARCH_DEST_PROP, false);
            if (!isEnabled) {
                String clsName = MiscUtil.getStringProperty(props, AuditServerConstants.PROP_DISPATCHER_CLASS);
                if (clsName != null && clsName.contains("AuditOpenSearchDispatcher")) {
                    isEnabled = true;
                }
            }

            if (!isEnabled) {
                LOG.warn("OpenSearch destination is disabled ({}=false). No dispatchers will be created.", OPENSEARCH_DEST_PROP);
                return;
            }

            initializeDispatcher(props, AuditServerConstants.PROP_DISPATCHER_PREFIX);

            if (dispatcher == null) {
                throw new RuntimeException("No OpenSearch dispatcher was created. Verify that " + OPENSEARCH_DEST_PROP + "=true and classes are configured correctly.");
            } else {
                LOG.info("Created OpenSearch dispatcher");

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    LOG.info("JVM shutdown detected, stopping OpenSearchDispatcherManager");
                    shutdown();
                }, "OpenSearchDispatcher-ShutdownHook"));

                startDispatcher();
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize OpenSearchDispatcherManager", e);
            throw new RuntimeException("Failed to initialize OpenSearchDispatcherManager", e);
        }

        LOG.info("<== OpenSearchDispatcherManager.init()");
    }

    public void shutdown() {
        LOG.info("==> OpenSearchDispatcherManager.shutdown()");

        if (dispatcher != null) {
            try {
                LOG.info("Shutting down dispatcher: {}", dispatcher.getClass().getSimpleName());
                dispatcher.shutdown();
                LOG.info("Dispatcher shutdown completed: {}", dispatcher.getClass().getSimpleName());
            } catch (Exception e) {
                LOG.error("Error shutting down dispatcher: {}", dispatcher.getClass().getSimpleName(), e);
            }
        }

        if (dispatcherThread != null && dispatcherThread.isAlive()) {
            try {
                LOG.info("Waiting for thread to terminate: {}", dispatcherThread.getName());
                dispatcherThread.join(SHUTDOWN_WAIT_MS);
                if (dispatcherThread.isAlive()) {
                    LOG.warn("Thread did not terminate within {}ms: {}", SHUTDOWN_WAIT_MS, dispatcherThread.getName());
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for thread to terminate: {}", dispatcherThread.getName(), e);
                Thread.currentThread().interrupt();
            }
        }

        dispatcher = null;
        dispatcherThread = null;
        tracker.clearActiveDispatcher(TYPE_OPENSEARCH);

        LOG.info("<== OpenSearchDispatcherManager.shutdown() - OpenSearch dispatcher stopped");
    }

    private void initializeDispatcher(final Properties props, final String propPrefix) {
        LOG.info("==> OpenSearchDispatcherManager.initializeDispatcher()");

        String clsStr = MiscUtil.getStringProperty(props, AuditServerConstants.PROP_DISPATCHER_CLASS, AuditOpenSearchDispatcher.class.getName());
        String className = clsStr.split(",")[0].trim();

        if (className.isEmpty()) {
            LOG.error("Dispatcher class name is empty");
            return;
        }

        long retryDelay = INIT_RETRY_MS;

        for (int attempt = 1; attempt <= MAX_INIT_ATTEMPTS; attempt++) {
            try {
                Class<?> cls = Class.forName(className);
                dispatcher = (AuditDispatcher) cls.getConstructor(Properties.class, String.class).newInstance(props, propPrefix);
                tracker.addActiveDispatcher(TYPE_OPENSEARCH, dispatcher);
                LOG.info("Successfully initialized dispatcher class: {}", cls.getName());
                break;
            } catch (ClassNotFoundException e) {
                LOG.error("Dispatcher class not found: {}. Ensure the class is on the classpath.", className, e);
                break;
            } catch (Exception e) {
                if (attempt < MAX_INIT_ATTEMPTS) {
                    LOG.warn("Dispatcher init attempt {}/{} failed, retrying in {}ms...", attempt, MAX_INIT_ATTEMPTS, retryDelay, e);
                    try {
                        Thread.sleep(retryDelay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    retryDelay *= 2;
                } else {
                    LOG.error("Error initializing dispatcher class after {} attempts: {}", MAX_INIT_ATTEMPTS, className, e);
                }
            }
        }

        LOG.info("<== OpenSearchDispatcherManager.initializeDispatcher()");
    }

    private void startDispatcher() {
        LOG.info("==> OpenSearchDispatcherManager.startDispatcher()");

        logStartupBanner();

        if (dispatcher != null) {
            try {
                String name = dispatcher.getClass().getSimpleName();
                dispatcherThread = new Thread(dispatcher, name);
                dispatcherThread.setDaemon(true);
                dispatcherThread.start();
                LOG.info("Started {} thread [Thread-ID: {}, Thread-Name: '{}']", name, dispatcherThread.getId(), dispatcherThread.getName());
            } catch (Exception e) {
                LOG.error("Error starting dispatcher: {}", dispatcher.getClass().getSimpleName(), e);
            }
        }

        LOG.info("<== OpenSearchDispatcherManager.startDispatcher()");
    }

    private void logStartupBanner() {
        LOG.info("########## OPENSEARCH DISPATCHER SERVICE STARTUP ##########");

        if (dispatcher == null) {
            LOG.warn("WARNING: No OpenSearch dispatchers are enabled!");
            LOG.warn("Verify: {}=true in configuration", OPENSEARCH_DEST_PROP);
        } else {
            AuditServerLogFormatter.LogBuilder builder = AuditServerLogFormatter.builder("OpenSearch Dispatcher Status");
            String type = dispatcher.getClass().getSimpleName();
            builder.add(type, "ENABLED");
            builder.add("Topic", dispatcher.getTopicName());
            builder.logInfo(LOG);
            LOG.info("Starting OpenSearch dispatcher thread...");
        }

        LOG.info("##########################################################");
    }
}
