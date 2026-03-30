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

import org.apache.ranger.audit.server.AuditConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditDispatcherApplication {
    private static final Logger LOG                           = LoggerFactory.getLogger(AuditDispatcherApplication.class);
    private static final String APP_NAME                      = "audit-dispatcher";
    private static final String CONFIG_PREFIX                 = "ranger.audit.dispatcher.";
    private static final String COMMON_CONFIG_FILE            = "ranger-audit-dispatcher-site.xml";
    private static final String HDFS_DISPATCHER_MANAGER_CLASS = "org.apache.ranger.audit.dispatcher.HdfsDispatcherManager";
    private static final String SOLR_DISPATCHER_MANAGER_CLASS = "org.apache.ranger.audit.dispatcher.SolrDispatcherManager";

    private AuditDispatcherApplication() {
    }

    public static void main(String[] args) {
        AuditConfig config = AuditConfig.getInstance();
        config.addResourceIfReadable(COMMON_CONFIG_FILE);
        LOG.info("Loaded common configuration from classpath: {}", COMMON_CONFIG_FILE);

        String dispatcherType = System.getProperty(CONFIG_PREFIX + "type");
        if (dispatcherType == null) {
            dispatcherType = config.get(CONFIG_PREFIX + "type");
            if (dispatcherType != null) {
                System.setProperty(CONFIG_PREFIX + "type", dispatcherType);
            }
        }

        // Load dispatcher-specific configuration from classpath
        if (dispatcherType != null) {
            String specificConfig = "ranger-audit-dispatcher-" + dispatcherType + "-site.xml";
            config.addResourceIfReadable(specificConfig);
            LOG.info("Loaded dispatcher-specific configuration from classpath: {}", specificConfig);
        } else {
            LOG.warn("No dispatcher type specified. Service might fail to start correctly.");
        }

        LOG.info("==========================================================================");
        LOG.info("==> Starting Ranger Audit Dispatcher Service (Type: {})", dispatcherType);
        LOG.info("==========================================================================");

        // Initialization dispatcher manager based on dispatcher type before starting EmbeddedServer
        boolean initSuccess = false;
        try {
            if ("hdfs".equalsIgnoreCase(dispatcherType)) {
                initSuccess = initializeDispatcherManager(HDFS_DISPATCHER_MANAGER_CLASS);
            } else if ("solr".equalsIgnoreCase(dispatcherType)) {
                initSuccess = initializeDispatcherManager(SOLR_DISPATCHER_MANAGER_CLASS);
            } else {
                LOG.error("Unknown dispatcher type: {}. Cannot initialize dispatcher manager.", dispatcherType);
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize DispatcherManager", e);
        }

        if (!initSuccess) {
            LOG.error("Dispatcher initialization failed. The service will continue running to allow log inspection, but no audits will be dispatched.");
        }

        try {
            EmbeddedServer server = new EmbeddedServer(config, APP_NAME, CONFIG_PREFIX);
            server.start();

            LOG.info("<== Ranger Audit Dispatcher Service Started Successfully");
        } catch (Exception e) {
            LOG.error("<== Failed to start Ranger Audit Dispatcher Service", e);
            System.exit(1);
        }
    }

    private static boolean initializeDispatcherManager(String managerClassName) throws Exception {
        Object manager = Class.forName(managerClassName, true, Thread.currentThread().getContextClassLoader()).newInstance();
        manager.getClass().getMethod("init").invoke(manager);
        LOG.info("{} initialized successfully", managerClassName);
        return true;
    }
}
