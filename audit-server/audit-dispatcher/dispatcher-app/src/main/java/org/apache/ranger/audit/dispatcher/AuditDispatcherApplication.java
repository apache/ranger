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

import java.io.File;
import java.util.Properties;

public class AuditDispatcherApplication {
    private static final Logger LOG                = LoggerFactory.getLogger(AuditDispatcherApplication.class);
    private static final String APP_NAME           = "audit-dispatcher";
    private static final String CONFIG_PREFIX      = "ranger.audit.dispatcher.";

    private AuditDispatcherApplication() {
    }

    public static void main(String[] args) {
        String dispatcherType = System.getProperty(CONFIG_PREFIX + "type");
        if (dispatcherType == null) {
            LOG.error("Dispatcher initialization failed.");
            LOG.error("No dispatcher type specified. Please set [ranger.audit.dispatcher.type] system property.");
            System.exit(1);
        }

        AuditConfig config = new AuditConfig();
        String specificConfig = getDispatcherConfigPath(dispatcherType);
        if (!config.addResourceIfReadable(specificConfig)) {
            LOG.error("Failed to load dispatcher configuration: {}", specificConfig);
            System.exit(1);
        }
        LOG.info("Loaded dispatcher-specific configuration: {}", specificConfig);

        LOG.info("==========================================================================");
        LOG.info("==> Starting Ranger Audit Dispatcher Service (Type: {})", dispatcherType);
        LOG.info("==========================================================================");

        // Initialization dispatcher manager based on dispatcher type before starting EmbeddedServer
        boolean initSuccess = false;
        try {
            String dispatcherMgrClass = config.get(CONFIG_PREFIX + dispatcherType + ".class");
            if (dispatcherMgrClass != null && !dispatcherMgrClass.trim().isEmpty()) {
                initSuccess = initializeDispatcherManager(dispatcherMgrClass, config.getProperties());
            } else {
                LOG.error("Unknown dispatcher type: {}. Cannot initialize dispatcher manager.", dispatcherType);
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize DispatcherManager", e);
        }

        if (!initSuccess) {
            LOG.error("Dispatcher initialization failed.");
            System.exit(1);
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

    private static boolean initializeDispatcherManager(String dispatcherMgrClass, Properties props) throws Exception {
        Object manager = Class.forName(dispatcherMgrClass, true, Thread.currentThread().getContextClassLoader()).newInstance();
        manager.getClass().getMethod("init", Properties.class).invoke(manager, props);
        LOG.info("{} initialized successfully", dispatcherMgrClass);
        return true;
    }

    private static String getDispatcherConfigPath(String dispatcherType) {
        String confDir = System.getenv("AUDIT_DISPATCHER_CONF_DIR");
        if (confDir == null || confDir.trim().isEmpty()) {
            confDir = System.getProperty("ranger.audit.dispatcher.conf.dir");
        }

        String fileName = "ranger-audit-dispatcher-" + dispatcherType + "-site.xml";
        if (confDir != null && !confDir.trim().isEmpty()) {
            return confDir + File.separator + fileName;
        }

        return fileName;
    }
}
