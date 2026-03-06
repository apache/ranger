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

package org.apache.ranger.audit.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class for Ranger Audit Server Service.
 * Loads audit server configuration files.
 */
public class AuditServerConfig extends AuditConfig {
    private static final    Logger            LOG              = LoggerFactory.getLogger(AuditServerConfig.class);
    private static final    String            CONFIG_FILE_PATH = "conf/ranger-audit-server-site.xml";
    private static volatile AuditServerConfig sInstance;

    private AuditServerConfig() {
        super();
        addAuditServerResources();
    }

    public static AuditServerConfig getInstance() {
        AuditServerConfig ret = AuditServerConfig.sInstance;

        if (ret == null) {
            synchronized (AuditServerConfig.class) {
                ret = AuditServerConfig.sInstance;

                if (ret == null) {
                    ret = new AuditServerConfig();
                    AuditServerConfig.sInstance = ret;
                }
            }
        }

        return ret;
    }

    private boolean addAuditServerResources() {
        LOG.debug("==> AuditServerConfig.addAuditServerResources()");

        boolean ret = true;

        // Load ranger-audit-server-site.xml
        if (!addAuditResource(CONFIG_FILE_PATH, true)) {
            LOG.error("Could not load required configuration: {}", CONFIG_FILE_PATH);
            ret = false;
        }

        LOG.debug("<== AuditServerConfig.addAuditServerResources(), result={}", ret);

        return ret;
    }
}
