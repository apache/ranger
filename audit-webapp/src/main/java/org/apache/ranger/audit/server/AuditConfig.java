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

import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AuditConfig extends RangerConfiguration {
    private static final    Logger      LOG                  = LoggerFactory.getLogger(AuditConfig.class);
    private static final    String      CONFIG_FILE_PATH     = "conf/audit-server-site.xml";
    private static final    String      CORE_SITE_FILE_PATH  = "conf/core-site.xml";
    private static final    String      HDFS_SITE_FILE_PATH  = "conf/hdfs-site.xml";
    private static volatile AuditConfig sInstance;

    private AuditConfig() {
        super();
        addAuditResources();
    }

    public static AuditConfig getInstance() {
        AuditConfig ret = AuditConfig.sInstance;

        if (ret == null) {
            synchronized (AuditConfig.class) {
                ret = AuditConfig.sInstance;

                if (ret == null) {
                    ret = new AuditConfig();
                    AuditConfig.sInstance = ret;
                }
            }
        }

        return ret;
    }

    public Properties getProperties() {
        return this.getProps();
    }

    private boolean addAuditResources() {
        LOG.debug("==> addAuditResources()");

        boolean ret = true;

        // Load core-site.xml
        if (!addResourceIfReadable(CORE_SITE_FILE_PATH)) {
            LOG.warn("Could not add {} to AuditConfig. This is optional but recommended for Hadoop security settings.", CORE_SITE_FILE_PATH);
            // Not setting ret=false since core-site.xml is optional
        } else {
            LOG.info("Successfully loaded {}", CORE_SITE_FILE_PATH);
        }

        // Load hdfs-site.xml
        if (!addResourceIfReadable(HDFS_SITE_FILE_PATH)) {
            LOG.error("Could not add {} to AuditConfig.", HDFS_SITE_FILE_PATH);
            ret = false;
        } else {
            LOG.info("Successfully loaded {}", HDFS_SITE_FILE_PATH);
        }

        // Load audit-server-site.xml
        if (!addResourceIfReadable(CONFIG_FILE_PATH)) {
            LOG.error("Could not add {} to AuditConfig.", CONFIG_FILE_PATH);
            ret = false;
        } else {
            LOG.info("Successfully loaded {}", CONFIG_FILE_PATH);
        }

        LOG.debug("<== addAuditResources(), result={}", ret);

        return ret;
    }
}
