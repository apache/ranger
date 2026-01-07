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

package org.apache.ranger.audit.destination;

import org.apache.ranger.audit.destination.kafka.AuditMessageQueue;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.AuditHandler;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditConfig;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.util.Properties;

@Component
public class AuditDestinationMgr {
    public  AuditHandler     auditHandler;
    public  AuditServerUtils auditServerUtils;
    private static final Logger LOG = LoggerFactory.getLogger(AuditDestinationMgr.class);

    @PostConstruct
    public void configure() {
        init();
    }

    public void init() {
        LOG.info("==> AuditDestinationMgr.init()");

        auditServerUtils = new AuditServerUtils();
        AuditConfig auditConfig = AuditConfig.getInstance();
        Properties  properties  = auditConfig.getProperties();
        if (properties != null) {
            auditServerUtils.setAuditConfig(properties);
        }

        String kafkaDestPrefix = AuditProviderFactory.AUDIT_DEST_BASE + "." + AuditServerConstants.DEFAULT_SERVICE_NAME;
        boolean isAuditToKafkaDestinationEnabled = MiscUtil.getBooleanProperty(properties, kafkaDestPrefix, false);
        if (isAuditToKafkaDestinationEnabled) {
            auditHandler = new AuditMessageQueue();
            auditHandler.init(properties, kafkaDestPrefix);
            auditHandler.start();
        }

        LOG.info("<== AuditDestinationMgr.init() AuditDestination: {} ", kafkaDestPrefix);
    }

    public boolean log(AuthzAuditEvent authzAuditEvent) throws Exception {
        boolean ret = false;

        if (auditHandler == null) {
            init();
        }
        ret = auditHandler.log(authzAuditEvent);

        return ret;
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("==> AuditDestinationMgr.shutdown()");

        if (auditHandler != null) {
            try {
                LOG.info("Shutting down audit handler: {}", auditHandler.getClass().getSimpleName());
                auditHandler.stop();
                LOG.info("Audit handler shutdown completed successfully");
            } catch (Exception e) {
                LOG.error("Error shutting down audit handler", e);
            }
        }

        LOG.info("<== AuditDestinationMgr.shutdown()");
    }
}
