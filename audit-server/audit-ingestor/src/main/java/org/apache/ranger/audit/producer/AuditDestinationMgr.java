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

package org.apache.ranger.audit.producer;

import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.producer.kafka.AuditMessageQueue;
import org.apache.ranger.audit.provider.AuditHandler;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConfig;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.util.Collection;
import java.util.Properties;

@Component
public class AuditDestinationMgr {
    private static final Logger LOG = LoggerFactory.getLogger(AuditDestinationMgr.class);

    private AuditHandler auditHandler;

    @PostConstruct
    public void init() {
        LOG.info("==> AuditDestinationMgr.init()");

        AuditServerConfig auditConfig                      = AuditServerConfig.getInstance();
        Properties        properties                       = auditConfig.getProperties();
        String            kafkaDestPrefix                  = AuditProviderFactory.AUDIT_DEST_BASE + "." + AuditServerConstants.DEFAULT_SERVICE_NAME;
        boolean           isAuditToKafkaDestinationEnabled = MiscUtil.getBooleanProperty(properties, kafkaDestPrefix, false);

        if (isAuditToKafkaDestinationEnabled) {
            auditHandler = new AuditMessageQueue();

            auditHandler.init(properties, kafkaDestPrefix);
            auditHandler.start();

            LOG.info("Kafka producer initialized and started");
        } else {
            LOG.warn("Kafka audit destination is not enabled. Producer service will not function.");
        }

        LOG.info("<== AuditDestinationMgr.init(): auditDestination: {} ", kafkaDestPrefix);
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("==> AuditDestinationMgr.shutdown()");

        AuditHandler auditHandler = this.auditHandler;

        if (auditHandler != null) {
            try {
                LOG.info("Shutting down audit handler: {}", auditHandler.getClass().getSimpleName());

                auditHandler.stop();

                LOG.info("Audit handler shutdown completed successfully");
            } catch (Exception e) {
                LOG.error("Error shutting down audit handler", e);
            } finally {
                this.auditHandler = null;
            }
        }

        LOG.info("<== AuditDestinationMgr.shutdown()");
    }

    /**
     * @param events List of audit events to process as a batch
     * @param appId The application ID for this batch (used as Kafka partition key)
     * @return true if batch was processed successfully, false otherwise
     * @throws Exception if processing fails
     */
    public boolean logBatch(Collection<? extends AuditEventBase> events, String appId) throws Exception {
        if (auditHandler == null) {
            init();
        }

        LOG.debug("Processing batch of {} events with appId: {}", events.size(), appId);

        AuditHandler auditHandler = this.auditHandler;
        boolean      ret          = auditHandler != null && auditHandler.log((Collection<AuditEventBase>) events, appId);

        if (!ret) {
            LOG.error("Batch processing failed for {} events with appId: {}. Events have been spooled to recovery system.", events.size(), appId);
        }

        return ret;
    }
}
