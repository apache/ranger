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


package org.apache.ranger.authorization.kafka.authorizer;

import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class RangerKafkaAuditHandler extends RangerDefaultAuditHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RangerKafkaAuditHandler.class);

    private AuthzAuditEvent auditEvent      = null;

    public RangerKafkaAuditHandler(){
    }

    @Override
    public void processResult(RangerAccessResult result) {
        // If Cluster Resource Level Topic Creation is not Allowed we don't audit.
        // Subsequent call from Kafka for Topic Creation at Topic resource Level will be audited.
        if(LOG.isTraceEnabled()) {
            LOG.trace("==> RangerKafkaAuditHandler.processResult()");
        }
        if (!isAuditingNeeded(result)) {
            return;
        }
        auditEvent = super.getAuthzEvents(result);
        if(LOG.isTraceEnabled()) {
            LOG.trace("<== RangerKafkaAuditHandler.processResult()");
        }
    }
    @Override
    public void processResults(Collection<RangerAccessResult> results) {
        if(LOG.isTraceEnabled()) {
            LOG.trace("==> RangerKafkaAuditHandler.processResults(" + results + ")");
        }
        if (results!=null){
            for(RangerAccessResult res: results){
                processResult(res);
                flushAudit();
            }
        }

        if(LOG.isTraceEnabled()) {
            LOG.trace("<== RangerKafkaAuditHandler.processResults(" + results + ")");
        }
    }


    private boolean isAuditingNeeded(final RangerAccessResult result) {
        if(LOG.isTraceEnabled()) {
            LOG.trace("==> RangerKafkaAuditHandler.isAuditingNeeded()");
        }
        boolean ret = true;
        boolean 			    isAllowed = result.getIsAllowed();
        RangerAccessRequest       request = result.getAccessRequest();
        RangerAccessResourceImpl resource = (RangerAccessResourceImpl) request.getResource();
        String resourceName 			  = (String) resource.getValue(RangerKafkaAuthorizer.KEY_CLUSTER);
        if (resourceName != null) {
            if (request.getAccessType().equalsIgnoreCase(RangerKafkaAuthorizer.ACCESS_TYPE_CREATE) && !isAllowed) {
                ret = false;
            }
        }
        if(LOG.isTraceEnabled()) {
            LOG.trace("RangerKafkaAuditHandler: isAuditingNeeded()");
            LOG.trace("request:"+request);
            LOG.trace("resource:"+resource);
            LOG.trace("resourceName:"+resourceName);
            LOG.trace("request.getAccessType():"+request.getAccessType());
            LOG.trace("isAllowed:"+isAllowed);
            LOG.trace("ret="+ret);
            LOG.trace("<== RangerKafkaAuditHandler.isAuditingNeeded() = "+ret+" for result="+result);
        }
        return ret;
    }

    public void flushAudit() {
        if(LOG.isTraceEnabled()) {
            LOG.trace("==> RangerKafkaAuditHandler.flushAudit(" + "AuditEvent: " + auditEvent+")");
        }
        if (auditEvent != null) {
            super.logAuthzAudit(auditEvent);
        }
        if(LOG.isTraceEnabled()) {
            LOG.trace("<== RangerKafkaAuditHandler.flushAudit()");
        }
    }
}
