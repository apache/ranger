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

import java.util.ArrayList;
import java.util.Collection;

public class RangerKafkaAuditHandler extends RangerDefaultAuditHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RangerKafkaAuditHandler.class);

    private AuthzAuditEvent auditEvent      = null;

    private ArrayList<AuthzAuditEvent> auditEventList = new ArrayList<>();

    public RangerKafkaAuditHandler(){
    }

    @Override
    public void processResult(RangerAccessResult result) {
        // If Cluster Resource Level Topic Creation is not Allowed we don't audit.
        // Subsequent call from Kafka for Topic Creation at Topic resource Level will be audited.
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerKafkaAuditHandler.processResult()");
        }
        if (!isAuditingNeeded(result)) {
            return;
        }
        auditEvent = super.getAuthzEvents(result);
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerKafkaAuditHandler.processResult()");
        }
    }
    @Override
    public void processResults(Collection<RangerAccessResult> results) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerKafkaAuditHandler.processResults(" + results + ")");
        }
        for(RangerAccessResult res: results){
            if (isAuditingNeeded(res)){
                AuthzAuditEvent event = super.getAuthzEvents(res);
                if(event!=null){
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Got event=" + event + " for RangerAccessResult=" + res);
                    }
                    auditEventList.add(event);
                }
                else{
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("No audit event for :" + res);
                    }
                }
            }
            else {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Auditing not required for :"+res);
                }
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerKafkaAuditHandler.processResults(" + results + ")");
        }
    }

    private boolean isAuditingNeeded(final RangerAccessResult result) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerKafkaAuditHandler.isAuditingNeeded()");
        }
        boolean ret = true;
        boolean 			    isAllowed = result.getIsAllowed();
        RangerAccessRequest request = result.getAccessRequest();
        RangerAccessResourceImpl resource = (RangerAccessResourceImpl) request.getResource();
        String resourceName 			  = (String) resource.getValue(RangerKafkaAuthorizer.KEY_CLUSTER);
        if (resourceName != null) {
            if (request.getAccessType().equalsIgnoreCase(RangerKafkaAuthorizer.ACCESS_TYPE_CREATE) && !isAllowed) {
                ret = false;
            }
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("RangerKafkaAuditHandler: isAuditingNeeded()");
            LOG.debug("request:"+request);
            LOG.debug("resource:"+resource);
            LOG.debug("resourceName:"+resourceName);
            LOG.debug("request.getAccessType():"+request.getAccessType());
            LOG.debug("isAllowed:"+isAllowed);
            LOG.debug("ret="+ret);
            LOG.debug("<== RangerKafkaAuditHandler.isAuditingNeeded() = "+ret+" for result="+result);
        }
        return ret;
    }

    public void flushAudit() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerKafkaAuditHandler.flushAudit(" + "AuditEvent: " + auditEvent +" list="+ auditEventList+ ")");
        }
        if (auditEvent != null) {
            super.logAuthzAudit(auditEvent);
        }
        else if (auditEventList.size()>0){
            super.logAuthzAudits(auditEventList);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerKafkaAuditHandler.flushAudit()");
        }
    }
}
