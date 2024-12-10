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

package org.apache.ranger.plugin.audit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.AuditHandler;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.policyengine.gds.GdsAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class RangerDefaultAuditHandler implements RangerAccessResultProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(RangerDefaultAuditHandler.class);

    private static final String  CONF_AUDIT_ID_STRICT_UUID    = "xasecure.audit.auditid.strict.uuid";
    private static final boolean DEFAULT_AUDIT_ID_STRICT_UUID = false;

    protected final String moduleName;

    private final boolean         auditIdStrictUUID;
    private       long            sequenceNumber;
    private final RangerRESTUtils restUtils = new RangerRESTUtils();
    private       String          uuid      = MiscUtil.generateUniqueId();
    private       AtomicInteger   counter   = new AtomicInteger(0);

    public RangerDefaultAuditHandler() {
        auditIdStrictUUID = DEFAULT_AUDIT_ID_STRICT_UUID;
        moduleName        = RangerHadoopConstants.DEFAULT_RANGER_MODULE_ACL_NAME;
    }

    public RangerDefaultAuditHandler(Configuration config) {
        auditIdStrictUUID = config.getBoolean(CONF_AUDIT_ID_STRICT_UUID, DEFAULT_AUDIT_ID_STRICT_UUID);
        moduleName        = config.get(RangerHadoopConstants.AUDITLOG_RANGER_MODULE_ACL_NAME_PROP, RangerHadoopConstants.DEFAULT_RANGER_MODULE_ACL_NAME);
    }

    @Override
    public void processResult(RangerAccessResult result) {
        LOG.debug("==> RangerDefaultAuditHandler.processResult({})", result);

        AuthzAuditEvent event = getAuthzEvents(result);

        logAuthzAudit(event);

        LOG.debug("<== RangerDefaultAuditHandler.processResult({})", result);
    }

    @Override
    public void processResults(Collection<RangerAccessResult> results) {
        LOG.debug("==> RangerDefaultAuditHandler.processResults({})", results);

        Collection<AuthzAuditEvent> events = getAuthzEvents(results);

        if (events != null) {
            logAuthzAudits(events);
        }

        LOG.debug("<== RangerDefaultAuditHandler.processResults({})", results);
    }

    public AuthzAuditEvent getAuthzEvents(RangerAccessResult result) {
        LOG.debug("==> RangerDefaultAuditHandler.getAuthzEvents({})", result);

        AuthzAuditEvent ret = null;

        RangerAccessRequest request = result != null ? result.getAccessRequest() : null;

        if (request != null && result.getIsAudited()) {
            //RangerServiceDef     serviceDef   = result.getServiceDef();
            RangerAccessResource resource     = request.getResource();
            String               resourceType = resource == null ? null : resource.getLeafName();
            String               resourcePath = resource == null ? null : resource.getAsString();

            ret = createAuthzAuditEvent();

            ret.setRepositoryName(result.getServiceName());
            ret.setRepositoryType(result.getServiceType());
            ret.setResourceType(resourceType);
            ret.setResourcePath(resourcePath);
            ret.setRequestData(request.getRequestData());
            ret.setEventTime(request.getAccessTime() != null ? request.getAccessTime() : new Date());
            ret.setUser(request.getUser());
            ret.setAction(request.getAccessType());
            ret.setAccessResult((short) (result.getIsAllowed() ? 1 : 0));
            ret.setPolicyId(result.getPolicyId());
            ret.setAccessType(request.getAction());
            ret.setClientIP(request.getClientIPAddress());
            ret.setClientType(request.getClientType());
            ret.setSessionId(request.getSessionId());
            ret.setAclEnforcer(moduleName);

            Set<String> tags = getTags(request);
            if (tags != null) {
                ret.setTags(tags);
            }

            ret.setDatasets(getDatasets(request));
            ret.setProjects(getProjects(request));
            ret.setAdditionalInfo(getAdditionalInfo(request));
            ret.setClusterName(request.getClusterName());
            ret.setZoneName(result.getZoneName());
            ret.setAgentHostname(restUtils.getAgentHostname());
            ret.setPolicyVersion(result.getPolicyVersion());

            populateDefaults(ret);

            result.setAuditLogId(ret.getEventId());
        }

        LOG.debug("<== RangerDefaultAuditHandler.getAuthzEvents({}): {}", result, ret);

        return ret;
    }

    public Collection<AuthzAuditEvent> getAuthzEvents(Collection<RangerAccessResult> results) {
        LOG.debug("==> RangerDefaultAuditHandler.getAuthzEvents({})", results);

        List<AuthzAuditEvent> ret = null;

        if (results != null) {
            // TODO: optimize the number of audit logs created
            for (RangerAccessResult result : results) {
                AuthzAuditEvent event = getAuthzEvents(result);

                if (event == null) {
                    continue;
                }

                if (ret == null) {
                    ret = new ArrayList<>();
                }

                ret.add(event);
            }
        }

        LOG.debug("<== RangerDefaultAuditHandler.getAuthzEvents({}): {}", results, ret);

        return ret;
    }

    public void logAuthzAudit(AuthzAuditEvent auditEvent) {
        LOG.debug("==> RangerDefaultAuditHandler.logAuthzAudit({})", auditEvent);

        if (auditEvent != null) {
            populateDefaults(auditEvent);

            AuditHandler auditProvider = RangerBasePlugin.getAuditProvider(auditEvent.getRepositoryName());
            if (auditProvider == null || !auditProvider.log(auditEvent)) {
                MiscUtil.logErrorMessageByInterval(LOG, "fail to log audit event " + auditEvent);
            }
        }

        LOG.debug("<== RangerDefaultAuditHandler.logAuthzAudit({})", auditEvent);
    }

    public void logAuthzAudits(Collection<AuthzAuditEvent> auditEvents) {
        LOG.debug("==> RangerDefaultAuditHandler.logAuthzAudits({})", auditEvents);

        if (auditEvents != null) {
            for (AuthzAuditEvent auditEvent : auditEvents) {
                logAuthzAudit(auditEvent);
            }
        }

        LOG.debug("<== RangerDefaultAuditHandler.logAuthzAudits({})", auditEvents);
    }

    public AuthzAuditEvent createAuthzAuditEvent() {
        return new AuthzAuditEvent();
    }

    public final Set<String> getDatasets(RangerAccessRequest request) {
        GdsAccessResult gdsResult = RangerAccessRequestUtil.getGdsResultFromContext(request.getContext());

        return gdsResult != null ? gdsResult.getDatasets() : null;
    }

    public final Set<String> getProjects(RangerAccessRequest request) {
        GdsAccessResult gdsResult = RangerAccessRequestUtil.getGdsResultFromContext(request.getContext());

        return gdsResult != null ? gdsResult.getProjects() : null;
    }

    public String getAdditionalInfo(RangerAccessRequest request) {
        if (StringUtils.isBlank(request.getRemoteIPAddress()) && CollectionUtils.isEmpty(request.getForwardedAddresses())) {
            return null;
        }

        Map<String, String> addInfomap = new HashMap<>();
        addInfomap.put("forwarded-ip-addresses", "[" + StringUtils.join(request.getForwardedAddresses(), ", ") + "]");
        addInfomap.put("remote-ip-address", request.getRemoteIPAddress());

        return JsonUtils.mapToJson(addInfomap);
    }

    protected final Set<String> getTags(RangerAccessRequest request) {
        Set<String>           ret  = null;
        Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

        if (CollectionUtils.isNotEmpty(tags)) {
            ret = new HashSet<>();

            for (RangerTagForEval tag : tags) {
                ret.add(writeObjectAsString(tag));
            }
        }

        return ret;
    }

    private void populateDefaults(AuthzAuditEvent auditEvent) {
        if (auditEvent.getAclEnforcer() == null || auditEvent.getAclEnforcer().isEmpty()) {
            auditEvent.setAclEnforcer("ranger-acl"); // TODO: review
        }

        if (auditEvent.getAgentHostname() == null || auditEvent.getAgentHostname().isEmpty()) {
            auditEvent.setAgentHostname(MiscUtil.getHostname());
        }

        if (auditEvent.getLogType() == null || auditEvent.getLogType().isEmpty()) {
            auditEvent.setLogType("RangerAudit");
        }

        if (auditEvent.getEventId() == null || auditEvent.getEventId().isEmpty()) {
            auditEvent.setEventId(generateNextAuditEventId());
        }

        if (auditEvent.getAgentId() == null) {
            auditEvent.setAgentId(MiscUtil.getApplicationType());
        }

        auditEvent.setSeqNum(sequenceNumber++);
    }

    private String generateNextAuditEventId() {
        final String ret;

        if (auditIdStrictUUID) {
            ret = MiscUtil.generateGuid();
        } else {
            int nextId = counter.getAndIncrement();

            if (nextId == Integer.MAX_VALUE) {
                // reset UUID and counter
                uuid    = MiscUtil.generateUniqueId();
                counter = new AtomicInteger(0);
            }

            ret = uuid + "-" + nextId;
        }

        LOG.debug("generateNextAuditEventId(): {}", ret);

        return ret;
    }

    private String writeObjectAsString(Serializable obj) {
        String jsonStr = StringUtils.EMPTY;
        try {
            jsonStr = JsonUtilsV2.objToJson(obj);
        } catch (Exception e) {
            LOG.error("Cannot create JSON string for object:[{}]", obj, e);
        }
        return jsonStr;
    }
}
