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

package org.apache.ranger.authorization.hadoop;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.ACCESS_TYPE_MONITOR_HEALTH;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.ALL_PERM;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.READ_ACCCESS_TYPE;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.READ_EXECUTE_PERM;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.READ_WRITE_PERM;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.WRITE_ACCCESS_TYPE;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.WRITE_EXECUTE_PERM;

class RangerHdfsAuditHandler extends RangerDefaultAuditHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RangerHdfsAuditHandler.class);

    private final String      pathToBeValidated;
    private final boolean     auditOnlyIfDenied;
    private final String      hadoopModuleName;
    private final Set<String> excludeUsers;
    private final String      callerContext;

    private boolean         isAuditEnabled;
    private AuthzAuditEvent auditEvent;

    public RangerHdfsAuditHandler(String pathToBeValidated, boolean auditOnlyIfDenied, String hadoopModuleName, Set<String> excludedUsers, String callerContext) {
        this.pathToBeValidated = pathToBeValidated;
        this.auditOnlyIfDenied = auditOnlyIfDenied;
        this.hadoopModuleName  = hadoopModuleName;
        this.excludeUsers      = excludedUsers;
        this.callerContext     = callerContext;
    }

    @Override
    public void processResult(RangerAccessResult result) {
        LOG.debug("==> RangerHdfsAuditHandler.logAudit({})", result);

        if (result != null) {
            isAuditEnabled = result.getIsAudited();

            if (auditEvent == null) {
                auditEvent = super.getAuthzEvents(result);
            }

            if (auditEvent != null) {
                RangerAccessRequest  request      = result.getAccessRequest();
                RangerAccessResource resource     = request.getResource();
                String               resourcePath = resource != null ? resource.getAsString() : null;

                // Overwrite fields in original auditEvent
                auditEvent.setEventTime(request.getAccessTime() != null ? request.getAccessTime() : new Date());
                auditEvent.setAccessType(request.getAction());
                auditEvent.setResourcePath(this.pathToBeValidated);
                auditEvent.setResultReason(resourcePath);

                auditEvent.setAccessResult((short) (result.getIsAllowed() ? 1 : 0));
                auditEvent.setPolicyId(result.getPolicyId());
                auditEvent.setPolicyVersion(result.getPolicyVersion());

                setRequestData();

                auditEvent.setAction(getAccessType(request.getAccessType()));
                auditEvent.setAdditionalInfo(getAdditionalInfo(request));

                Set<String> tags = getTags(request);

                if (tags != null) {
                    auditEvent.setTags(tags);
                }
            }
        }

        LOG.debug("<== RangerHdfsAuditHandler.logAudit({}): {}", result, auditEvent);
    }

    @Override
    public String getAdditionalInfo(RangerAccessRequest request) {
        String              additionalInfo = super.getAdditionalInfo(request);
        Map<String, String> addInfoMap     = JsonUtils.jsonToMapStringString(additionalInfo);

        if (addInfoMap == null || addInfoMap.isEmpty()) {
            addInfoMap = new HashMap<>();
        }

        String accessTypes = getAccessTypesAsString(request);

        if (accessTypes != null) {
            addInfoMap.put("accessTypes", "[" + accessTypes + "]");
        }

        return JsonUtils.mapToJson(addInfoMap);
    }

    public void logHadoopEvent(String path, FsAction action, boolean accessGranted) {
        LOG.debug("==> RangerHdfsAuditHandler.logHadoopEvent({}, {}, {})", path, action, accessGranted);

        if (auditEvent != null) {
            auditEvent.setResultReason(path);
            auditEvent.setAccessResult((short) (accessGranted ? 1 : 0));
            auditEvent.setAclEnforcer(hadoopModuleName);
            auditEvent.setPolicyId(-1);

            String accessType = (action == null) ? null : action.toString();

            if (StringUtils.isBlank(auditEvent.getAccessType())) { // retain existing value
                auditEvent.setAccessType(accessType);
            }

            if (accessType != null) {
                auditEvent.setAction(getAccessType(accessType));
            }

            setRequestData();
        }

        LOG.debug("<== RangerHdfsAuditHandler.logHadoopEvent({}, {}, {}): {}", path, action, accessGranted, auditEvent);
    }

    public void flushAudit() {
        LOG.debug("==> RangerHdfsAuditHandler.flushAudit({}, {})", isAuditEnabled, auditEvent);

        if (isAuditEnabled && auditEvent != null && !StringUtils.isEmpty(auditEvent.getAccessType())) {
            String username   = auditEvent.getUser();
            String accessType = auditEvent.getAccessType();

            boolean skipLog = (username != null && excludeUsers != null && excludeUsers.contains(username))
                    || (auditOnlyIfDenied && auditEvent.getAccessResult() != 0)
                    || (ACCESS_TYPE_MONITOR_HEALTH.equals(accessType));

            if (!skipLog) {
                super.logAuthzAudit(auditEvent);
            }
        }

        LOG.debug("<== RangerHdfsAuditHandler.flushAudit({}, {})", isAuditEnabled, auditEvent);
    }

    private String getAccessType(String accessType) {
        String ret = accessType;

        switch (accessType) {
            case READ_EXECUTE_PERM:
                ret = READ_ACCCESS_TYPE;
                break;
            case WRITE_EXECUTE_PERM:
            case READ_WRITE_PERM:
            case ALL_PERM:
                ret = WRITE_ACCCESS_TYPE;
                break;
            default:
                break;
        }

        return ret.toLowerCase();
    }

    private String getAccessTypesAsString(RangerAccessRequest request) {
        String      ret         = null;
        Set<String> accessTypes = RangerAccessRequestUtil.getAllRequestedAccessTypes(request);

        if (CollectionUtils.isNotEmpty(accessTypes)) {
            try {
                ret = getFormattedAccessType(accessTypes);
            } catch (Throwable t) {
                LOG.error("getAccessTypesAsString(): failed to get accessTypes from context", t);
            }
        } else {
            ret = request.getAccessType();
        }
        return ret;
    }

    private String getFormattedAccessType(Set<String> accessTypes) {
        String ret = null;

        if (CollectionUtils.isNotEmpty(accessTypes)) {
            ret = String.join(", ", accessTypes);
        }

        return ret;
    }

    private void setRequestData() {
        if (StringUtils.isNotBlank(auditEvent.getAccessType()) && StringUtils.isNotBlank(callerContext)) {
            auditEvent.setRequestData(auditEvent.getAccessType() + "/" + callerContext);
        }
    }
}
