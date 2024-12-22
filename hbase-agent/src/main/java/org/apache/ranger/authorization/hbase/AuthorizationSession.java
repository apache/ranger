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
package org.apache.ranger.authorization.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Set;

public class AuthorizationSession {
    private static final Logger LOG = LoggerFactory.getLogger(AuthorizationSession.class.getName());

    // collaborator objects
    final HbaseFactory   factory   = HbaseFactory.getInstance();
    final HbaseUserUtils userUtils = factory.getUserUtils();
    final HbaseAuthUtils authUtils = factory.getAuthUtils();

    // immutable state
    final RangerHBasePlugin authorizer;

    // Mutable state: Use supplied state information
    String operation;
    String otherInformation;
    String access;
    String table;
    String column;
    String columnFamily;
    String remoteAddress;

    User                user;
    Set<String>         groups;       // this exits to avoid having to get group for a user repeatedly.  It is kept in sync with _user;
    HbaseAuditHandler   auditHandler; // Passing a null handler to policy engine would suppress audit logging.
    boolean             superUser;    // is this session for a super user?
    RangerAccessRequest request;      // internal state per-authorization
    RangerAccessResult  result;

    private RangerAccessRequest.ResourceMatchingScope resourceMatchingScope = RangerAccessRequest.ResourceMatchingScope.SELF;
    private boolean                                   ignoreDescendantDeny  = true;

    public AuthorizationSession(RangerHBasePlugin authorizer) {
        this.authorizer = authorizer;
    }

    public boolean getPropertyIsColumnAuthOptimizationEnabled() {
        return authorizer.getPropertyIsColumnAuthOptimizationEnabled();
    }

    AuthorizationSession operation(String anOperation) {
        operation = anOperation;
        return this;
    }

    AuthorizationSession otherInformation(String information) {
        otherInformation = information;
        return this;
    }

    AuthorizationSession remoteAddress(String ipAddress) {
        remoteAddress = ipAddress;
        return this;
    }

    AuthorizationSession access(String anAccess) {
        access = anAccess;
        return this;
    }

    AuthorizationSession user(User aUser) {
        user = aUser;

        if (user == null) {
            LOG.warn("AuthorizationSession.user: user is null!");

            groups = null;
        } else {
            groups = userUtils.getUserGroups(user);

            if (groups.isEmpty() && user.getUGI() != null) {
                String[] groups = user.getUGI().getGroupNames();

                if (groups != null) {
                    this.groups = Sets.newHashSet(groups);
                }
            }

            superUser = userUtils.isSuperUser(user);
        }

        return this;
    }

    AuthorizationSession table(String aTable) {
        table = aTable;
        return this;
    }

    AuthorizationSession columnFamily(String aColumnFamily) {
        columnFamily = aColumnFamily;
        return this;
    }

    AuthorizationSession column(String aColumn) {
        column = aColumn;
        return this;
    }

    void verifyBuildable() {
        String template = "Internal error: Incomplete/inconsisten state: [%s]. Can't build auth request!";

        if (factory == null) {
            String message = String.format(template, "factory is null");

            LOG.error(message);

            throw new IllegalStateException(message);
        }

        if (access == null || access.isEmpty()) {
            String message = String.format(template, "access is null");

            LOG.error(message);

            throw new IllegalStateException(message);
        }

        if (user == null) {
            String message = String.format(template, "user is null");

            LOG.error(message);

            throw new IllegalStateException(message);
        }

        if (isProvided(columnFamily) && !isProvided(table)) {
            String message = String.format(template, "Table must be provided if column-family is provided");

            LOG.error(message);

            throw new IllegalStateException(message);
        }

        if (isProvided(column) && !isProvided(columnFamily)) {
            String message = String.format(template, "Column family must be provided if column is provided");

            LOG.error(message);

            throw new IllegalStateException(message);
        }
    }

    void zapAuthorizationState() {
        request = null;
        result  = null;
    }

    boolean isProvided(String aString) {
        return aString != null && !aString.isEmpty();
    }

    boolean isNameSpaceOperation() {
        return StringUtils.equals(operation, "createNamespace") ||
                StringUtils.equals(operation, "deleteNamespace") ||
                StringUtils.equals(operation, "modifyNamespace") ||
                StringUtils.equals(operation, "setUserNamespaceQuota") ||
                StringUtils.equals(operation, "setNamespaceQuota") ||
                StringUtils.equals(operation, "getUserPermissionForNamespace");
    }

    AuthorizationSession buildRequest() {
        verifyBuildable();

        // session can be reused so reset its state
        zapAuthorizationState();

        request = createRangerRequest();

        LOG.debug("Built request: {}", request);

        return this;
    }

    AuthorizationSession authorize() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AuthorizationSession.authorize: {}", getRequestMessage());
        }

        if (request == null) {
            String message = "Invalid state transition: buildRequest() must be called before authorize().  This request would ultimately get denied.!";

            throw new IllegalStateException(message);
        } else {
            // ok to pass potentially null handler to policy engine.  Null handler effectively suppresses the audit.
            if (auditHandler != null && superUser) {
                LOG.debug("Setting super-user override on audit handler");

                auditHandler.setSuperUserOverride(superUser);
            }

            result = authorizer.isAccessAllowed(request, auditHandler);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AuthorizationSession.authorize: {}", getLogMessage(isAuthorized(), getDenialReason()));
        }

        return this;
    }

    void logCapturedEvents() {
        if (auditHandler != null) {
            List<AuthzAuditEvent> events = auditHandler.getCapturedEvents();

            auditHandler.logAuthzAudits(events);
        }
    }

    void publishResults() throws AccessDeniedException {
        LOG.debug("==> AuthorizationSession.publishResults()");

        boolean authorized = isAuthorized();

        if (auditHandler != null && isAudited()) {
            List<AuthzAuditEvent> events = null;

            /*
             * What we log to audit depends on authorization status.  For success we log all accumulated events.  In case of failure
             * we log just the last set of audit messages as we only need to record the cause of overall denial.
             */
            if (authorized) {
                List<AuthzAuditEvent> theseEvents = auditHandler.getCapturedEvents();

                if (theseEvents != null && !theseEvents.isEmpty()) {
                    events = theseEvents;
                }
            } else {
                AuthzAuditEvent event = auditHandler.getAndDiscardMostRecentEvent();

                if (event != null) {
                    events = Lists.newArrayList(event);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Writing {} messages to audit: [{}]", (events == null ? 0 : events.size()), (events == null ? "" : events.toString()));
            }

            auditHandler.logAuthzAudits(events);
        }

        if (!authorized) {
            // and throw and exception... callers expect this behavior
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AuthorizationSession.publishResults: throwing exception: {}", getLogMessage(false, getDenialReason()));
            }

            throw new AccessDeniedException("Insufficient permissions for user '" + user.getName() + "' (action=" + access + ")");
        }

        LOG.debug("<== AuthorizationSession.publishResults()");
    }

    boolean isAudited() {
        boolean audited = false;

        if (result == null) {
            LOG.error("Internal error: _result was null!  Assuming no audit. Request[{}]", request);
        } else {
            audited = result.getIsAudited();
        }

        return audited;
    }

    boolean isAuthorized() {
        boolean allowed = false;
        if (result == null) {
            LOG.error("Internal error: _result was null! Returning false.");
        } else {
            allowed = result.getIsAllowed();
        }

        if (!allowed && superUser) {
            LOG.debug("User [{}] is a superUser!  Overriding policy engine's decision.  Request is deemed authorized!", user);

            allowed = true;
        }

        return allowed;
    }

    String getDenialReason() {
        String reason = "";
        if (result == null) {
            LOG.error("Internal error: _result was null!  Returning empty reason.");
        } else {
            boolean allowed = result.getIsAllowed();

            if (!allowed) {
                reason = result.getReason();
            }
        }

        return reason;
    }

    String requestToString() {
        return toString();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(request.getClass())
                .add("operation", operation)
                .add("otherInformation", otherInformation)
                .add("access", access)
                .add("user", user != null ? user.getName() : null)
                .add("groups", groups)
                .add("auditHandler", auditHandler != null ? auditHandler.getClass().getSimpleName() : null)
                .add(RangerHBaseResource.KEY_TABLE, table)
                .add(RangerHBaseResource.KEY_COLUMN, column)
                .add(RangerHBaseResource.KEY_COLUMN_FAMILY, columnFamily)
                .add("resource-matching-scope", resourceMatchingScope)
                .add("ignoreDescendantDeny", ignoreDescendantDeny)
                .toString();
    }

    String getPrintableValue(String value) {
        if (isProvided(value)) {
            return value;
        } else {
            return "";
        }
    }

    String getRequestMessage() {
        String format = "Access[%s] by user[%s] belonging to groups[%s] to table[%s] for column-family[%s], column[%s] triggered by operation[%s], otherInformation[%s]";
        String user   = userUtils.getUserAsString();
        return String.format(format, getPrintableValue(access), getPrintableValue(user), groups, getPrintableValue(table),
                getPrintableValue(columnFamily), getPrintableValue(column), getPrintableValue(operation), getPrintableValue(otherInformation));
    }

    String getLogMessage(boolean allowed, String reason) {
        String format  = " %s: status[%s], reason[%s]";
        return String.format(format, getRequestMessage(), allowed ? "allowed" : "denied", reason);
    }

    /**
     * This method could potentially null out an earlier audit handler -- which effectively would suppress audits.
     */
    AuthorizationSession auditHandler(HbaseAuditHandler anAuditHandler) {
        auditHandler = anAuditHandler;
        return this;
    }

    AuthorizationSession resourceMatchingScope(RangerAccessRequest.ResourceMatchingScope scope) {
        resourceMatchingScope = scope;
        return this;
    }

    AuthorizationSession ignoreDescendantDeny(boolean ignoreDescendantDeny) {
        this.ignoreDescendantDeny = ignoreDescendantDeny;
        return this;
    }

    private RangerAccessResource createHBaseResource() {
        // TODO get this via a factory instead
        RangerAccessResourceImpl resource = new RangerHBaseResource();

        // policy engine should deal sensibly with null/empty values, if any
        if (isNameSpaceOperation() && StringUtils.isNotBlank(otherInformation)) {
            resource.setValue(RangerHBaseResource.KEY_TABLE, otherInformation + RangerHBaseResource.NAMESPACE_SEPARATOR);
        } else {
            resource.setValue(RangerHBaseResource.KEY_TABLE, table);
        }

        resource.setValue(RangerHBaseResource.KEY_COLUMN_FAMILY, columnFamily);
        resource.setValue(RangerHBaseResource.KEY_COLUMN, column);

        return resource;
    }

    private RangerAccessRequest createRangerRequest() {
        RangerAccessResource    resource = createHBaseResource();
        String                  user     = userUtils.getUserAsString(this.user);
        RangerAccessRequestImpl request  = new RangerAccessRequestImpl(resource, access, user, groups, null);

        request.setAction(operation);
        request.setRequestData(otherInformation);
        request.setClientIPAddress(remoteAddress);
        request.setResourceMatchingScope(resourceMatchingScope);
        request.setAccessTime(new Date());
        request.setIgnoreDescendantDeny(ignoreDescendantDeny);

        return request;
    }
}
