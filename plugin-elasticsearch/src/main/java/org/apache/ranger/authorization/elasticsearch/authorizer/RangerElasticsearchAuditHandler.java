/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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
package org.apache.ranger.authorization.elasticsearch.authorizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class RangerElasticsearchAuditHandler extends RangerDefaultAuditHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RangerElasticsearchAuditHandler.class);

    private static final String PROP_ES_PLUGIN_AUDIT_EXCLUDED_USERS = "ranger.elasticsearch.plugin.audit.excluded.users";
    private static final String PROP_ES_PLUGIN_AUDIT_INDEX          = "xasecure.audit.destination.elasticsearch.index";
    private static final String PROP_AUDITSERVER_URL                = "xasecure.audit.destination.auditserver.url";

    private final String       indexName;
    private final List<String> excludeUsers;
    private final String       auditServerUrl;

    public RangerElasticsearchAuditHandler(Configuration config) {
        super(config);

        String esUser          = "elasticsearch";
        String excludeUserList = config.get(PROP_ES_PLUGIN_AUDIT_EXCLUDED_USERS, esUser);

        excludeUsers    = Arrays.asList(excludeUserList.split(","));
        indexName       = config.get(PROP_ES_PLUGIN_AUDIT_INDEX, "ranger_audits");
        auditServerUrl  = config.get(PROP_AUDITSERVER_URL);

        ElasticsearchAuditIngestorClient.init(config);
    }

    @Override
    public void processResult(RangerAccessResult result) {
        // We don't audit "allowed" operation for user "elasticsearch" on index "ranger_audits" to avoid recursive
        // logging due to updated of ranger_audits index by elasticsearch plugin's audit creation.
        if (!isAuditingNeeded(result)) {
            return;
        }

        AuthzAuditEvent auditEvent = super.getAuthzEvents(result);

        if (auditEvent == null) {
            return;
        }

        if (!logAuditEvent(auditEvent)) {
            MiscUtil.logErrorMessageByInterval(LOG, "fail to log audit event " + auditEvent);
        }
    }

    private boolean logAuditEvent(AuthzAuditEvent auditEvent) {
        if (StringUtils.isNotBlank(auditServerUrl) && ElasticsearchAuditIngestorClient.post(auditServerUrl, auditEvent)) {
            return true;
        }

        if (AuditProviderFactory.getInstance().logOnRequestThread(auditEvent)) {
            return true;
        }

        super.logAuthzAudit(auditEvent);

        return true;
    }

    private boolean isAuditingNeeded(final RangerAccessResult result) {
        if (result == null) {
            return false;
        }

        RangerAccessRequest request = result.getAccessRequest();

        if (request == null) {
            return false;
        }

        boolean                  ret          = true;
        boolean                  isAllowed    = result.getIsAllowed();
        RangerAccessResourceImpl resource     = (RangerAccessResourceImpl) request.getResource();
        String                   resourceName = resource == null ? null : (String) resource.getValue("index");
        String                   requestUser  = request.getUser();

        if (resourceName != null && resourceName.equals(indexName) && excludeUsers.contains(requestUser) && isAllowed) {
            ret = false;
        }

        return ret;
    }
}
