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

package org.apache.ranger.authz.embedded;

import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.ArrayList;
import java.util.Collection;

public class RangerAuthzAuditHandler extends RangerDefaultAuditHandler implements AutoCloseable {
    private final RangerBasePlugin            plugin;
    private final Collection<AuthzAuditEvent> auditEvents = new ArrayList<>();
    private       boolean                     deniedExists;

    public RangerAuthzAuditHandler(RangerBasePlugin plugin) {
        super();

        this.plugin = plugin;
    }

    @Override
    public void processResult(RangerAccessResult result) {
        AuthzAuditEvent auditEvent = getAuthzEvents(result);

        // in case denied access, log only the first denied access; ignore all others
        if (auditEvent != null && !deniedExists) {
            auditEvent.setAgentId(plugin.getAppId());

            if (result.getIsAccessDetermined() && !result.getIsAllowed()) {
                deniedExists = true;

                auditEvents.clear();
            }

            auditEvents.add(auditEvent);
        }
    }

    @Override
    public void processResults(Collection<RangerAccessResult> results) {
        results.forEach(this::processResult);
    }

    @Override
    public void close() {
        auditEvents.forEach(super::logAuthzAudit);
    }
}
