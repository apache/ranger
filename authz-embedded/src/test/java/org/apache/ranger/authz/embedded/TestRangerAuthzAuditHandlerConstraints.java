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
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerConstraintResult;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher.MatchType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRangerAuthzAuditHandlerConstraints {
    @Test
    public void testConstraintAuditEventsAreCollected() {
        List<AuthzAuditEvent> events;
        try (RangerAuthzAuditHandler handler = new RangerAuthzAuditHandler("test-app")) {
            RangerAccessResult result = result(true);
            result.addConstraint(new RangerConstraintResult("requireApproval", Collections.singletonMap("approver", "finance"), 21L, 1L, 0, MatchType.SELF));
            handler.processResult(result);
            events = new ArrayList<>(handler.getAuditEvents());
        }

        assertEquals(2, events.size());
        assertEquals("select", events.get(0).getAction());
        assertEquals("CONSTRAINT:requireApproval", events.get(1).getAccessType());
        assertEquals(21L, events.get(1).getPolicyId());
        assertEquals("test-app", events.get(1).getAgentId());
    }

    @Test
    public void testDeniedResultReplacesConstraintAuditEvents() {
        try (RangerAuthzAuditHandler handler = new RangerAuthzAuditHandler("test-app")) {
            RangerAccessResult allowed = result(true);
            allowed.addConstraint(new RangerConstraintResult("requireApproval", null, 21L, 1L, 0, MatchType.SELF));
            handler.processResult(allowed);
            handler.processResult(result(false));
            // only the denied audit is kept, as for other audits
            assertEquals(1, handler.getAuditEvents().size());
            assertEquals((short) 0, handler.getAuditEvents().iterator().next().getAccessResult());
        }
    }

    private static RangerAccessResult result(boolean isAllowed) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();

        request.setUser("alice");
        request.setAccessType("select");
        request.setAction("select");

        RangerAccessResult ret = new RangerAccessResult(0, "svc", null, request);

        ret.setIsAudited(true);
        ret.setIsAccessDetermined(true);
        ret.setIsAllowed(isAllowed);
        ret.setPolicyId(10L);

        return ret;
    }
}
