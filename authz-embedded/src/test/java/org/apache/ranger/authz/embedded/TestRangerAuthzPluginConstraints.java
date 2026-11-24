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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerAuthzResult.AccessDecision;
import org.apache.ranger.authz.model.RangerAuthzResult.AccessResult;
import org.apache.ranger.authz.model.RangerAuthzResult.ConstraintResult;
import org.apache.ranger.authz.model.RangerAuthzResult.PermissionResult;
import org.apache.ranger.authz.model.RangerAuthzResult.PolicyInfo;
import org.apache.ranger.authz.model.RangerAuthzResult.ResultInfo;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerConstraintResult;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher.MatchType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestRangerAuthzPluginConstraints {
    @Test
    public void testAllowedResultReturnsSortedConstraints() {
        RangerAccessResult result = accessResult(true);

        result.addConstraint(constraint("requireApproval", 30L, RangerPolicy.POLICY_PRIORITY_NORMAL));
        result.addConstraint(constraint("maxRows", 20L, RangerPolicy.POLICY_PRIORITY_NORMAL));
        result.addConstraint(constraint("requireApproval", 40L, RangerPolicy.POLICY_PRIORITY_OVERRIDE));

        List<ConstraintResult> constraints = RangerAuthzPlugin.toConstraintResults(result);

        assertEquals(3, constraints.size());

        // name, then policy priority (OVERRIDE first), then policy id
        assertEquals("maxRows", constraints.get(0).getName());
        assertEquals(new PolicyInfo(40L, 2L), constraints.get(1).getPolicy());
        assertEquals(Integer.valueOf(RangerPolicy.POLICY_PRIORITY_OVERRIDE), constraints.get(1).getPolicyPriority());
        assertEquals(new PolicyInfo(30L, 2L), constraints.get(2).getPolicy());
        assertEquals("finance-approvers", constraints.get(2).getParameters().get("approver"));
    }

    @Test
    public void testNoConstraintsWhenNoneMatched() {
        assertNull(RangerAuthzPlugin.toConstraintResults(accessResult(true)));
    }

    @Test
    public void testNoConstraintsWhenDeniedOrNotDetermined() {
        RangerAccessResult denied = accessResult(true);

        denied.addConstraint(constraint("requireApproval", 30L, RangerPolicy.POLICY_PRIORITY_NORMAL));
        denied.setIsAllowed(false);

        assertNull(RangerAuthzPlugin.toConstraintResults(denied));

        RangerAccessResult notDetermined = new RangerAccessResult(RangerPolicy.POLICY_TYPE_ACCESS, "agent", null, null);

        notDetermined.addConstraint(constraint("requireApproval", 30L, RangerPolicy.POLICY_PRIORITY_NORMAL));

        assertNull(RangerAuthzPlugin.toConstraintResults(notDetermined));
        assertNull(RangerAuthzPlugin.toConstraintResults(null));
    }

    @Test
    public void testParametersAreCopied() {
        RangerAccessResult result = accessResult(true);

        result.addConstraint(constraint("requireApproval", 30L, RangerPolicy.POLICY_PRIORITY_NORMAL));

        ConstraintResult constraint = RangerAuthzPlugin.toConstraintResults(result).get(0);

        constraint.getParameters().put("approver", "changed");

        assertEquals("finance-approvers", result.getConstraints().get(0).getParameters().get("approver"));
    }

    @Test
    public void testJsonRoundTrip() throws Exception {
        ObjectMapper mapper     = new ObjectMapper();
        ResultInfo   resultInfo = new ResultInfo(new AccessResult(AccessDecision.ALLOW, new PolicyInfo(12L, 3L)), null, null, null);

        resultInfo.setConstraints(Collections.singletonList(new ConstraintResult("requireApproval", Collections.singletonMap("approver", "finance-approvers"), new PolicyInfo(21L, 1L), 0)));

        Map<String, PermissionResult> permissions = new HashMap<>();

        permissions.put("execute", new PermissionResult("execute", resultInfo));

        RangerAuthzResult authzResult = new RangerAuthzResult("req-1", AccessDecision.ALLOW, permissions);
        String            json        = mapper.writeValueAsString(authzResult);

        assertEquals(authzResult, mapper.readValue(json, RangerAuthzResult.class));

        RangerAuthzResult noConstraints = new RangerAuthzResult("req-2", AccessDecision.ALLOW, Collections.singletonMap("execute",
                new PermissionResult("execute", new ResultInfo(new AccessResult(AccessDecision.ALLOW, null), null, null, null))));

        assertFalse(mapper.writeValueAsString(noConstraints).contains("constraints"));
    }

    private static RangerAccessResult accessResult(boolean isAllowed) {
        RangerAccessResult ret = new RangerAccessResult(RangerPolicy.POLICY_TYPE_ACCESS, "agent", null, null);

        ret.setIsAccessDetermined(true);
        ret.setIsAllowed(isAllowed);

        return ret;
    }

    private static RangerConstraintResult constraint(String name, long policyId, int policyPriority) {
        Map<String, Object> parameters = new HashMap<>();

        parameters.put("approver", "finance-approvers");

        return new RangerConstraintResult(name, parameters, policyId, 2L, policyPriority, MatchType.SELF);
    }
}
