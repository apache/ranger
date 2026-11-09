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

package org.apache.ranger.authz.model;

import org.apache.ranger.authz.model.RangerAuthzResult.AccessDecision;
import org.apache.ranger.authz.model.RangerAuthzResult.AccessResult;
import org.apache.ranger.authz.model.RangerAuthzResult.ConstraintResult;
import org.apache.ranger.authz.model.RangerAuthzResult.PermissionResult;
import org.apache.ranger.authz.model.RangerAuthzResult.PolicyInfo;
import org.apache.ranger.authz.model.RangerAuthzResult.ResultInfo;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRangerAuthzResult {
    @Test
    public void testPermissionResultCopiesConstraintsFromResultInfo() {
        List<ConstraintResult> constraints = Collections.singletonList(requireApproval(21L));
        ResultInfo             resultInfo  = new ResultInfo(new AccessResult(AccessDecision.ALLOW, new PolicyInfo(12L, 3L)), null, null, null);

        resultInfo.setConstraints(constraints);

        PermissionResult permResult = new PermissionResult("execute", resultInfo);

        assertEquals(constraints, permResult.getConstraints());
        assertEquals(AccessDecision.ALLOW, permResult.getAccess().getDecision());
    }

    @Test
    public void testConstraintsInEqualsAndHashCode() {
        ResultInfo withConstraint    = new ResultInfo(new AccessResult(AccessDecision.ALLOW, null), null, null, null);
        ResultInfo withoutConstraint = new ResultInfo(new AccessResult(AccessDecision.ALLOW, null), null, null, null);

        withConstraint.setConstraints(Collections.singletonList(requireApproval(21L)));

        assertNotEquals(withConstraint, withoutConstraint);

        withoutConstraint.setConstraints(Collections.singletonList(requireApproval(21L)));

        assertEquals(withConstraint, withoutConstraint);
        assertEquals(withConstraint.hashCode(), withoutConstraint.hashCode());

        PermissionResult perm1 = new PermissionResult("execute", withConstraint);
        PermissionResult perm2 = new PermissionResult("execute", withoutConstraint);

        assertEquals(perm1, perm2);

        perm2.setConstraints(Collections.singletonList(requireApproval(22L)));

        assertNotEquals(perm1, perm2);
    }

    @Test
    public void testConstraintResult() {
        ConstraintResult constraint = requireApproval(21L);

        assertEquals("requireApproval", constraint.getName());
        assertEquals("finance-approvers", constraint.getParameters().get("approver"));
        assertEquals(new PolicyInfo(21L, 1L), constraint.getPolicy());
        assertEquals(Integer.valueOf(0), constraint.getPolicyPriority());
        assertEquals(requireApproval(21L), constraint);
        assertNotEquals(requireApproval(22L), constraint);
        assertTrue(constraint.toString().contains("requireApproval"));
    }

    private static ConstraintResult requireApproval(Long policyId) {
        return new ConstraintResult("requireApproval", Collections.singletonMap("approver", "finance-approvers"), new PolicyInfo(policyId, 1L), 0);
    }
}
