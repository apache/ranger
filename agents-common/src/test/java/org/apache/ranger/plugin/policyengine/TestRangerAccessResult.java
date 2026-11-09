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
package org.apache.ranger.plugin.policyengine;

import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher.MatchType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRangerAccessResult {
    @Test
    public void constraintsAreEmptyUntilOneIsAdded() {
        RangerAccessResult result = newResult();

        assertTrue(result.getConstraints().isEmpty());

        result.addConstraint(null);
        assertTrue(result.getConstraints().isEmpty());

        RangerConstraintResult constraint = constraint(10L);

        result.addConstraint(constraint);

        assertEquals(Arrays.asList(constraint), result.getConstraints());
    }

    @Test
    public void setConstraintsCopiesTheInputList() {
        RangerAccessResult result = newResult();
        List<RangerConstraintResult> constraints = new ArrayList<>();
        RangerConstraintResult constraint = constraint(10L);

        constraints.add(constraint);
        result.setConstraints(constraints);
        constraints.clear();

        assertEquals(Arrays.asList(constraint), result.getConstraints());

        result.setConstraints(new ArrayList<>());

        assertTrue(result.getConstraints().isEmpty());
    }

    @Test
    public void setAccessResultFromCopiesConstraints() {
        RangerAccessResult source = newResult();
        RangerAccessResult copied = newResult();
        RangerConstraintResult constraint = constraint(10L);

        source.addConstraint(constraint);
        copied.setAccessResultFrom(source);

        assertEquals(Arrays.asList(constraint), copied.getConstraints());
    }

    @Test
    public void denyClearsConstraints() {
        RangerAccessResult result = newResult();

        result.addConstraint(constraint(10L));
        result.setIsAllowed(false);

        assertTrue(result.getConstraints().isEmpty());

        result.addConstraint(constraint(11L));
        result.setIsAllowed(true);

        assertEquals(1, result.getConstraints().size());
    }

    private static RangerAccessResult newResult() {
        return new RangerAccessResult(0, "agent-studio", null, null);
    }

    private static RangerConstraintResult constraint(long policyId) {
        return new RangerConstraintResult("requireApproval", null, policyId, 1L, 0, MatchType.SELF);
    }
}
