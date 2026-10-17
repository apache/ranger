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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRangerConstraintResult {
    @Test
    public void nullParametersBecomeAnEmptyMap() {
        RangerConstraintResult result = result("requireApproval", null, 10L, 3L, 0, MatchType.SELF);

        assertNotNull(result.getParameters());
        assertTrue(result.getParameters().isEmpty());
    }

    @Test
    public void parametersAreCopiedFromTheInputMap() {
        Map<String, Object> parameters = new HashMap<>();

        parameters.put("approver", "alice");

        RangerConstraintResult result = result("requireApproval", parameters, 10L, 3L, 0, MatchType.SELF);

        parameters.put("approver", "bob");
        parameters.put("extra", "x");

        assertEquals("alice", result.getParameters().get("approver"));
        assertFalse(result.getParameters().containsKey("extra"));
        assertEquals(1, result.getParameters().size());
    }

    @Test
    public void equalsAndHashCodeIncludePolicyPriority() {
        RangerConstraintResult result = result("requireApproval", mapOf("approver", "alice"), 10L, 3L, 0, MatchType.SELF);
        RangerConstraintResult same   = result("requireApproval", mapOf("approver", "alice"), 10L, 3L, 0, MatchType.SELF);
        RangerConstraintResult higher = result("requireApproval", mapOf("approver", "alice"), 10L, 3L, 1, MatchType.SELF);

        assertEquals(result, same);
        assertEquals(result.hashCode(), same.hashCode());
        assertNotEquals(result, higher);
        assertTrue(result.toString().contains("policyPriority=0"));
        assertTrue(result.toString().contains("requireApproval"));
    }

    @Test
    public void comparatorOrdersByNamePriorityMatchTypeAndPolicyId() {
        RangerConstraintResult maxRows          = result("maxRows", null, 20L, 1L, 1, MatchType.DESCENDANT);
        RangerConstraintResult lowPriority      = result("requireApproval", null, 30L, 1L, 0, MatchType.DESCENDANT);
        RangerConstraintResult selfMatch        = result("requireApproval", null, 40L, 1L, 1, MatchType.SELF);
        RangerConstraintResult laterPolicy      = result("requireApproval", null, 12L, 1L, 1, MatchType.DESCENDANT);
        RangerConstraintResult earlierPolicy    = result("requireApproval", null, 11L, 1L, 1, MatchType.DESCENDANT);
        List<RangerConstraintResult> constraints = Arrays.asList(laterPolicy, selfMatch, maxRows, lowPriority, earlierPolicy);

        constraints.sort(RangerConstraintResult.CONSTRAINT_RESULT_COMPARATOR);

        assertEquals(Arrays.asList(maxRows, selfMatch, earlierPolicy, laterPolicy, lowPriority), constraints);
    }

    private static RangerConstraintResult result(String name, Map<String, Object> parameters, long policyId, Long policyVersion, int policyPriority, MatchType matchType) {
        return new RangerConstraintResult(name, parameters, policyId, policyVersion, policyPriority, matchType);
    }

    private static Map<String, Object> mapOf(String key, String value) {
        Map<String, Object> ret = new HashMap<>();

        ret.put(key, value);

        return ret;
    }
}
