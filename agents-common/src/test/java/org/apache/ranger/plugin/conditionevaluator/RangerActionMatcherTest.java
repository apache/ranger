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

package org.apache.ranger.plugin.conditionevaluator;

import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RangerActionMatcherTest {
    @Test
    void testNullOrEmptyConditionMatchesAll() {
        final RangerActionMatcher matcherNull = createMatcher(null);

        assertTrue(matcherNull.isMatched(createRequest("PutObject")));
        assertTrue(matcherNull.isMatched(createRequest(null)));

        final RangerActionMatcher matcherEmpty = createMatcher(new String[] {});

        assertTrue(matcherEmpty.isMatched(createRequest("PutObject")));
        assertTrue(matcherEmpty.isMatched(createRequest(null)));
    }

    @Test
    void testWildcardMatchesAll() {
        final RangerActionMatcher matcher = createMatcher(new String[] {"*"});

        assertTrue(matcher.isMatched(createRequest("GetObject")));
        assertTrue(matcher.isMatched(createRequest("PutObject")));
        assertTrue(matcher.isMatched(createRequest(null)));
    }

    @Test
    void testNoRequestActionDoesNotEnforce() {
        final RangerActionMatcher matcher = createMatcher(new String[] {"PutObject"});

        assertTrue(matcher.isMatched(createRequest(null)));
        assertTrue(matcher.isMatched(createRequest("")));
        assertTrue(matcher.isMatched(createRequest("   ")));
    }

    @Test
    void testExactMatchCaseInsensitive() {
        final RangerActionMatcher matcher = createMatcher(new String[] {"GetObject"});

        assertTrue(matcher.isMatched(createRequest("GetObject")));
        assertTrue(matcher.isMatched(createRequest("getobject")));
        assertFalse(matcher.isMatched(createRequest("PutObject")));
    }

    @Test
    void testTrailingWildcardPrefixMatchCaseInsensitive() {
        final RangerActionMatcher matcher = createMatcher(new String[] {"Put*"});

        assertTrue(matcher.isMatched(createRequest("PutObject")));
        assertTrue(matcher.isMatched(createRequest("putobjecttagging")));
        assertFalse(matcher.isMatched(createRequest("GetObject")));
    }

    private RangerActionMatcher createMatcher(final String[] actionsArray) {
        final RangerActionMatcher matcher = new RangerActionMatcher();

        if (actionsArray == null) {
            matcher.setConditionDef(null);
            matcher.setPolicyItemCondition(null);
        } else {
            final RangerPolicyItemCondition condition = mock(RangerPolicyItemCondition.class);
            final List<String>              actions   = Arrays.asList(actionsArray);

            when(condition.getValues()).thenReturn(actions);
            matcher.setConditionDef(null);
            matcher.setPolicyItemCondition(condition);
        }

        matcher.init();

        return matcher;
    }

    private RangerAccessRequest createRequest(final String action) {
        final RangerAccessRequest request = mock(RangerAccessRequest.class);

        when(request.getAction()).thenReturn(action);

        return request;
    }
}
