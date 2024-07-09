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

package org.apache.ranger.plugin.resourcematcher;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceElementMatchingScope;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceElementMatchType;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceElementMatchType.*;

public class RangerDefaultResourceMatcherTest {

    Object[][] data = {
         // { resource, policy, excludes, matchType, result, user }
            { "*",  "*",  false, SELF, true,  "user" },  // resource is all values
            { "*",  "*",  true,  NONE, false, "user" },
            { "*",  "a*", false, NONE, false, "user" }, // but, policy is not match any
            { "*",  "a*", true,  NONE, false, "user" }, // ==> compare with above: exclude flag has no effect here
            { "a*", "a",  false, NONE, false, "user" }, // resource has regex marker!
            { "a*", "a",  true,  SELF, true,  "user" },
            { "a",  "a",  false, SELF, true,  "user" },  // exact match
            { "a",  "a",  true,  NONE, false, "user" },
            { "a1", "a*", false, SELF, true,  "user" },  // trivial regex match
            { "a1", "a*", true,  NONE, false, "user" },

            // matchScope=SELF, excludes=false
            { "*",   "*",    false, SELF, true,  "user" }, // resource is all values
            { "a*",  "*",    false, SELF, true,  "user" }, // resource has regex, policy matches
            { "a*",  "a",    false, NONE, false, "user" }, // resource has regex, policy does not match
            { "*",   "a*",   false, NONE, false, "user" }, // resource has regex, policy does not match
            { "a*",  "a*",   false, SELF, true,  "user" }, // resource has regex, policy matches
            { "a?",  "a*",   false, SELF, true,  "user" }, // resource has regex, policy matches
            { "a*b", "a*",   false, SELF, true,  "user" }, // resource has regex, policy matches
            { "a?b", "a*",   false, SELF, true,  "user" }, // resource has regex, policy matches
            { "a*b", "a*b",  false, SELF, true,  "user" }, // resource has regex, policy matches
            { "a?b", "a?b",  false, SELF, true,  "user" }, // resource has regex, policy matches
            { "a1b", "a1b",  false, SELF, true,  "user" }, // exact match
            { "a1b", "a*",   false, SELF, true,  "user" }, // regex match - suffix
            { "a1b", "*b",   false, SELF, true,  "user" }, // regex match - prefix
            { "a1b", "*1*",  false, SELF, true,  "user" }, // regex match
            { "a1b", "a?b",  false, SELF, true,  "user" }, // regex match - single char
            { "a",   "abc",  false, NONE, false, "user" }, // policy has more than resource
            { "ab",  "abc",  false, NONE, false, "user" }, // policy has more than resource
            { "ab",  "*c",   false, NONE, false, "user" }, // policy has more than resource
            { "*b",  "a*bc", false, NONE, false, "user" }, // policy has more than resource
            { "a*b", "a*bc", false, NONE, false, "user" }, // policy has more than resource

            // matchScope=SELF, excludes=true
            { "*",   "*",    true, NONE, false, "user" },
            { "a*",  "*",    true, NONE, false, "user" },
            { "a*",  "a",    true, SELF, true,  "user" },
            { "*",   "a*",   true, NONE, false, "user" },  // ==> compare with above: exclude flag has no effect here
            { "a*",  "a*",   true, NONE, false, "user" },
            { "a?",  "a*",   true, NONE, false, "user" },
            { "a*b", "a*",   true, NONE, false, "user" },
            { "a?b", "a*",   true, NONE, false, "user" },
            { "a*b", "a*b",  true, NONE, false, "user" },
            { "a?b", "a?b",  true, NONE, false, "user" },
            { "a1b", "a1b",  true, NONE, false, "user" },
            { "a1b", "a*",   true, NONE, false, "user" },
            { "a1b", "*b",   true, NONE, false, "user" },
            { "a1b", "*1*",  true, NONE, false, "user" },
            { "a1b", "a?b",  true, NONE, false, "user" },
            { "a",   "abc",  true, SELF, true,  "user" },
            { "ab",  "abc",  true, SELF, true,  "user" },
            { "ab",  "*c",   true, SELF, true,  "user" },
            { "*b",  "a*bc", true, SELF, true,  "user" },
            { "a*b", "a*bc", true, SELF, true,  "user" },
    };

    Object[][] dataForPrefixMatch = {
            // { resource, policy, excludes, matchType, result, user }
            { "a",    "abc",    false, PREFIX, true,  "user" },
            { "ab",   "abc",    false, PREFIX, true,  "user" },
            { "ab",   "*c",     false, PREFIX, true,  "user" },
            { "a",    "a*c",    false, PREFIX, true,  "user" },
            { "ab",   "a*c",    false, PREFIX, true,  "user" },
            { "abc",  "a*c",    false, SELF,   true,  "user" },
            { "abcd", "a*c",    false, PREFIX, true,  "user" },
            { "abcd", "a*c*d",  false, SELF,   true,  "user" },
            { "abcd", "a*c*de", false, PREFIX, true,  "user" },
            { "acbd", "ab*c",   false, NONE,   false, "user" },
            { "b",    "ab*c",   false, NONE,   false, "user" },
            { "a",    "ab*",    false, PREFIX, true,  "user" },
            { "b",    "ab*",    false, NONE,   false, "user" },
    };

    @Test
    public void testIsMatch() throws Exception {
        ResourceElementMatchingScope matchScope = ResourceElementMatchingScope.SELF;

        for (Object[] row : data) {
            String                   resource    = (String)row[0];
            String                   policyValue = (String)row[1];
            boolean                  excludes    = (boolean)row[2];
            ResourceElementMatchType matchType   = (ResourceElementMatchType) row[3];
            boolean                  result      = (boolean)row[4];
            String                   user        = (String) row[5];
            Map<String, Object>      evalContext = new HashMap<>();

            RangerAccessRequestUtil.setCurrentUserInContext(evalContext, user);

            MatcherWrapper matcher = new MatcherWrapper(policyValue, excludes);

            assertEquals(getMessage(row), matchType, matcher.getMatchType(resource, matchScope, evalContext));
            assertEquals(getMessage(row), result, matcher.isMatch(resource, matchScope, evalContext));
        }
    }

    @Test
    public void testIsPrefixMatch() {
        ResourceElementMatchingScope matchScope = ResourceElementMatchingScope.SELF_OR_PREFIX;

        for (Object[] row : dataForPrefixMatch) {
            String                   resource    = (String)row[0];
            String                   policyValue = (String)row[1];
            boolean                  excludes    = (boolean)row[2];
            ResourceElementMatchType matchType   = (ResourceElementMatchType) row[3];
            boolean                  result      = (boolean)row[4];
            String                   user        = (String) row[5];
            Map<String, Object>      evalContext = new HashMap<>();

            RangerAccessRequestUtil.setCurrentUserInContext(evalContext, user);

            MatcherWrapper matcher = new MatcherWrapper(policyValue, excludes);

            assertEquals(getMessage(row), matchType, matcher.getMatchType(resource, matchScope, evalContext));
            assertEquals(getMessage(row), result, matcher.isMatch(resource, matchScope, evalContext));
        }
    }

    String getMessage(Object[] row) {
        return String.format("Resource=%s, Policy=%s, excludes=%s, matchScope=%s, matchType=%s, result=%s",
                row[0], row[1], row[2], row[3], row[4], row[5]);
    }

    static class MatcherWrapper extends RangerDefaultResourceMatcher {
        MatcherWrapper(String policyValue, boolean exclude) {
            RangerResourceDef   resourceDef    = new RangerResourceDef();
            Map<String, String> matcherOptions = new HashMap<>();

            matcherOptions.put(OPTION_WILD_CARD, Boolean.toString(policyValue.contains(WILDCARD_ASTERISK) || policyValue.contains(WILDCARD_QUESTION_MARK)));
            matcherOptions.put(OPTION_IGNORE_CASE, Boolean.toString(false));

            resourceDef.setMatcherOptions(matcherOptions);

            setResourceDef(resourceDef);

            RangerPolicy.RangerPolicyResource policyResource = new RangerPolicy.RangerPolicyResource();
            policyResource.setIsExcludes(exclude);
            policyResource.setValues(Lists.newArrayList(policyValue));
            setPolicyResource(policyResource);


            init();
        }
    }

}