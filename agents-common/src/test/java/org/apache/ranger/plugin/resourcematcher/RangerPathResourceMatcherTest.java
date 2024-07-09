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
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RangerPathResourceMatcherTest {

    Object[][] data = {
            // resource               policy               wildcard  recursive  result user
            { "/app/hive/test.db",    "/",                 true,     false,     false, "user" },
            { "/app/hive/test.db",    "/",                 true,     true,      true,  "user" },
            { "/app/hive/test.db",    "/*",                true,     false,     true,  "user" },
            { "/app/hbase/test.tbl",  "/*",                true,     false,     true,  "user" },
            { "/app/hive/test.db",    "/app",              true,     false,     false, "user" },
            { "/app/hive/test.db",    "/app/",             true,     false,     false, "user" },
            { "/app/hive/test.db",    "/app/",             true,     true,      true,  "user" },
            { "/app/hive/test.db",    "/app/*",            true,     false,     true,  "user" },
            { "/app/hbase/test.tbl",  "/app/*",            true,     false,     true,  "user" },
            { "/app/hive/test.db",    "/app/hive/*",       true,     false,     true,  "user" },
            { "/app/hbase/test.tbl",  "/app/hive/*",       true,     false,     false, "user" },
            { "/app/hive/test.db",    "/app/hive/test*",   true,     false,     true,  "user" },
            { "/app/hbase/test.tbl",  "/app/hive/test*",   true,     false,     false, "user" },
            { "/app/hive/test.db",    "/app/hive/test.db", true,     false,     true,  "user" },
            { "/app/hbase/test.tbl",  "/app/hive/test.db", true,     false,     false, "user" },
            { "app/hive/*",           "app/hive/*",        false,    false,     true,  "user" },  // simple string match
            { "app/hive/test.db",     "app/hive/*",        false,    false,     false, "user" }, // simple string match
            { "/app/",                "/app/",             true,     true,      true,  "user" },
            { "/app/",                "/app/",             true,     false,     true,  "user" },
            { "/app",                 "/app/",             true,     true,      false, "user" },
            { "/app",                 "/app/",             true,     false,     false, "user" },
            { "/app/",                "/app/*",            true,     true,      true,  "user" },
            { "/app/",                "/app/*",            true,     false,     true,  "user" },
            { "/app",                 "/app/*",            true,     true,      false, "user" },
            { "/app",                 "/app/*",            true,     false,     false, "user" },
    };

    Object[][] dataForSelfOrChildScope = {
            // { resource, policy, optWildcard, recursive, result
            { "/app/hive/test.db",    "/",                 true, false, false, "user" },
            { "/app/hive/test.db",    "/",                 true, true,  true, "user" },
            { "/app/hive/test.db",    "/*",                true, false, true, "user" },
            { "/app/hbase/test.tbl",  "/*",                true, false, true, "user" },
            { "/app/hive/test.db",    "/app",              true, false, false, "user" },
            { "/app/hive/test.db",    "/app/",             true, false, false, "user" },
            { "/app/hive/test.db",    "/app/",             true, true,  true, "user" },
            { "/app/hive/test.db",    "/app/*",            true, false, true, "user" },
            { "/app/hbase/test.tbl",  "/app/*",            true, false, true, "user" },
            { "/app/hive/test.db",    "/app/hive/*",       true, false, true, "user" },
            { "/app/hbase/test.tbl",  "/app/hive/*",       true, false, false, "user" },
            { "/app/hive/test.db",    "/app/hive/test*",   true, false, true, "user" },
            { "/app/hbase/test.tbl",  "/app/hive/test*",   true, false, false, "user" },
            { "/app/hive/test.db",    "/app/hive/test.db", true, false, true, "user" },
            { "/app/hbase/test.tbl",  "/app/hive/test.db", true, false, false, "user" },
            { "/app/hbase/test.db",   "/app/hbase",        true, true, true,   "user" },
            { "/app/hbase/test.db",   "/app/hbase/test.db/test.tbl", true, true, true, "user" },
            { "/app/hbase/test.db/",  "/app/hbase/test.db/test.tbl", true, true, true, "user" },
            { "/app/hbase/test.db",   "/app/hbase/test.db/test.tbl/test.col", true, true, false, "user" },
            { "/app/hbase/test.db",   "/app/h*/test.db/test.tbl",    true, true, true, "user" },
            { "/app/hbase/test.db",   "/app/hbase/test.db/test.tbl", true, false, true, "user" },
            { "/app/hbase/test.db/",  "/app/hbase/test.db/test.tbl", true, false, true, "user" },
            { "/app/hbase/test.db/",  "/app/hbase/test.db/test.tbl", true, false, true, "user" },
            { "/app/hbase/test.db",   "/app/h*/test.db/test.tbl",    true, false, true, "user" },
            { "/app/hbase/test.db",   "*/hbase/test.db/test.tbl",    true, false, true, "user" },
            { "/app/hbase/test.db",   "/app/hbase/test.db/test.t*",  true, false, true, "user" },
            { "/app/hbase/test.db",   "/app/hbase/test.db/tmp/test.t*",  true, false, false, "user" },
    };

    Object[][] dataForSelfOrPrefixScope = {
            // { resource, policy, optWildcard, recursive, result
            { "/",                 "/app/hive/test.db", true, false, true, "user" },
            { "/app",              "/app/hive/test.db", true, false, true, "user" },
            { "/app/",             "/app/hive/test.db", true, false, true, "user" },
            { "/app/hive",         "/app/hive/test.db", true, false, true, "user" },
            { "/app/hive/",        "/app/hive/test.db", true, false, true, "user" },
            { "/app/hive/test.db", "/app/hive/test.db", true, false, true, "user" },
            { "/",                 "/app/*/test.db",    true, false, true, "user" },
            { "/app",              "/app/*/test.db",    true, false, true, "user" },
            { "/app/",             "/app/*/test.db",    true, false, true, "user" },
            { "/app/hive",         "/app/*/test.db",    true, false, true, "user" },
            { "/app/hive/",        "/app/*/test.db",    true, false, true, "user" },
            { "/app/hive/test.db", "/app/*/test.db",    true, false, true, "user" },
            { "/",                 "*/hive/test.db",    true, false, true, "user" },
            { "/app",              "*/hive/test.db",    true, false, true, "user" },
            { "/app/",             "*/hive/test.db",    true, false, true, "user" },
            { "/app/hive",         "*/hive/test.db",    true, false, true, "user" },
            { "/app/hive/",        "*/hive/test.db",    true, false, true, "user" },
            { "/app/hive/test.db", "*/hive/test.db",    true, false, true, "user" },
            { "/",                 "/*",                true, false, true, "user" },
            { "/app",              "/*",                true, false, true, "user" },
            { "/app/",             "/*",                true, false, true, "user" },
            { "/app/hive",         "/*",                true, false, true, "user" },
            { "/app/hive/",        "/*",                true, false, true, "user" },
            { "/app/hive/test.db", "/*",                true, false, true, "user" },

            { "/",                 "/app/hive/test.db", true, true, true, "user" },
            { "/app",              "/app/hive/test.db", true, true, true, "user" },
            { "/app/",             "/app/hive/test.db", true, true, true, "user" },
            { "/app/hive",         "/app/hive/test.db", true, true, true, "user" },
            { "/app/hive/",        "/app/hive/test.db", true, true, true, "user" },
            { "/app/hive/test.db", "/app/hive/test.db", true, true, true, "user" },
            { "/",                 "/app/*/test.db",    true, true, true, "user" },
            { "/app",              "/app/*/test.db",    true, true, true, "user" },
            { "/app/",             "/app/*/test.db",    true, true, true, "user" },
            { "/app/hive",         "/app/*/test.db",    true, true, true, "user" },
            { "/app/hive/",        "/app/*/test.db",    true, true, true, "user" },
            { "/app/hive/test.db", "/app/*/test.db",    true, true, true, "user" },
            { "/",                 "*/hive/test.db",    true, true, true, "user" },
            { "/app",              "*/hive/test.db",    true, true, true, "user" },
            { "/app/",             "*/hive/test.db",    true, true, true, "user" },
            { "/app/hive",         "*/hive/test.db",    true, true, true, "user" },
            { "/app/hive/",        "*/hive/test.db",    true, true, true, "user" },
            { "/app/hive/test.db", "*/hive/test.db",    true, true, true, "user" },
            { "/",                 "/",                 true, true, true, "user" },
            { "/app",              "/",                 true, true, true, "user" },
            { "/app/",             "/",                 true, true, true, "user" },
            { "/app/hive",         "/",                 true, true, true, "user" },
            { "/app/hive/",        "/",                 true, true, true, "user" },
            { "/app/hive/test.db", "/",                 true, true, true, "user" },
    };

    @Test
    public void testIsMatch() throws Exception {
        for (Object[] row : data) {
            String resource = (String)row[0];
            String policyValue = (String)row[1];
            boolean optWildcard = (boolean)row[2];
            boolean isRecursive = (boolean)row[3];
            boolean result = (boolean)row[4];
            String user = (String) row[5];

            Map<String, Object> evalContext = new HashMap<>();
            RangerAccessRequestUtil.setCurrentUserInContext(evalContext, user);

            MatcherWrapper matcher = new MatcherWrapper(policyValue, optWildcard, isRecursive);
            assertEquals(getMessage(row), result, matcher.isMatch(resource, ResourceElementMatchingScope.SELF, evalContext));
        }
    }

    @Test
    public void testIsMatchForSelfOrChildScope() throws Exception {
        for (Object[] row : dataForSelfOrChildScope) {
            String resource = (String)row[0];
            String policyValue = (String)row[1];
            boolean optWildcard = (boolean)row[2];
            boolean isRecursive = (boolean)row[3];
            boolean result = (boolean)row[4];
            String user = (String) row[5];

            Map<String, Object> evalContext = new HashMap<>();
            RangerAccessRequestUtil.setCurrentUserInContext(evalContext, user);

            MatcherWrapper matcher = new MatcherWrapper(policyValue, optWildcard, isRecursive);
            assertEquals(getMessage(row), result, matcher.isMatch(resource, ResourceElementMatchingScope.SELF_OR_CHILD, evalContext));
        }
    }

    @Test
    public void testIsMatchForSelfOrPrefixScope() {
        ResourceElementMatchingScope matchScope = ResourceElementMatchingScope.SELF_OR_PREFIX;

        for (Object[] row : dataForSelfOrPrefixScope) {
            String  resource    = (String)row[0];
            String  policyValue = (String)row[1];
            boolean optWildcard = (boolean)row[2];
            boolean isRecursive = (boolean)row[3];
            boolean result      = (boolean)row[4];
            String  user        = (String) row[5];
            Map<String, Object> evalContext = new HashMap<>();

            RangerAccessRequestUtil.setCurrentUserInContext(evalContext, user);

            MatcherWrapper matcher = new MatcherWrapper(policyValue, optWildcard, isRecursive);
            assertEquals(getMessage(row), result, matcher.isMatch(resource, matchScope, evalContext));
        }
    }

    String getMessage(Object[] row) {
        return String.format("Resource=%s, Policy=%s, optWildcard=%s, recursive=%s, result=%s",
                (String)row[0], (String)row[1], (boolean)row[2], (boolean)row[3], (boolean)row[4]);
    }

    static class MatcherWrapper extends RangerPathResourceMatcher {
        MatcherWrapper(String policyValue, boolean optWildcard, boolean isRecursive) {
            RangerResourceDef   resourceDef    = new RangerResourceDef();
            Map<String, String> matcherOptions = Collections.singletonMap(OPTION_WILD_CARD, Boolean.toString(optWildcard));

            resourceDef.setMatcherOptions(matcherOptions);

            setResourceDef(resourceDef);

            RangerPolicy.RangerPolicyResource policyResource = new RangerPolicy.RangerPolicyResource();
            policyResource.setIsRecursive(isRecursive);
            policyResource.setValues(Lists.newArrayList(policyValue));
            setPolicyResource(policyResource);

            init();
       }
    }

}