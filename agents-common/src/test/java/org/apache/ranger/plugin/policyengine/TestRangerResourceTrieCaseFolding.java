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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.TestStringUtil;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceElementMatchingScope;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerResourceEvaluator;
import org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestRangerResourceTrieCaseFolding {
    private static final char DOTLESS_I       = '\u0131'; // LATIN SMALL LETTER DOTLESS I
    private static final char LONG_S          = '\u017F'; // LATIN SMALL LETTER LONG S
    private static final char DOTTED_I        = '\u0130'; // LATIN CAPITAL LETTER I WITH DOT ABOVE
    private static final char KELVIN_SIGN     = '\u212A'; // KELVIN SIGN
    private static final char ANGSTROM_SIGN   = '\u212B'; // ANGSTROM SIGN
    private static final char LATIN_A_RING    = '\u00E5'; // LATIN SMALL LETTER A WITH RING ABOVE
    private static final char SHARP_S         = '\u00DF'; // LATIN SMALL LETTER SHARP S
    private static final char CAPITAL_SHARP_S = '\u1E9E'; // LATIN CAPITAL LETTER SHARP S

    private static final RangerResourceDef TABLE_RESOURCE_DEF = getTableResourceDef();

    private static final String SERVICE_TYPE        = "hive";
    private static final String SERVICE_NAME        = "test-hive";
    private static final String APP_ID              = "test-app";
    private static final String DATABASE_RESOURCE   = "database";
    private static final String ACCESS              = "select";
    private static final String USER                = "bob";
    private static final String SIBLING_USER        = "carol";
    private static final String INTERNAL_DB         = "internal";
    private static final String INTERNAL_DB_VARIANT = DOTLESS_I + "nternal";
    private static final String INTERNAL_DB_UPPER   = "INTERNAL";
    private static final String SIBLING_DB          = "internet";
    private static final String SIBLING_DB_VARIANT  = DOTLESS_I + "nternet";

    // folding sanity
    @Test
    public void testCaseFoldingMismatch_documented() {
        Object[][] cases = {
            {"i vs dotless-i", 'i', DOTLESS_I, true, false},
            {"s vs long-s", 's', LONG_S, true, false},
            {"I vs dotless-i", 'I', DOTLESS_I, true, false},
            {"S vs long-s", 'S', LONG_S, true, false},
            {"Sigma vs final-sigma", '\u03A3', '\u03C2', true, false},
            {"ASCII self", 'a', 'A', true, true},
        };

        for (Object[] c : cases) {
            String  label                      = (String) c[0];
            char    c1                         = (Character) c[1];
            char    c2                         = (Character) c[2];
            boolean expectedEqualsIgnoreCase   = (Boolean) c[3];
            boolean expectedToLowerCaseEqual   = (Boolean) c[4];

            assertEquals(label, expectedEqualsIgnoreCase, StringUtils.equalsIgnoreCase(String.valueOf(c1), String.valueOf(c2)));
            assertEquals(label, expectedToLowerCaseEqual, Character.toLowerCase(c1) == Character.toLowerCase(c2));
            // The bug exists precisely when equalsIgnoreCase is true but toLowerCase keys differ.
            if (expectedEqualsIgnoreCase && !expectedToLowerCaseEqual) {
                assertTrue(label + ": pair must be equal under equalsIgnoreCase", StringUtils.equalsIgnoreCase(String.valueOf(c1), String.valueOf(c2)));
                assertNotEquals(label + ": trie toLowerCase fold must not be assumed equivalent to equalsIgnoreCase", Character.toLowerCase(c1), Character.toLowerCase(c2));
            }
        }
    }

    @Test
    public void testDoubleFoldRequired_singleUpperCaseInsufficient() {
        Object[][] cases = {
            {"i vs dotted-I", 'i', DOTTED_I},
            {"k vs kelvin-sign", 'k', KELVIN_SIGN},
            {"K vs kelvin-sign", 'K', KELVIN_SIGN},
            {"a-ring vs angstrom-sign", LATIN_A_RING, ANGSTROM_SIGN},
            {"A-ring vs angstrom-sign", '\u00C5', ANGSTROM_SIGN},
            {"sharp-s vs capital-sharp-s", SHARP_S, CAPITAL_SHARP_S},
        };

        for (Object[] c : cases) {
            String label = (String) c[0];
            char   c1    = (Character) c[1];
            char   c2    = (Character) c[2];

            assertTrue(label + ": matcher treats pair as equal", StringUtils.equalsIgnoreCase(String.valueOf(c1), String.valueOf(c2)));
            assertNotEquals(label + ": single toUpperCase would still split trie child keys", Character.toUpperCase(c1), Character.toUpperCase(c2));
            assertEquals(label + ": double-fold must produce a common trie lookup key", trieLookupFold(c1), trieLookupFold(c2));
        }
    }

    @Test
    public void testTrieFindsEvaluatorWhenSingleUpperCaseWouldMiss() {
        Object[][] cases = {
            {"dotted-I vs i", "tablei", "table" + DOTTED_I, "tablej"},
            {"kelvin-sign vs k", "fook", "foo" + KELVIN_SIGN, "foot"},
            {"angstrom-sign vs a-ring", "table" + LATIN_A_RING, "table" + ANGSTROM_SIGN, "tableb"},
            {"capital-sharp-s vs sharp-s", "table" + SHARP_S, "table" + CAPITAL_SHARP_S, "tablex"},
        };

        for (Object[] c : cases) {
            String label        = (String) c[0];
            String policyName   = (String) c[1];
            String requestName  = (String) c[2];
            String siblingName  = (String) c[3];

            RangerResourceEvaluator                     evalPolicy  = evaluator(policyName, "policy-" + label);
            RangerResourceEvaluator                     evalSibling = evaluator(siblingName, "sibling-" + label);
            RangerResourceTrie<RangerResourceEvaluator> trie        = newTrie(evalPolicy, evalSibling);

            Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource(requestName, ResourceElementMatchingScope.SELF);

            assertContainsEvaluators(result, evalPolicy);
            assertFalse(label + ": request must not resolve only to the unrelated sibling branch", containsOnly(result, evalSibling));
        }
    }

    // matcher baseline
    @Test
    public void testMatcherTreatsDotlessIAsEqualToI() {
        RangerResourceMatcher matcher = newMatcher("tablei");

        assertTrue(matcher.isMatch("table" + DOTLESS_I, ResourceElementMatchingScope.SELF, null));
        assertTrue(matcher.isMatch("tablei", ResourceElementMatchingScope.SELF, null));
    }

    @Test
    public void testMatcherTreatsLongSAsEqualToS() {
        RangerResourceMatcher matcher = newMatcher("foo" + 's');

        assertTrue(matcher.isMatch("foo" + LONG_S, ResourceElementMatchingScope.SELF, null));
        assertTrue(matcher.isMatch("foos", ResourceElementMatchingScope.SELF, null));
    }

    @Test
    public void testMatcherTreatsDottedIAsEqualToI() {
        RangerResourceMatcher matcher = newMatcher("tablei");

        assertTrue(matcher.isMatch("table" + DOTTED_I, ResourceElementMatchingScope.SELF, null));
        assertTrue(matcher.isMatch("tablei", ResourceElementMatchingScope.SELF, null));
    }

    @Test
    public void testMatcherTreatsKelvinSignAsEqualToK() {
        RangerResourceMatcher matcher = newMatcher("fook");

        assertTrue(matcher.isMatch("foo" + KELVIN_SIGN, ResourceElementMatchingScope.SELF, null));
        assertTrue(matcher.isMatch("fook", ResourceElementMatchingScope.SELF, null));
    }

    @Test
    public void testMatcherTreatsAngstromSignAsEqualToARing() {
        RangerResourceMatcher matcher = newMatcher("table" + LATIN_A_RING);

        assertTrue(matcher.isMatch("table" + ANGSTROM_SIGN, ResourceElementMatchingScope.SELF, null));
        assertTrue(matcher.isMatch("table" + LATIN_A_RING, ResourceElementMatchingScope.SELF, null));
    }

    // trie lookup at branch boundaries
    @Test
    public void testTrieFindsExactEvaluatorForDotlessIAtBranchBoundary() {
        RangerResourceEvaluator                     evalTableI = evaluator("tablei", "deny-tablei");
        RangerResourceEvaluator                     evalTableJ = evaluator("tablej", "other-tablej");
        RangerResourceTrie<RangerResourceEvaluator> trie       = newTrie(evalTableI, evalTableJ);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource("table" + DOTLESS_I, ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalTableI);
        assertFalse("dotless-i request must not resolve only to the unrelated sibling branch", containsOnly(result, evalTableJ));
    }

    @Test
    public void testTrieFindsExactEvaluatorForLongSAtBranchBoundary() {
        RangerResourceEvaluator                     evalFoos = evaluator("foos", "deny-foos");
        RangerResourceEvaluator                     evalFooT = evaluator("foot", "other-foot");
        RangerResourceTrie<RangerResourceEvaluator> trie     = newTrie(evalFoos, evalFooT);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource("foo" + LONG_S, ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalFoos);
    }

    @Test
    public void testTrieFindsSingleCharEvaluatorForDotlessI() {
        RangerResourceEvaluator                     evalI = evaluator("i", "deny-i");
        RangerResourceEvaluator                     evalJ = evaluator("j", "other-j");
        RangerResourceTrie<RangerResourceEvaluator> trie  = newTrie(evalI, evalJ);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource(String.valueOf(DOTLESS_I), ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalI);
    }

    @Test
    public void testTrieFindsSingleCharEvaluatorForLongS() {
        RangerResourceEvaluator                     evalS = evaluator("s", "deny-s");
        RangerResourceEvaluator                     evalT = evaluator("t", "other-t");
        RangerResourceTrie<RangerResourceEvaluator> trie  = newTrie(evalS, evalT);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource(String.valueOf(LONG_S), ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalS);
    }

    @Test
    public void testDenyEvaluatorIncludedWhenRequestUsesDotlessI() {
        RangerResourceEvaluator                     evalDeny  = evaluator("tablei", "deny-tablei");
        RangerResourceEvaluator                     evalAllow = evaluator("*", "allow-all");
        RangerResourceEvaluator                     evalOther = evaluator("tablej", "other-tablej");
        RangerResourceTrie<RangerResourceEvaluator> trie      = newTrie(evalDeny, evalAllow, evalOther);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource("table" + DOTLESS_I, ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalDeny);
        assertTrue("broader ALLOW wildcard evaluator is still a valid candidate", result.contains(evalAllow));
        assertNotEquals("DENY evaluator must not be omitted while only the broader ALLOW remains", result, Collections.singleton(evalAllow));
    }

    @Test
    public void testDenyEvaluatorIncludedWhenRequestUsesLongS() {
        RangerResourceEvaluator                     evalDeny  = evaluator("foos", "deny-foos");
        RangerResourceEvaluator                     evalAllow = evaluator("*", "allow-all");
        RangerResourceEvaluator                     evalOther = evaluator("foot", "other-foot");
        RangerResourceTrie<RangerResourceEvaluator> trie      = newTrie(evalDeny, evalAllow, evalOther);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource("foo" + LONG_S, ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalDeny);
        assertNotEquals("DENY evaluator must not be omitted while only the broader ALLOW remains", result, Collections.singleton(evalAllow));
    }

    @Test
    public void testPolicyEngineEvaluatesDotlessIVariantForInternalDatabase() {
        RangerPluginConfig pluginConfig = pluginConfigForPolicyEngineTests();
        RangerBasePlugin   plugin       = new RangerBasePlugin(pluginConfig, createServicePoliciesForPluginTests(), null, null);

        assertFalse(pluginConfig.getPolicyEngineOptions().disableTrieLookupPrefilter);

        RangerAccessResult control = evaluate(plugin, USER, INTERNAL_DB);
        RangerAccessResult upper   = evaluate(plugin, USER, INTERNAL_DB_UPPER);
        RangerAccessResult variant = evaluate(plugin, USER, INTERNAL_DB_VARIANT);

        assertTrue("control: bob select on database=internal", isAccessNotAllowed(control));
        assertTrue("negative control: bob select on database=INTERNAL", isAccessNotAllowed(upper));
        assertTrue("dotless-i variant: bob select on database=" + INTERNAL_DB_VARIANT, isAccessNotAllowed(variant));
    }

    @Test
    public void testPolicyEngineEvaluatesSiblingDotlessIVariantWhenTrieBranchSplits() {
        RangerPluginConfig pluginConfig = pluginConfigForPolicyEngineTests();
        RangerBasePlugin   plugin       = new RangerBasePlugin(pluginConfig, createServicePoliciesForPluginTests(), null, null);

        assertFalse(pluginConfig.getPolicyEngineOptions().disableTrieLookupPrefilter);

        RangerAccessResult control = evaluate(plugin, SIBLING_USER, SIBLING_DB);
        RangerAccessResult variant = evaluate(plugin, SIBLING_USER, SIBLING_DB_VARIANT);

        assertTrue("control: carol select on database=internet", isAccessNotAllowed(control));
        assertTrue("dotless-i variant: carol select on database=" + SIBLING_DB_VARIANT, isAccessNotAllowed(variant));
    }

    // regression / edge cases
    @Test
    public void testAsciiCaseFoldingStillWorks() {
        RangerResourceEvaluator                     eval = evaluator("MyTable", "allow-mytable");
        RangerResourceTrie<RangerResourceEvaluator> trie = newTrie(eval);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource("mytable", ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, eval);
    }

    @Test
    public void testCompressedNodeMatchesDotlessIWithoutSiblingBranch() {
        RangerResourceEvaluator                     evalTableI = evaluator("tablei", "deny-tablei");
        RangerResourceTrie<RangerResourceEvaluator> trie       = newTrie(evalTableI);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource("table" + DOTLESS_I, ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalTableI);
    }

    @Test
    public void testBidirectionalLookup_iAndDotlessI() {
        RangerResourceEvaluator                     evalDotlessI = evaluator("table" + DOTLESS_I, "policy-dotless");
        RangerResourceEvaluator                     evalOther    = evaluator("tablej", "other");
        RangerResourceTrie<RangerResourceEvaluator> trie         = newTrie(evalDotlessI, evalOther);

        Set<RangerResourceEvaluator> resultForI = trie.getEvaluatorsForResource("tablei", ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(resultForI, evalDotlessI);
    }

    // helpers

    private static char trieLookupFold(char ch) {
        return Character.toLowerCase(Character.toUpperCase(ch));
    }

    private static void assertContainsEvaluators(Set<RangerResourceEvaluator> result, RangerResourceEvaluator... expected) {
        assertNotNull("trie must return evaluators, not null", result);
        for (RangerResourceEvaluator evaluator : expected) {
            assertTrue("expected evaluator " + evaluator + " in result " + result, result.contains(evaluator));
        }
    }

    private static boolean containsOnly(Set<RangerResourceEvaluator> result, RangerResourceEvaluator evaluator) {
        return result != null && result.size() == 1 && result.contains(evaluator);
    }

    private static RangerResourceTrie<RangerResourceEvaluator> newTrie(RangerResourceEvaluator... evaluators) {
        List<RangerResourceEvaluator> list = Arrays.asList(evaluators);
        return new RangerResourceTrie<>(TABLE_RESOURCE_DEF, list);
    }

    private static RangerResourceEvaluator evaluator(String resourceValue, String label) {
        return new TestResourceEvaluator(new RangerPolicyResource(resourceValue, false, false), label);
    }

    private static RangerResourceMatcher newMatcher(String policyValue) {
        RangerDefaultResourceMatcher matcher = new RangerDefaultResourceMatcher();
        matcher.setResourceDef(TABLE_RESOURCE_DEF);
        matcher.setPolicyResource(new RangerPolicyResource(policyValue, false, false));
        matcher.init();
        return matcher;
    }

    private static RangerResourceDef getTableResourceDef() {
        RangerResourceDef ret = new RangerResourceDef();
        ret.setItemId(1L);
        ret.setName("table");
        ret.setType("string");
        ret.setLevel(20);
        ret.setMatcher("org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher");
        ret.setMatcherOptions(TestStringUtil.mapFromStrings("wildCard", "true", "ignoreCase", "true"));
        return ret;
    }

    private static RangerPluginConfig pluginConfigForPolicyEngineTests() {
        RangerPolicyEngineOptions peOptions = new RangerPolicyEngineOptions();
        peOptions.disablePolicyRefresher    = true;
        peOptions.disableTagRetriever       = true;
        peOptions.disableUserStoreRetriever = true;
        return new RangerPluginConfig(SERVICE_TYPE, SERVICE_NAME, APP_ID, "cl1", "on-prem", peOptions);
    }

    private static RangerAccessResult evaluate(RangerBasePlugin plugin, String user, String database) {
        Map<String, Object> resource = new HashMap<>();
        resource.put(DATABASE_RESOURCE, database);

        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setResource(new RangerAccessResourceImpl(resource));
        request.setUser(user);
        request.setAccessType(ACCESS);
        request.setAction(ACCESS);
        return plugin.isAccessAllowed(request);
    }

    private static boolean isAccessNotAllowed(RangerAccessResult result) {
        boolean ret = result != null && result.getIsAccessDetermined() && !result.getIsAllowed();
        return ret;
    }

    private static ServicePolicies createServicePoliciesForPluginTests() {
        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceName(SERVICE_NAME);
        servicePolicies.setServiceDef(createServiceDefForPluginTests());
        servicePolicies.setPolicyVersion(1L);
        servicePolicies.setPolicies(Collections.unmodifiableList(Arrays.asList(broadAllowPolicy(), internalDatabasePolicy(), internetDatabasePolicy())));
        return servicePolicies;
    }

    private static RangerPolicy internalDatabasePolicy() {
        return databasePolicyForUser(100L, "policy-internal", INTERNAL_DB, USER);
    }

    private static RangerPolicy internetDatabasePolicy() {
        return databasePolicyForUser(102L, "policy-internet", SIBLING_DB, SIBLING_USER);
    }

    private static RangerPolicy databasePolicyForUser(long id, String name, String dbValue, String user) {
        RangerPolicy p = basePolicy(id, name);
        p.setResources(Collections.singletonMap(DATABASE_RESOURCE, new RangerPolicyResource(dbValue, false, false)));

        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(Collections.singletonList(user));
        item.setAccesses(Collections.singletonList(new RangerPolicyItemAccess(ACCESS, Boolean.TRUE)));
        p.setDenyPolicyItems(Collections.singletonList(item));
        return p;
    }

    private static RangerPolicy broadAllowPolicy() {
        RangerPolicy p = basePolicy(101L, "allow-all-db");
        p.setResources(Collections.singletonMap(DATABASE_RESOURCE, new RangerPolicyResource("*", false, false)));

        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(Arrays.asList(USER, SIBLING_USER));
        item.setAccesses(Collections.singletonList(new RangerPolicyItemAccess(ACCESS, Boolean.TRUE)));
        p.setPolicyItems(Collections.singletonList(item));
        return p;
    }

    private static RangerPolicy basePolicy(long id, String name) {
        RangerPolicy ret = new RangerPolicy();
        ret.setId(id);
        ret.setName(name);
        ret.setService(SERVICE_NAME);
        ret.setIsEnabled(true);
        return ret;
    }

    private static RangerServiceDef createServiceDefForPluginTests() {
        RangerResourceDef databaseResourceDef = new RangerResourceDef();
        databaseResourceDef.setItemId(1L);
        databaseResourceDef.setName(DATABASE_RESOURCE);
        databaseResourceDef.setType("string");
        databaseResourceDef.setLevel(10);
        databaseResourceDef.setParent("");
        databaseResourceDef.setMatcher("org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher");
        databaseResourceDef.setMatcherOptions(TestStringUtil.mapFromStrings("wildCard", "true", "ignoreCase", "true"));

        RangerAccessTypeDef accessTypeDef = new RangerAccessTypeDef();
        accessTypeDef.setItemId(1L);
        accessTypeDef.setName(ACCESS);

        RangerServiceDef ret = new RangerServiceDef();
        ret.setName(SERVICE_TYPE);
        ret.setResources(Collections.singletonList(databaseResourceDef));
        ret.setAccessTypes(Collections.singletonList(accessTypeDef));
        return ret;
    }

    private static class TestResourceEvaluator implements RangerResourceEvaluator {
        private static long nextId = 1;

        private final long                  id;
        private final RangerPolicyResource  policyResource;
        private final RangerResourceMatcher resourceMatcher;
        private final String                label;

        TestResourceEvaluator(RangerPolicyResource policyResource, String label) {
            this.id              = nextId++;
            this.policyResource  = policyResource;
            this.label           = label;
            this.resourceMatcher = new RangerDefaultResourceMatcher();
            resourceMatcher.setResourceDef(TABLE_RESOURCE_DEF);
            resourceMatcher.setPolicyResource(policyResource);
            resourceMatcher.init();
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public RangerPolicyResourceMatcher getPolicyResourceMatcher() {
            return null;
        }

        @Override
        public Map<String, RangerPolicyResource> getPolicyResource() {
            return Collections.singletonMap(TABLE_RESOURCE_DEF.getName(), policyResource);
        }

        @Override
        public RangerResourceMatcher getResourceMatcher(String resourceName) {
            return resourceMatcher;
        }

        @Override
        public boolean isAncestorOf(RangerResourceDef resourceDef) {
            return false;
        }

        @Override
        public boolean isLeaf(String resourceName) {
            return true;
        }

        @Override
        public String toString() {
            return label + "(id=" + id + ", resource=" + policyResource.getValues() + ")";
        }
    }
}
