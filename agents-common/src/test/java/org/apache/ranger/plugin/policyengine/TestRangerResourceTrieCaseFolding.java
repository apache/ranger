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
import org.apache.ranger.authorization.utils.TestStringUtil;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceElementMatchingScope;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerResourceEvaluator;
import org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

class TestRangerResourceTrieCaseFolding {
    private static final char DOTLESS_I       = '\u0131'; // LATIN SMALL LETTER DOTLESS I
    private static final char LONG_S          = '\u017F'; // LATIN SMALL LETTER LONG S
    private static final char DOTTED_I        = '\u0130'; // LATIN CAPITAL LETTER I WITH DOT ABOVE
    private static final char KELVIN_SIGN     = '\u212A'; // KELVIN SIGN
    private static final char ANGSTROM_SIGN   = '\u212B'; // ANGSTROM SIGN
    private static final char LATIN_A_RING    = '\u00E5'; // LATIN SMALL LETTER A WITH RING ABOVE
    private static final char SHARP_S         = '\u00DF'; // LATIN SMALL LETTER SHARP S
    private static final char CAPITAL_SHARP_S = '\u1E9E'; // LATIN CAPITAL LETTER SHARP S

    private static final RangerResourceDef TABLE_RESOURCE_DEF = getTableResourceDef();

    // folding sanity
    static Stream<Arguments> caseFoldingMismatchPairs() {
        return Stream.of(Arguments.of("i vs dotless-i", 'i', DOTLESS_I, true, false), Arguments.of("s vs long-s", 's', LONG_S, true, false), Arguments.of("I vs dotless-i", 'I', DOTLESS_I, true, false), Arguments.of("S vs long-s", 'S', LONG_S, true, false), Arguments.of("Sigma vs final-sigma", '\u03A3', '\u03C2', true, false), Arguments.of("ASCII self", 'a', 'A', true, true));
    }

    static Stream<Arguments> doubleFoldRequiredPairs() {
        return Stream.of(Arguments.of("i vs dotted-I", 'i', DOTTED_I), Arguments.of("k vs kelvin-sign", 'k', KELVIN_SIGN), Arguments.of("K vs kelvin-sign", 'K', KELVIN_SIGN), Arguments.of("a-ring vs angstrom-sign", LATIN_A_RING, ANGSTROM_SIGN), Arguments.of("A-ring vs angstrom-sign", '\u00C5', ANGSTROM_SIGN), Arguments.of("sharp-s vs capital-sharp-s", SHARP_S, CAPITAL_SHARP_S));
    }

    static Stream<Arguments> doubleFoldBranchBoundaryCases() {
        return Stream.of(Arguments.of("dotted-I vs i", "tablei", "table" + DOTTED_I, "tablej"), Arguments.of("kelvin-sign vs k", "fook", "foo" + KELVIN_SIGN, "foot"), Arguments.of("angstrom-sign vs a-ring", "table" + LATIN_A_RING, "table" + ANGSTROM_SIGN, "tableb"), Arguments.of("capital-sharp-s vs sharp-s", "table" + SHARP_S, "table" + CAPITAL_SHARP_S, "tablex"));
    }

    @ParameterizedTest(name = "{0} vs {1}: equalsIgnoreCase={2}, toLowerCaseEqual={3}")
    @MethodSource("caseFoldingMismatchPairs")
    void testCaseFoldingMismatch_documented(String label, char c1, char c2, boolean expectedEqualsIgnoreCase, boolean expectedToLowerCaseEqual) {
        Assertions.assertEquals(expectedEqualsIgnoreCase, StringUtils.equalsIgnoreCase(String.valueOf(c1), String.valueOf(c2)), label);
        Assertions.assertEquals(expectedToLowerCaseEqual, Character.toLowerCase(c1) == Character.toLowerCase(c2), label);
        // The bug exists precisely when equalsIgnoreCase is true but toLowerCase keys differ.
        if (expectedEqualsIgnoreCase && !expectedToLowerCaseEqual) {
            Assertions.assertTrue(StringUtils.equalsIgnoreCase(String.valueOf(c1), String.valueOf(c2)), label + ": pair must be equal under equalsIgnoreCase");
            Assertions.assertNotEquals(Character.toLowerCase(c1), Character.toLowerCase(c2), label + ": trie toLowerCase fold must not be assumed equivalent to equalsIgnoreCase");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("doubleFoldRequiredPairs")
    void testDoubleFoldRequired_singleUpperCaseInsufficient(String label, char c1, char c2) {
        Assertions.assertTrue(StringUtils.equalsIgnoreCase(String.valueOf(c1), String.valueOf(c2)), label + ": matcher treats pair as equal");
        Assertions.assertNotEquals(Character.toUpperCase(c1), Character.toUpperCase(c2), label + ": single toUpperCase would still split trie child keys");
        Assertions.assertEquals(trieLookupFold(c1), trieLookupFold(c2), label + ": double-fold must produce a common trie lookup key");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("doubleFoldBranchBoundaryCases")
    void testTrieFindsEvaluatorWhenSingleUpperCaseWouldMiss(String label, String policyName, String requestName, String siblingName) {
        RangerResourceEvaluator                     evalPolicy  = evaluator(policyName, "policy-" + label);
        RangerResourceEvaluator                     evalSibling = evaluator(siblingName, "sibling-" + label);
        RangerResourceTrie<RangerResourceEvaluator> trie        = newTrie(evalPolicy, evalSibling);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource(requestName, ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalPolicy);
        Assertions.assertFalse(containsOnly(result, evalSibling), label + ": request must not resolve only to the unrelated sibling branch");
    }

    // matcher baseline
    @Test
    void testMatcherTreatsDotlessIAsEqualToI() {
        RangerResourceMatcher matcher = newMatcher("tablei");

        Assertions.assertTrue(matcher.isMatch("table" + DOTLESS_I, ResourceElementMatchingScope.SELF, null));
        Assertions.assertTrue(matcher.isMatch("tablei", ResourceElementMatchingScope.SELF, null));
    }

    @Test
    void testMatcherTreatsLongSAsEqualToS() {
        RangerResourceMatcher matcher = newMatcher("foo" + 's');

        Assertions.assertTrue(matcher.isMatch("foo" + LONG_S, ResourceElementMatchingScope.SELF, null));
        Assertions.assertTrue(matcher.isMatch("foos", ResourceElementMatchingScope.SELF, null));
    }

    @Test
    void testMatcherTreatsDottedIAsEqualToI() {
        RangerResourceMatcher matcher = newMatcher("tablei");

        Assertions.assertTrue(matcher.isMatch("table" + DOTTED_I, ResourceElementMatchingScope.SELF, null));
        Assertions.assertTrue(matcher.isMatch("tablei", ResourceElementMatchingScope.SELF, null));
    }

    @Test
    void testMatcherTreatsKelvinSignAsEqualToK() {
        RangerResourceMatcher matcher = newMatcher("fook");

        Assertions.assertTrue(matcher.isMatch("foo" + KELVIN_SIGN, ResourceElementMatchingScope.SELF, null));
        Assertions.assertTrue(matcher.isMatch("fook", ResourceElementMatchingScope.SELF, null));
    }

    @Test
    void testMatcherTreatsAngstromSignAsEqualToARing() {
        RangerResourceMatcher matcher = newMatcher("table" + LATIN_A_RING);

        Assertions.assertTrue(matcher.isMatch("table" + ANGSTROM_SIGN, ResourceElementMatchingScope.SELF, null));
        Assertions.assertTrue(matcher.isMatch("table" + LATIN_A_RING, ResourceElementMatchingScope.SELF, null));
    }

    // trie lookup at branch boundaries
    @Test
    void testTrieFindsExactEvaluatorForDotlessIAtBranchBoundary() {
        RangerResourceEvaluator                     evalTableI = evaluator("tablei", "deny-tablei");
        RangerResourceEvaluator                     evalTableJ = evaluator("tablej", "other-tablej");
        RangerResourceTrie<RangerResourceEvaluator> trie       = newTrie(evalTableI, evalTableJ);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource("table" + DOTLESS_I, ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalTableI);
        Assertions.assertFalse(containsOnly(result, evalTableJ), "dotless-i request must not resolve only to the unrelated sibling branch");
    }

    @Test
    void testTrieFindsExactEvaluatorForLongSAtBranchBoundary() {
        RangerResourceEvaluator                     evalFoos = evaluator("foos", "deny-foos");
        RangerResourceEvaluator                     evalFooT = evaluator("foot", "other-foot");
        RangerResourceTrie<RangerResourceEvaluator> trie     = newTrie(evalFoos, evalFooT);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource("foo" + LONG_S, ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalFoos);
    }

    @Test
    void testTrieFindsSingleCharEvaluatorForDotlessI() {
        RangerResourceEvaluator                     evalI = evaluator("i", "deny-i");
        RangerResourceEvaluator                     evalJ = evaluator("j", "other-j");
        RangerResourceTrie<RangerResourceEvaluator> trie  = newTrie(evalI, evalJ);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource(String.valueOf(DOTLESS_I), ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalI);
    }

    @Test
    void testTrieFindsSingleCharEvaluatorForLongS() {
        RangerResourceEvaluator                     evalS = evaluator("s", "deny-s");
        RangerResourceEvaluator                     evalT = evaluator("t", "other-t");
        RangerResourceTrie<RangerResourceEvaluator> trie  = newTrie(evalS, evalT);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource(String.valueOf(LONG_S), ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalS);
    }

    @Test
    void testDenyEvaluatorIncludedWhenRequestUsesDotlessI() {
        RangerResourceEvaluator                     evalDeny  = evaluator("tablei", "deny-tablei");
        RangerResourceEvaluator                     evalAllow = evaluator("*", "allow-all");
        RangerResourceEvaluator                     evalOther = evaluator("tablej", "other-tablej");
        RangerResourceTrie<RangerResourceEvaluator> trie      = newTrie(evalDeny, evalAllow, evalOther);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource("table" + DOTLESS_I, ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalDeny);
        Assertions.assertTrue(result.contains(evalAllow), "broader ALLOW wildcard evaluator is still a valid candidate");
        Assertions.assertNotEquals(result, Collections.singleton(evalAllow), "DENY evaluator must not be omitted while only the broader ALLOW remains");
    }

    @Test
    void testDenyEvaluatorIncludedWhenRequestUsesLongS() {
        RangerResourceEvaluator                     evalDeny  = evaluator("foos", "deny-foos");
        RangerResourceEvaluator                     evalAllow = evaluator("*", "allow-all");
        RangerResourceEvaluator                     evalOther = evaluator("foot", "other-foot");
        RangerResourceTrie<RangerResourceEvaluator> trie      = newTrie(evalDeny, evalAllow, evalOther);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource("foo" + LONG_S, ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalDeny);
        Assertions.assertNotEquals(result, Collections.singleton(evalAllow), "DENY evaluator must not be omitted while only the broader ALLOW remains");
    }

    // regression / edge cases
    @Test
    void testAsciiCaseFoldingStillWorks() {
        RangerResourceEvaluator                     eval = evaluator("MyTable", "allow-mytable");
        RangerResourceTrie<RangerResourceEvaluator> trie = newTrie(eval);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource("mytable", ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, eval);
    }

    @Test
    void testCompressedNodeMatchesDotlessIWithoutSiblingBranch() {
        RangerResourceEvaluator                     evalTableI = evaluator("tablei", "deny-tablei");
        RangerResourceTrie<RangerResourceEvaluator> trie       = newTrie(evalTableI);

        Set<RangerResourceEvaluator> result = trie.getEvaluatorsForResource("table" + DOTLESS_I, ResourceElementMatchingScope.SELF);

        assertContainsEvaluators(result, evalTableI);
    }

    @Test
    void testBidirectionalLookup_iAndDotlessI() {
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
        Assertions.assertNotNull(result, "trie must return evaluators, not null");
        for (RangerResourceEvaluator evaluator : expected) {
            Assertions.assertTrue(result.contains(evaluator), "expected evaluator " + evaluator + " in result " + result);
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
