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

package org.apache.ranger.plugin.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RangerActionListMatcherTest {
    @Test
    void testNullActionsMatchAll() {
        final RangerActionListMatcher matcher = new RangerActionListMatcher(null);

        assertTrue(matcher.isMatch("PutObject"));
        assertTrue(matcher.isMatch("GetObject"));
        assertTrue(matcher.isMatch(null));
        assertTrue(matcher.isMatch(""));
    }

    @Test
    void testEmptyActionsMatchAll() {
        final RangerActionListMatcher matcher = new RangerActionListMatcher(Collections.emptyList());

        assertTrue(matcher.isMatch("PutObject"));
        assertTrue(matcher.isMatch("GetObject"));
        assertTrue(matcher.isMatch(null));
    }

    @Test
    void testUniversalWildcard() {
        final RangerActionListMatcher matcher = new RangerActionListMatcher(Arrays.asList("*"));

        assertTrue(matcher.isMatch("PutObject"));
        assertTrue(matcher.isMatch("GetObject"));
        assertTrue(matcher.isMatch("DeleteBucket"));
        assertTrue(matcher.isMatch(null));
    }

    @Test
    void testBareWildcardWithSpaces() {
        // "  *  " after trim becomes "*" which should be treated as allow-all
        final RangerActionListMatcher matcher = new RangerActionListMatcher(Arrays.asList("  *  "));

        assertTrue(matcher.isMatch("PutObject"));
        assertTrue(matcher.isMatch("GetObject"));
    }

    @Test
    void testExactMatchCaseInsensitive() {
        final RangerActionListMatcher matcher = new RangerActionListMatcher(Arrays.asList("GetObject"));

        assertTrue(matcher.isMatch("GetObject"));
        assertTrue(matcher.isMatch("getobject"));
        assertTrue(matcher.isMatch("GETOBJECT"));
        assertFalse(matcher.isMatch("PutObject"));
        assertFalse(matcher.isMatch("GetObjectTagging"));
    }

    @Test
    void testMultipleExactActions() {
        final RangerActionListMatcher matcher = new RangerActionListMatcher(
                Arrays.asList("GetObject", "PutObject", "DeleteObject"));

        assertTrue(matcher.isMatch("GetObject"));
        assertTrue(matcher.isMatch("PutObject"));
        assertTrue(matcher.isMatch("DeleteObject"));
        assertFalse(matcher.isMatch("ListBucket"));
        assertFalse(matcher.isMatch("CreateBucket"));
    }

    @Test
    void testSinglePrefixWildcard() {
        final RangerActionListMatcher matcher = new RangerActionListMatcher(Arrays.asList("Put*"));

        assertTrue(matcher.isMatch("PutObject"));
        assertTrue(matcher.isMatch("PutObjectTagging"));
        assertTrue(matcher.isMatch("PutBucketAcl"));
        assertFalse(matcher.isMatch("GetObject"));
        assertFalse(matcher.isMatch("DeleteObject"));
    }

    @Test
    void testMultiplePrefixWildcards() {
        final RangerActionListMatcher matcher = new RangerActionListMatcher(
                Arrays.asList("Put*", "Get*"));

        assertTrue(matcher.isMatch("PutObject"));
        assertTrue(matcher.isMatch("GetObject"));
        assertTrue(matcher.isMatch("GetObjectTagging"));
        assertTrue(matcher.isMatch("PutBucketAcl"));
        assertFalse(matcher.isMatch("DeleteObject"));
        assertFalse(matcher.isMatch("ListBucket"));
    }

    @Test
    void testMixedExactAndPrefix() {
        final RangerActionListMatcher matcher = new RangerActionListMatcher(
                Arrays.asList("GetObject", "Put*", "ListBucket"));

        assertTrue(matcher.isMatch("GetObject"));
        assertTrue(matcher.isMatch("PutObject"));
        assertTrue(matcher.isMatch("PutObjectTagging"));
        assertTrue(matcher.isMatch("ListBucket"));
        assertFalse(matcher.isMatch("GetObjectTagging"));
        assertFalse(matcher.isMatch("DeleteObject"));
        assertFalse(matcher.isMatch("ListAllMyBuckets"));
    }

    @Test
    void testPrefixOptimizationRedundantPrefixes() {
        // The constructor optimizes prefix patterns by removing any longer prefix that is
        // already covered by a shorter one. Here "Put*" matches anything starting with "put"
        // (lowercased), which is a superset of what "PutObject*" matches (only things starting
        // with "putobject"). So "PutObject*" is redundant and gets removed from the prefix
        // array to avoid unnecessary iteration at match time. This test proves the optimization
        // doesn't break matching — "PutBucketAcl" still matches via the surviving "Put*" prefix.
        final RangerActionListMatcher matcher = new RangerActionListMatcher(
                Arrays.asList("PutObject*", "Put*"));

        assertTrue(matcher.isMatch("PutObject"));
        assertTrue(matcher.isMatch("PutObjectTagging"));
        assertTrue(matcher.isMatch("PutBucketAcl"));
    }

    @Test
    void testPrefixOptimizationNonRedundant() {
        // "Put*" and "Get*" are independent — neither covers the other
        final RangerActionListMatcher matcher = new RangerActionListMatcher(
                Arrays.asList("Put*", "Get*"));

        assertTrue(matcher.isMatch("PutObject"));
        assertTrue(matcher.isMatch("GetObject"));
        assertFalse(matcher.isMatch("DeleteObject"));
    }

    @Test
    void testDuplicatePrefixes() {
        // Duplicate "Put*" entries should be handled gracefully
        final RangerActionListMatcher matcher = new RangerActionListMatcher(
                Arrays.asList("Put*", "Put*", "Put*"));

        assertTrue(matcher.isMatch("PutObject"));
        assertFalse(matcher.isMatch("GetObject"));
    }

    @Test
    void testBlankRequestActionBypassesRestriction() {
        final RangerActionListMatcher matcher = new RangerActionListMatcher(
                Arrays.asList("PutObject"));

        // When no action is provided, action restrictions are not enforced
        assertTrue(matcher.isMatch(null));
        assertTrue(matcher.isMatch(""));
        assertTrue(matcher.isMatch("   "));
    }

    @Test
    void testBlankEntriesInActionsIgnored() {
        final RangerActionListMatcher matcher = new RangerActionListMatcher(
                Arrays.asList("", "  ", "PutObject"));

        assertTrue(matcher.isMatch("PutObject"));
        assertFalse(matcher.isMatch("GetObject"));
    }

    @Test
    void testPrefixWildcardCaseInsensitive() {
        final RangerActionListMatcher matcher = new RangerActionListMatcher(Arrays.asList("Put*"));

        assertTrue(matcher.isMatch("PUTOBJECT"));
        assertTrue(matcher.isMatch("putobject"));
        assertTrue(matcher.isMatch("PuToBjEcT"));
    }

    @Test
    void testWildcardAmongOtherActions() {
        // If "*" appears alongside other actions, it should still match all
        final RangerActionListMatcher matcher = new RangerActionListMatcher(
                Arrays.asList("PutObject", "*", "GetObject"));

        assertTrue(matcher.isMatch("DeleteBucket"));
        assertTrue(matcher.isMatch("ListAllMyBuckets"));
        assertTrue(matcher.isMatch("PutObject"));
    }

    @Test
    void testSingleCharacterPrefix() {
        // "G*" should match anything starting with G
        final RangerActionListMatcher matcher = new RangerActionListMatcher(Arrays.asList("G*"));

        assertTrue(matcher.isMatch("GetObject"));
        assertTrue(matcher.isMatch("GetBucketAcl"));
        assertFalse(matcher.isMatch("PutObject"));
    }

    @Test
    void testAllOzoneS3Actions() {
        // Grant with both wildcards and exact actions matching Ozone S3 patterns
        final List<String> grantActions = Arrays.asList("Get*", "List*", "PutObject");
        final RangerActionListMatcher matcher = new RangerActionListMatcher(grantActions);

        // Should match
        assertTrue(matcher.isMatch("GetObject"));
        assertTrue(matcher.isMatch("GetObjectTagging"));
        assertTrue(matcher.isMatch("GetBucketAcl"));
        assertTrue(matcher.isMatch("ListBucket"));
        assertTrue(matcher.isMatch("ListAllMyBuckets"));
        assertTrue(matcher.isMatch("ListBucketMultipartUploads"));
        assertTrue(matcher.isMatch("ListMultipartUploadParts"));
        assertTrue(matcher.isMatch("PutObject"));

        // Should not match
        assertFalse(matcher.isMatch("PutObjectTagging"));
        assertFalse(matcher.isMatch("PutBucketAcl"));
        assertFalse(matcher.isMatch("DeleteObject"));
        assertFalse(matcher.isMatch("DeleteObjectTagging"));
        assertFalse(matcher.isMatch("DeleteBucket"));
        assertFalse(matcher.isMatch("CreateBucket"));
        assertFalse(matcher.isMatch("AbortMultipartUpload"));
    }
}
