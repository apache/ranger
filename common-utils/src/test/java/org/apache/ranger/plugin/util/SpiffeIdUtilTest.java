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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpiffeIdUtilTest {
    private static final String VALID = "spiffe://my-cluster/ns/service-namespace/sa/service-sa";

    @Test
    void isValidSpiffeId_acceptsExactFormat() {
        assertTrue(SpiffeIdUtil.isValidSpiffeId(VALID));
        assertTrue(SpiffeIdUtil.isValidSpiffeId("  " + VALID + "  ")); // trimmed
    }

    @Test
    void isValidSpiffeId_rejectsMalformed() {
        assertFalse(SpiffeIdUtil.isValidSpiffeId(null));
        assertFalse(SpiffeIdUtil.isValidSpiffeId(""));
        assertFalse(SpiffeIdUtil.isValidSpiffeId("   "));
        assertFalse(SpiffeIdUtil.isValidSpiffeId("not-a-spiffe-id"));
        assertFalse(SpiffeIdUtil.isValidSpiffeId("https://my-cluster/ns/n/sa/s"));   // wrong scheme
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster"));            // trust domain only, no path
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/"));           // trailing slash, empty path
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns//sa/s"));   // empty path segment
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe:///ns/n/sa/s"));            // empty trust domain
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/n/sa/"));   // empty trailing segment
        assertFalse(SpiffeIdUtil.isValidSpiffeId(VALID + "/"));                      // trailing slash
    }

    @Test
    void isValidSpiffeId_rejectsUnsafeOrIllegalCharacters() {
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/prod/sa/service sa"));   // embedded space
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/prod/sa/service\tsa"));  // embedded tab
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/prod/sa/service\nsa"));  // embedded newline
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/prod/sa/svc@admin"));    // illegal char '@'
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://My-Cluster/ns/prod/sa/service-sa"));   // trust domain must be lowercase per spec
    }

    @Test
    void isValidSpiffeId_rejectsRelativePathModifierSegments() {
        // Per SPIFFE spec 2.2, path segments must not be the relative modifiers '.' or '..'.
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/./sa/service-sa"));   // namespace '.'
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/../sa/service-sa"));  // namespace '..'
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/prod/sa/."));         // service-account '.'
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/prod/sa/.."));        // service-account '..'

        // Dots inside a segment (not a bare '.'/'..') remain valid.
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/prod/sa/svc.v1"));
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/..prod/sa/svc.v1"));
    }

    @Test
    void isValidSpiffeId_acceptsSpecAllowedCharacters() {
        // SPIFFE path segments are case-sensitive and allow letters (any case), digits, '.', '-', '_'.
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster.example.com/ns/service-namespace/sa/service-sa"));
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/Prod/sa/Service-SA")); // mixed case
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://cluster1/ns/ns_2/sa/svc_01"));       // underscore
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://cluster1/ns/ns-2/sa/svc-01"));       // hyphen/digits
    }

    @Test
    void isValidSpiffeId_handlesRealisticProductionIds() {
        // Kubernetes trust domain with a DNS-style cluster name and typical namespace/service-account names.
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://prod-cluster.k8s.example.com/ns/ingress-nginx/sa/nginx-ingress"));

        // Cloud/mesh style trust domain (e.g. the header value from the PR description).
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/service-namespace/sa/service-sa"));

        // Istio-style default service account in an application namespace.
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://cluster.local/ns/payments/sa/default"));

        // Namespace and service-account with digits and hyphens, as commonly generated by platforms.
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://data-plane.internal/ns/team-analytics-42/sa/spark-driver-01"));
    }

    @Test
    void acceptsSpecValidNonSpireLayoutSpiffeId() {
        // Well-formed SPIFFE IDs per the spec (valid scheme + trust domain + one or more path segments)
        // that do not follow the SPIRE spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>
        // convention. Since the full SPIFFE ID is used as the principal, these must be accepted.
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://example.org/workload/frontend")); // arbitrary path
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://example.org/ns/prod"));           // /ns/ but no /sa/
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/sa/service-sa"));      // /sa/ but no /ns/
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/n"));               // single-segment style
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://example.org/single"));            // single path segment
        assertTrue(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/sa/s/ns/n"));          // segments in any order
        assertTrue(SpiffeIdUtil.isValidSpiffeId(VALID + "/extra"));                         // additional trailing segment
    }

    @Test
    void parseHeaderNames_handlesSingleAndMultiple() {
        assertTrue(SpiffeIdUtil.parseHeaderNames(null).isEmpty());
        assertTrue(SpiffeIdUtil.parseHeaderNames("   ").isEmpty());

        assertEquals(List.of("x-spiffe"), SpiffeIdUtil.parseHeaderNames("x-spiffe"));

        List<String> names = SpiffeIdUtil.parseHeaderNames(" x-source , x-upstream ,");

        assertEquals(List.of("x-source", "x-upstream"), names);
    }
}
