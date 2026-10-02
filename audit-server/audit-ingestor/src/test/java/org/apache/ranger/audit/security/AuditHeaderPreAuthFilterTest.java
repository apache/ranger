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

package org.apache.ranger.audit.security;

import org.apache.ranger.audit.server.AuditServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AuditHeaderPreAuthFilterTest {
    private static final String USERNAME_HEADER = "X-Forwarded-User";
    private static final String SPIFFE_HEADER   = "X-Spiffe-Id";

    private HttpServletRequest  request;
    private HttpServletResponse response;
    private FilterChain         chain;

    @BeforeEach
    void setUp() {
        SecurityContextHolder.clearContext();

        request  = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);
        chain    = mock(FilterChain.class);
    }

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();

        AuditServerConfig.getInstance().unset(AuditHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED);
        AuditServerConfig.getInstance().unset(AuditHeaderPreAuthFilter.PROP_USERNAME_HEADER_NAME);
        AuditServerConfig.getInstance().unset(AuditHeaderPreAuthFilter.PROP_SPIFFE_HEADER_NAME);
    }

    @Test
    void testDoFilter_disabled_passesThrough() throws Exception {
        AuditHeaderPreAuthFilter filter = newFilter("false", USERNAME_HEADER, SPIFFE_HEADER);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        verify(request, never()).getHeader(anyString());
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    void testDoFilter_enabledWithoutHeaderNames_isDisabled() throws Exception {
        AuditHeaderPreAuthFilter filter = newFilter("true", " ", " ");

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        verify(request, never()).getHeader(anyString());
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    void testDoFilter_enabled_missingUsername_passesThrough() throws Exception {
        AuditHeaderPreAuthFilter filter = newFilter("true", USERNAME_HEADER, null);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        verify(request).getHeader(USERNAME_HEADER);
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    void testDoFilter_enabled_blankUsername_passesThrough() throws Exception {
        AuditHeaderPreAuthFilter filter = newFilter("true", USERNAME_HEADER, null);

        when(request.getHeader(USERNAME_HEADER)).thenReturn("   ");

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    void testDoFilter_enabled_withUsername_setsAuthentication() throws Exception {
        AuditHeaderPreAuthFilter filter = newFilter("true", USERNAME_HEADER, null);

        when(request.getHeader(USERNAME_HEADER)).thenReturn(" hdfs ");

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        assertAuthenticatedAs("hdfs");
    }

    @Test
    void testDoFilter_enabled_withSpiffeHeader_setsSpiffeIdAuthentication() throws Exception {
        AuditHeaderPreAuthFilter filter   = newFilter("true", USERNAME_HEADER, SPIFFE_HEADER);
        String                   spiffeId = "spiffe://prod-cluster.k8s.example.com/ns/hadoop/sa/hdfs";

        when(request.getHeader(SPIFFE_HEADER)).thenReturn(spiffeId);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        assertAuthenticatedAs(spiffeId);
    }

    @Test
    void testDoFilter_enabled_spiffeOnlyConfig_setsSpiffeIdAuthentication() throws Exception {
        AuditHeaderPreAuthFilter filter   = newFilter("true", null, SPIFFE_HEADER);
        String                   spiffeId = "spiffe://example.org/workload/hive";

        when(request.getHeader(SPIFFE_HEADER)).thenReturn(spiffeId);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        verify(request, never()).getHeader(USERNAME_HEADER);
        assertAuthenticatedAs(spiffeId);
    }

    @Test
    void testDoFilter_enabled_usernameTakesPrecedenceOverSpiffe() throws Exception {
        AuditHeaderPreAuthFilter filter = newFilter("true", USERNAME_HEADER, SPIFFE_HEADER);

        when(request.getHeader(USERNAME_HEADER)).thenReturn("hdfs");

        filter.doFilter(request, response, chain);

        verify(request, never()).getHeader(SPIFFE_HEADER);
        assertAuthenticatedAs("hdfs");
    }

    @Test
    void testDoFilter_enabled_multipleSpiffeHeaders_usesFirstValid() throws Exception {
        AuditHeaderPreAuthFilter filter   = newFilter("true", null, SPIFFE_HEADER + ", x-awc-upstream-workload-id");
        String                   spiffeId = "spiffe://my-cluster/ns/service-namespace/sa/service-sa";

        when(request.getHeader(SPIFFE_HEADER)).thenReturn("not-a-spiffe-id");
        when(request.getHeader("x-awc-upstream-workload-id")).thenReturn(spiffeId);

        filter.doFilter(request, response, chain);

        assertAuthenticatedAs(spiffeId);
    }

    @Test
    void testDoFilter_enabled_malformedSpiffeHeader_passesThrough() throws Exception {
        AuditHeaderPreAuthFilter filter = newFilter("true", USERNAME_HEADER, SPIFFE_HEADER);

        // whitespace is not an allowed SPIFFE ID character
        when(request.getHeader(SPIFFE_HEADER)).thenReturn("spiffe://my-cluster/ns/prod/sa/service sa");

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    void testDoFilter_enabled_existingAuthenticatedContext_doesNotOverrideAuthentication() throws Exception {
        AuditHeaderPreAuthFilter            filter       = newFilter("true", USERNAME_HEADER, SPIFFE_HEADER);
        UsernamePasswordAuthenticationToken existingAuth = new UsernamePasswordAuthenticationToken("kerberos-user", "", Collections.singletonList(new SimpleGrantedAuthority("ROLE_USER")));

        SecurityContextHolder.getContext().setAuthentication(existingAuth);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        verify(request, never()).getHeader(anyString());
        assertSame(existingAuth, SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    void testDoFilter_enabled_existingUnauthenticatedContext_setsAuthentication() throws Exception {
        AuditHeaderPreAuthFilter filter = newFilter("true", USERNAME_HEADER, null);

        SecurityContextHolder.getContext().setAuthentication(new UsernamePasswordAuthenticationToken("unverified-user", ""));

        when(request.getHeader(USERNAME_HEADER)).thenReturn("hive");

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        assertAuthenticatedAs("hive");
    }

    private static void assertAuthenticatedAs(String expectedUser) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();

        assertNotNull(auth);
        assertTrue(auth.isAuthenticated());
        assertEquals(expectedUser, auth.getName());
        assertEquals(1, auth.getAuthorities().size());
        assertTrue(auth.getAuthorities().stream().anyMatch(a -> "ROLE_USER".equals(a.getAuthority())));
    }

    private static AuditHeaderPreAuthFilter newFilter(String enabled, String userNameHeaderName, String spiffeHeaderNames) {
        AuditServerConfig config = AuditServerConfig.getInstance();

        config.set(AuditHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED, enabled);

        if (userNameHeaderName != null) {
            config.set(AuditHeaderPreAuthFilter.PROP_USERNAME_HEADER_NAME, userNameHeaderName);
        } else {
            config.unset(AuditHeaderPreAuthFilter.PROP_USERNAME_HEADER_NAME);
        }

        if (spiffeHeaderNames != null) {
            config.set(AuditHeaderPreAuthFilter.PROP_SPIFFE_HEADER_NAME, spiffeHeaderNames);
        } else {
            config.unset(AuditHeaderPreAuthFilter.PROP_SPIFFE_HEADER_NAME);
        }

        AuditHeaderPreAuthFilter ret = new AuditHeaderPreAuthFilter();

        ret.initialize();

        return ret;
    }
}
