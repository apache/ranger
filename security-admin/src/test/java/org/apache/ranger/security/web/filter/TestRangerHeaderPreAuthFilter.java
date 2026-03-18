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
package org.apache.ranger.security.web.filter;

import org.apache.ranger.biz.UserMgr;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.entity.XXAuthSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestRangerHeaderPreAuthFilter {
    @BeforeEach
    public void setUp() {
        SecurityContextHolder.clearContext();
    }

    @AfterEach
    public void tearDown() {
        SecurityContextHolder.clearContext();

        PropertiesUtil.getPropertiesMap().remove(RangerHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED);
        PropertiesUtil.getPropertiesMap().remove(RangerHeaderPreAuthFilter.PROP_USERNAME_HEADER_NAME);
        PropertiesUtil.getPropertiesMap().remove(RangerHeaderPreAuthFilter.PROP_REQUEST_ID_HEADER_NAME);
    }

    @Test
    public void testDoFilter_disabled_passesThrough() throws Exception {
        RangerHeaderPreAuthFilter filter  = new RangerHeaderPreAuthFilter();
        UserMgr                   userMgr = mock(UserMgr.class);

        filter.userMgr = userMgr;
        filter.initialize(null);

        HttpServletRequest  request  = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain         chain    = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        verify(userMgr, never()).getRolesByLoginId(anyString());
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    public void testDoFilter_enabled_missingUsername_passesThrough() throws Exception {
        PropertiesUtil.getPropertiesMap().put(RangerHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED, "true");
        PropertiesUtil.getPropertiesMap().put(RangerHeaderPreAuthFilter.PROP_USERNAME_HEADER_NAME, "x-awc-username");

        RangerHeaderPreAuthFilter filter  = new RangerHeaderPreAuthFilter();
        UserMgr                   userMgr = mock(UserMgr.class);

        filter.userMgr = userMgr;
        filter.initialize(null);

        HttpServletRequest  request  = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain         chain    = mock(FilterChain.class);

        // no username header — getHeader returns null by default

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        verify(userMgr, never()).getRolesByLoginId(anyString());
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    public void testDoFilter_enabled_withUsername_setsAuthenticationFromRangerDbRoles() throws Exception {
        PropertiesUtil.getPropertiesMap().put(RangerHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED, "true");
        PropertiesUtil.getPropertiesMap().put(RangerHeaderPreAuthFilter.PROP_USERNAME_HEADER_NAME, "x-awc-username");

        RangerHeaderPreAuthFilter filter  = new RangerHeaderPreAuthFilter();
        UserMgr                   userMgr = mock(UserMgr.class);

        filter.userMgr = userMgr;
        filter.initialize(null);

        when(userMgr.getRolesByLoginId("joeuser")).thenReturn(Arrays.asList("ROLE_SYS_ADMIN", "ROLE_USER"));

        HttpServletRequest  request  = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);

        when(request.getHeader("x-awc-username")).thenReturn("joeuser");

        FilterChain chain = new FilterChain() {
            @Override
            public void doFilter(ServletRequest req, ServletResponse res) {
                org.springframework.security.core.Authentication auth = SecurityContextHolder.getContext().getAuthentication();

                assertNotNull(auth);
                assertTrue(auth instanceof RangerAuthenticationToken);
                RangerAuthenticationToken rangerAuth = (RangerAuthenticationToken) auth;
                assertEquals(XXAuthSession.AUTH_TYPE_TRUSTED_PROXY, rangerAuth.getAuthType());
                assertEquals("joeuser", auth.getName());

                Collection<?> authorities = auth.getAuthorities();
                assertEquals(2, authorities.size());
                assertTrue(authorities.stream().anyMatch(a -> "ROLE_SYS_ADMIN".equals(a.toString())));
                assertTrue(authorities.stream().anyMatch(a -> "ROLE_USER".equals(a.toString())));
            }
        };

        filter.doFilter(request, response, chain);
    }

    @Test
    public void testDoFilter_enabled_existingAuthenticatedContext_doesNotOverrideAuthentication() throws Exception {
        PropertiesUtil.getPropertiesMap().put(RangerHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED, "true");
        PropertiesUtil.getPropertiesMap().put(RangerHeaderPreAuthFilter.PROP_USERNAME_HEADER_NAME, "x-awc-username");

        RangerHeaderPreAuthFilter filter  = new RangerHeaderPreAuthFilter();
        UserMgr                   userMgr = mock(UserMgr.class);

        filter.userMgr = userMgr;
        filter.initialize(null);

        UsernamePasswordAuthenticationToken existingAuth = new UsernamePasswordAuthenticationToken("existing-user", "pwd");

        SecurityContextHolder.getContext().setAuthentication(existingAuth);

        HttpServletRequest  request  = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain         chain    = mock(FilterChain.class);

        when(request.getHeader("x-awc-username")).thenReturn("joeuser");

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        verify(userMgr, never()).getRolesByLoginId(anyString());
        assertEquals(existingAuth, SecurityContextHolder.getContext().getAuthentication());
    }
}
