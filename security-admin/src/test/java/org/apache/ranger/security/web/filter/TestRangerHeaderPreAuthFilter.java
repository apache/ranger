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
import org.apache.ranger.entity.XXPortalUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
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
    public void testDoFilter_disabled_bypassesHeaderAuthentication() throws Exception {
        RangerHeaderPreAuthFilter filter = new RangerHeaderPreAuthFilter();
        UserMgr                   userMgr = mock(UserMgr.class);

        filter.userMgr = userMgr;

        HttpServletRequest  request  = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain         chain    = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        verify(userMgr, never()).findByLoginId(Mockito.anyString());
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    public void testDoFilter_enabled_setsAuthenticationFromRangerDbRoles() throws Exception {
        PropertiesUtil.getPropertiesMap().put(RangerHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED, "true");

        RangerHeaderPreAuthFilter filter = new RangerHeaderPreAuthFilter();
        UserMgr                   userMgr = mock(UserMgr.class);
        XXPortalUser              portalUser = new XXPortalUser();

        portalUser.setLoginId("joeuser");
        filter.userMgr = userMgr;

        when(userMgr.findByLoginId("joeuser")).thenReturn(portalUser);
        when(userMgr.getRolesByLoginId("joeuser")).thenReturn(Arrays.asList("ROLE_SYS_ADMIN", "ROLE_USER"));

        HttpServletRequest  request  = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(request.getHeader("x-awc-username")).thenReturn("joeuser");
        when(request.getHeader("x-awc-requestid")).thenReturn("03e3f8a4-b8e5-4c61-8ff5-b50236a05eea");

        FilterChain chain = new FilterChain() {
            @Override
            public void doFilter(ServletRequest req, ServletResponse res) {
                assertNotNull(SecurityContextHolder.getContext().getAuthentication());
                assertEquals("joeuser", SecurityContextHolder.getContext().getAuthentication().getName());
                Collection<?> authorities = SecurityContextHolder.getContext().getAuthentication().getAuthorities();
                assertEquals(2, authorities.size());
                assertTrue(authorities.stream().anyMatch(a -> "ROLE_SYS_ADMIN".equals(a.toString())));
                assertTrue(authorities.stream().anyMatch(a -> "ROLE_USER".equals(a.toString())));
            }
        };

        filter.doFilter(request, response, chain);
    }

    @Test
    public void testDoFilter_enabled_missingRequestIdReturnsUnauthorized() throws Exception {
        PropertiesUtil.getPropertiesMap().put(RangerHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED, "true");

        RangerHeaderPreAuthFilter filter = new RangerHeaderPreAuthFilter();
        filter.userMgr = mock(UserMgr.class);

        HttpServletRequest  request  = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain         chain    = mock(FilterChain.class);
        when(request.getHeader("x-awc-username")).thenReturn("joeuser");

        filter.doFilter(request, response, chain);

        verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Missing trusted request-id header");
        verify(chain, never()).doFilter(request, response);
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    public void testDoFilter_enabled_missingUsernameReturnsUnauthorized() throws Exception {
        PropertiesUtil.getPropertiesMap().put(RangerHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED, "true");

        RangerHeaderPreAuthFilter filter = new RangerHeaderPreAuthFilter();
        filter.userMgr = mock(UserMgr.class);

        HttpServletRequest  request  = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain         chain    = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Missing trusted username header");
        verify(chain, never()).doFilter(request, response);
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    public void testDoFilter_enabled_unknownUserReturnsUnauthorized() throws Exception {
        PropertiesUtil.getPropertiesMap().put(RangerHeaderPreAuthFilter.PROP_HEADER_AUTH_ENABLED, "true");

        RangerHeaderPreAuthFilter filter = new RangerHeaderPreAuthFilter();
        UserMgr                   userMgr = mock(UserMgr.class);

        filter.userMgr = userMgr;

        HttpServletRequest  request  = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain         chain    = mock(FilterChain.class);
        when(request.getHeader("x-awc-username")).thenReturn("joeuser");
        when(request.getHeader("x-awc-requestid")).thenReturn("03e3f8a4-b8e5-4c61-8ff5-b50236a05eea");

        filter.doFilter(request, response, chain);

        verify(userMgr).findByLoginId("joeuser");
        verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unknown Ranger user");
        verify(chain, never()).doFilter(request, response);
    }
}
