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

import org.apache.ranger.biz.SessionMgr;
import org.apache.ranger.common.RangerConstants;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Concurrent-session expiry handling for RangerKRBAuthenticationFilter (RANGER-5749).
 * This class is new on ranger-2.10; master already had a broader filter test suite.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestRangerKRBAuthenticationFilter {
    @Test
    public void testDoFilter_concurrentSessionExpiredSso_redirectsToKnox() throws Exception {
        RangerKRBAuthenticationFilter filter = new RangerKRBAuthenticationFilter();

        HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
        FilterChain chain = Mockito.mock(FilterChain.class);
        HttpSession session = Mockito.mock(HttpSession.class);

        when(res.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
        when(req.getSession(false)).thenReturn(session);
        when(session.getAttribute(SessionMgr.SESSION_ATTR_CONCURRENT_EXPIRED)).thenReturn(Boolean.TRUE);
        when(session.getAttribute(SessionMgr.SESSION_ATTR_CONCURRENT_EXPIRED_SSO)).thenReturn(Boolean.TRUE);
        when(req.getRequestedSessionId()).thenReturn("sid");
        when(req.getRequestURI()).thenReturn("/index.html");
        when(req.getRequestURL()).thenReturn(new StringBuffer("http://localhost/index.html"));
        when(req.getHeaderNames()).thenReturn(Collections.emptyEnumeration());
        doNothing().when(res).sendRedirect(Mockito.anyString());
        doNothing().when(session).invalidate();

        filter.doFilter(req, res, chain);

        verify(res, times(1)).sendRedirect(Mockito.anyString());
        verify(session, times(1)).invalidate();
        verify(chain, never()).doFilter(any(ServletRequest.class), any(ServletResponse.class));
    }

    @Test
    public void testDoFilter_concurrentSessionExpiredFormLogin_redirectsToRangerLogin() throws Exception {
        RangerKRBAuthenticationFilter filter = new RangerKRBAuthenticationFilter();

        HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
        FilterChain chain = Mockito.mock(FilterChain.class);
        HttpSession session = Mockito.mock(HttpSession.class);

        when(req.getSession(false)).thenReturn(session);
        when(session.getAttribute(SessionMgr.SESSION_ATTR_CONCURRENT_EXPIRED)).thenReturn(Boolean.TRUE);
        when(session.getAttribute(SessionMgr.SESSION_ATTR_CONCURRENT_EXPIRED_SSO)).thenReturn(Boolean.FALSE);
        when(req.getContextPath()).thenReturn("");
        doNothing().when(res).sendRedirect(Mockito.anyString());
        doNothing().when(session).invalidate();

        filter.doFilter(req, res, chain);

        verify(session, times(1)).invalidate();
        verify(res, times(1)).sendRedirect("/login.jsp");
        verify(chain, never()).doFilter(any(ServletRequest.class), any(ServletResponse.class));
    }

    @Test
    public void testDoFilter_concurrentSessionExpiredFormLogin_ajaxReturnsTimeout() throws Exception {
        RangerKRBAuthenticationFilter filter = new RangerKRBAuthenticationFilter();

        HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
        FilterChain chain = Mockito.mock(FilterChain.class);
        HttpSession session = Mockito.mock(HttpSession.class);

        when(req.getSession(false)).thenReturn(session);
        when(session.getAttribute(SessionMgr.SESSION_ATTR_CONCURRENT_EXPIRED)).thenReturn(Boolean.TRUE);
        when(session.getAttribute(SessionMgr.SESSION_ATTR_CONCURRENT_EXPIRED_SSO)).thenReturn(Boolean.FALSE);
        when(req.getContextPath()).thenReturn("");
        when(req.getHeader("X-Requested-With")).thenReturn("XMLHttpRequest");
        doNothing().when(session).invalidate();

        filter.doFilter(req, res, chain);

        verify(session, times(1)).invalidate();
        verify(res).setStatus(RangerConstants.SC_AUTHENTICATION_TIMEOUT);
        verify(res).setHeader("X-Rngr-Redirect-Url", "/login.jsp");
        verify(res, never()).sendRedirect(Mockito.anyString());
        verify(chain, never()).doFilter(any(ServletRequest.class), any(ServletResponse.class));
    }
}
