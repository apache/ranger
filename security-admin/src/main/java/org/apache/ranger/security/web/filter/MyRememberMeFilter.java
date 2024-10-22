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

 /**
 *
 */
package org.apache.ranger.security.web.filter;

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.RememberMeServices;
import org.springframework.security.web.authentication.rememberme.RememberMeAuthenticationFilter;

/**
 *
 *
 */
@SuppressWarnings("deprecation")
public class MyRememberMeFilter extends RememberMeAuthenticationFilter {

    public MyRememberMeFilter(AuthenticationManager authenticationManager, RememberMeServices rememberMeServices) {
		super(authenticationManager, rememberMeServices);
	}

    private static final Logger LOG = LoggerFactory.getLogger(MyRememberMeFilter.class);

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.security.web.authentication.rememberme.
     * RememberMeAuthenticationFilter#doFilter(jakarta.servlet.ServletRequest,
     * jakarta.servlet.ServletResponse, jakarta.servlet.FilterChain)
     */
    @Override
    public void doFilter(ServletRequest arg0, ServletResponse arg1,
	    FilterChain arg2) throws IOException, ServletException {
    	HttpServletResponse res = (HttpServletResponse)arg1;
    	res.setHeader("X-Frame-Options", "DENY" );
    	super.doFilter(arg0, res, arg2);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.security.web.authentication.rememberme.
     * RememberMeAuthenticationFilter
     * #onSuccessfulAuthentication(jakarta.servlet.http.HttpServletRequest,
     * jakarta.servlet.http.HttpServletResponse,
     * org.springframework.security.core.Authentication)
     */
    @Override
    protected void onSuccessfulAuthentication(HttpServletRequest request,
	    HttpServletResponse response, Authentication authResult) {
    	response.setHeader("X-Frame-Options", "DENY" );
	super.onSuccessfulAuthentication(request, response, authResult);
	LOG.info("onSuccessfulAuthentication() authResult=" + authResult);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.security.web.authentication.rememberme.
     * RememberMeAuthenticationFilter
     * #onUnsuccessfulAuthentication(jakarta.servlet.http.HttpServletRequest,
     * jakarta.servlet.http.HttpServletResponse,
     * org.springframework.security.core.AuthenticationException)
     */
    @Override
    protected void onUnsuccessfulAuthentication(HttpServletRequest request,
	    HttpServletResponse response, AuthenticationException failed) {
	LOG.error("Authentication failure. failed=" + failed,
			  new Throwable());
	response.setHeader("X-Frame-Options", "DENY" );
	super.onUnsuccessfulAuthentication(request, response, failed);
    }

}
