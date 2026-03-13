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

import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.Collection;

public class RangerAuthenticationToken extends AbstractAuthenticationToken {
    private static final long serialVersionUID = 1L;

    /**
     * Identifies which Ranger authentication mechanism produced this token.
     */
    public enum AuthMechanism {
        /** Trusted-proxy header auth via RangerHeaderPreAuthFilter */
        HEADER
    }

    private final Object        principal;
    private final AuthMechanism authMechanism;
    private final int           authType;
    private final String        requestId;

    public RangerAuthenticationToken(UserDetails principal, Collection<? extends GrantedAuthority> authorities, int authType, AuthMechanism authMechanism) {
        this(principal, authorities, authType, authMechanism, null);
    }

    public RangerAuthenticationToken(UserDetails principal, Collection<? extends GrantedAuthority> authorities, int authType, AuthMechanism authMechanism, String requestId) {
        super(authorities);

        this.principal     = principal;
        this.authType      = authType;
        this.authMechanism = authMechanism;
        this.requestId     = requestId;

        super.setAuthenticated(true);
    }

    @Override
    public Object getPrincipal() {
        return principal;
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    /**
     * The Ranger authentication mechanism that produced this token.
     * Use this to distinguish header auth from SSO, Kerberos, etc.
     */
    public AuthMechanism getAuthMechanism() {
        return authMechanism;
    }

    /**
     * Maps to XXAuthSession.AUTH_TYPE_* for session/audit recording.
     */
    public int getAuthType() {
        return authType;
    }

    /**
     * The upstream request ID. Populated only for HEADER auth tokens.
     */
    public String getRequestId() {
        return requestId;
    }
}
