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

package org.apache.ranger.pdp.security;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ranger.pdp.config.RangerPdpConstants;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Authenticates requests using Kerberos SPNEGO (HTTP Negotiate).
 *
 * <p>Uses the JDK's built-in GSSAPI/JGSS support – no external Kerberos library is required.
 * The service principal and keytab must be configured via:
 * <ul>
 *   <li>{@code ranger.pdp.kerberos.spnego.principal} – e.g. {@code HTTP/host.example.com@REALM}
 *   <li>{@code ranger.pdp.kerberos.spnego.keytab}    – absolute path to the keytab file
 *   <li>{@code hadoop.security.auth_to_local} – Hadoop-style name rules (default: {@code DEFAULT})
 * </ul>
 *
 * <p>Authentication flow:
 * <ol>
 *   <li>If no {@code Authorization: Negotiate} header is present the handler returns {@code SKIP}.
 *   <li>The SPNEGO token is extracted, validated via GSSAPI, and – if a response token is
 *       produced (mutual authentication) – written to {@code WWW-Authenticate: Negotiate <token>}.
 *   <li>On success the short-form principal name (strip {@literal @REALM} and host components)
 *       is returned as the authenticated user.
 *   <li>On failure a {@code 401 Negotiate} challenge is sent and {@code CHALLENGE} is returned.
 * </ol>
 */
public class KerberosAuthNHandler implements PdpAuthNHandler {
    private static final Logger LOG = LoggerFactory.getLogger(KerberosAuthNHandler.class);

    public static final String AUTH_TYPE = "KERBEROS";

    private static final String NEGOTIATE_PREFIX    = "Negotiate ";
    private static final String WWW_AUTHENTICATE    = "WWW-Authenticate";
    private static final String AUTHORIZATION       = "Authorization";
    private static final Oid    SPNEGO_OID;
    private static final Oid    KRB5_OID;

    static {
        try {
            SPNEGO_OID = new Oid("1.3.6.1.5.5.2");
            KRB5_OID   = new Oid("1.2.840.113554.1.2.2");
        } catch (GSSException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private Subject       serviceSubject;
    private GSSManager    gssManager;
    private GSSCredential serverCred;

    @Override
    public void init(Properties config) throws Exception {
        String principal     = config.getProperty(RangerPdpConstants.PROP_AUTHN_KERBEROS_SPNEGO_PRINCIPAL);
        String keytab        = config.getProperty(RangerPdpConstants.PROP_AUTHN_KERBEROS_SPNEGO_KEYTAB);
        String nameRules     = config.getProperty(RangerPdpConstants.PROP_AUTHN_KERBEROS_NAME_RULES, "DEFAULT");
        String tokenValidity = config.getProperty(RangerPdpConstants.PROP_AUTHN_KERBEROS_KRB_TOKEN_VALIDITY);

        int tokenLifetime = StringUtils.isBlank(tokenValidity) ? GSSCredential.INDEFINITE_LIFETIME : Integer.parseInt(tokenValidity);

        if (StringUtils.isBlank(principal) || StringUtils.isBlank(keytab)) {
            throw new IllegalArgumentException("Kerberos auth requires configurations " + RangerPdpConstants.PROP_AUTHN_KERBEROS_SPNEGO_PRINCIPAL + " and " + RangerPdpConstants.PROP_AUTHN_KERBEROS_SPNEGO_KEYTAB);
        }

        serviceSubject = loginWithKeytab(principal, keytab);

        initializeKerberosNameRules(nameRules);

        gssManager = GSSManager.getInstance();

        GSSName serverName = gssManager.createName(principal, GSSName.NT_USER_NAME);

        // Create acceptor credentials in the logged-in Subject to avoid fallback to JVM default keytab.
        serverCred = Subject.doAs(serviceSubject, (PrivilegedExceptionAction<GSSCredential>) () ->
                gssManager.createCredential(serverName, tokenLifetime, new Oid[] {SPNEGO_OID, KRB5_OID}, GSSCredential.ACCEPT_ONLY));

        LOG.info("KerberosAuthHandler initialized; principal={} (bound acceptor credential to configured principal)", principal);
    }

    @Override
    public Result authenticate(HttpServletRequest request, HttpServletResponse response) {
        String authHeader = request.getHeader(AUTHORIZATION);

        if (authHeader == null || !authHeader.startsWith(NEGOTIATE_PREFIX)) {
            return Result.skip();
        }

        byte[] inputToken = Base64.decodeBase64(authHeader.substring(NEGOTIATE_PREFIX.length()).trim());

        try {
            return Subject.doAs(serviceSubject, (PrivilegedExceptionAction<Result>) () -> validateSpnegoToken(inputToken, response));
        } catch (Exception e) {
            LOG.warn("authenticate(): SPNEGO validation error", e);

            sendUnauthorized(response, null);

            return Result.challenge();
        }
    }

    @Override
    public String getChallengeHeader() {
        return NEGOTIATE_PREFIX.trim();
    }

    private Result validateSpnegoToken(byte[] inputToken, HttpServletResponse response) throws GSSException {
        GSSContext gssCtx = gssManager.createContext(serverCred);

        try {
            byte[] outputToken = gssCtx.acceptSecContext(inputToken, 0, inputToken.length);

            if (outputToken != null && outputToken.length > 0) {
                response.addHeader(WWW_AUTHENTICATE, NEGOTIATE_PREFIX + Base64.encodeBase64String(outputToken));
            }

            if (!gssCtx.isEstablished()) {
                sendUnauthorized(response, outputToken);

                return Result.challenge();
            }

            String principal = gssCtx.getSrcName().toString();
            String userName  = applyNameRules(principal);

            LOG.debug("authenticate(): SPNEGO success, principal={}, user={}", principal, userName);

            return Result.authenticated(userName, AUTH_TYPE);
        } finally {
            try {
                gssCtx.dispose();
            } catch (GSSException excp) {
                LOG.debug("GSSContext.dispose() failed. Ignored", excp);
            }
        }
    }

    private void sendUnauthorized(HttpServletResponse response, byte[] outputToken) {
        if (!response.isCommitted()) {
            if (outputToken != null && outputToken.length > 0) {
                response.setHeader(WWW_AUTHENTICATE, NEGOTIATE_PREFIX + Base64.encodeBase64String(outputToken));
            } else {
                response.setHeader(WWW_AUTHENTICATE, NEGOTIATE_PREFIX.trim());
            }

            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    /**
     * Applies configured auth_to_local rules.
     *
     * <p>Uses Hadoop {@link KerberosName} with configured auth_to_local rules.
     * Falls back to DEFAULT short-name mapping only if transformation fails.
     */
    private String applyNameRules(String principal) {
        try {
            String shortName = new KerberosName(principal).getShortName();

            if (StringUtils.isNotBlank(shortName)) {
                return shortName;
            }
        } catch (Exception e) {
            LOG.warn("Failed KerberosName transformation for principal '{}'; using DEFAULT short-name mapping", principal, e);
        }

        return defaultShortName(principal);
    }

    private void initializeKerberosNameRules(String configuredRules) {
        String effectiveRules = StringUtils.defaultIfBlank(configuredRules, "DEFAULT").trim();

        KerberosName.setRules(effectiveRules);

        LOG.info("Initialized Kerberos name rules: {}='{}'", RangerPdpConstants.PROP_AUTHN_KERBEROS_NAME_RULES, effectiveRules);
    }

    private String defaultShortName(String principal) {
        String name   = principal;
        int    atSign = name.indexOf('@');

        if (atSign >= 0) {
            name = name.substring(0, atSign);
        }

        int slash = name.indexOf('/');

        if (slash >= 0) {
            name = name.substring(0, slash);
        }

        return name;
    }

    private Subject loginWithKeytab(String principal, String keytab) throws LoginException {
        Configuration jaasConfig = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                Map<String, String> opts = new HashMap<>();

                opts.put("useKeyTab", "true");
                opts.put("storeKey", "true");
                opts.put("doNotPrompt", "true");
                opts.put("useTicketCache", "false");
                opts.put("keyTab", keytab);
                opts.put("principal", principal);
                opts.put("refreshKrb5Config", "true");
                opts.put("isInitiator", "false");

                return new AppConfigurationEntry[] {
                    new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, opts)
                };
            }
        };

        LoginContext lc = new LoginContext("Ranger-PDP-Kerberos", null, noOpCallbackHandler(), jaasConfig);

        lc.login();

        return lc.getSubject();
    }

    private static CallbackHandler noOpCallbackHandler() {
        return (Callback[] callbacks) -> {
            for (Callback cb : callbacks) {
                LOG.warn("Unexpected JAAS callback: {}", cb.getClass().getName());

                throw new UnsupportedCallbackException(cb);
            }
        };
    }
}
