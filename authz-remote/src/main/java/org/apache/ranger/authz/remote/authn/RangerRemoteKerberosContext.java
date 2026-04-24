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

package org.apache.ranger.authz.remote.authn;

import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.remote.RangerRemoteAuthzConfig;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.KERBEROS_LOGIN_FAILED;

public final class RangerRemoteKerberosContext {
    private static final String CRED_CONF_NAME = "RangerRemoteClientKerberos";
    private static final Oid    SPNEGO_OID     = getSpnegoOid();

    private final Subject                      subject;
    private final CredentialsProvider          credentialsProvider;
    private final Lookup<AuthSchemeProvider>   authSchemeRegistry;

    private RangerRemoteKerberosContext(Subject subject, CredentialsProvider credentialsProvider, Lookup<AuthSchemeProvider> authSchemeRegistry) {
        this.subject             = subject;
        this.credentialsProvider = credentialsProvider;
        this.authSchemeRegistry  = authSchemeRegistry;
    }

    public static RangerRemoteKerberosContext create(RangerRemoteAuthzConfig config) throws RangerAuthzException {
        String principal = config.getKerberosPrincipal();
        String keytab    = config.getKerberosKeytab();

        try {
            final GSSManager gssManager = GSSManager.getInstance();
            final Subject    subject    = login(principal, keytab, config.isKerberosDebugEnabled());
            final GSSName    userName   = gssManager.createName(principal, GSSName.NT_USER_NAME);
            final GSSCredential credential = Subject.doAs(subject, (PrivilegedExceptionAction<GSSCredential>) () ->
                    gssManager.createCredential(userName, GSSCredential.DEFAULT_LIFETIME, SPNEGO_OID, GSSCredential.INITIATE_ONLY));

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthSchemes.SPNEGO),
                    new KerberosCredentials(credential));

            Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create()
                    .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true, true))
                    .build();

            return new RangerRemoteKerberosContext(subject, credentialsProvider, authSchemeRegistry);
        } catch (Exception e) {
            throw new RangerAuthzException(KERBEROS_LOGIN_FAILED, e, config.getPdpUrl());
        }
    }

    public <T> T doAs(PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        return Subject.doAs(subject, action);
    }

    public void applyToHttpClientBuilder(HttpClientBuilder builder) {
        builder.setDefaultCredentialsProvider(credentialsProvider);
        builder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
    }

    private static Subject login(String principal, String keytab, boolean enableDebug) throws LoginException {
        Subject subject = new Subject(false, Collections.singleton(new KerberosPrincipal(principal)), Collections.emptySet(), Collections.emptySet());
        Configuration config = new KeytabJaasConf(principal, keytab, enableDebug);
        LoginContext loginContext = new LoginContext(CRED_CONF_NAME, subject, noOpCallbackHandler(), config);

        loginContext.login();

        return loginContext.getSubject();
    }

    private static Oid getSpnegoOid() {
        try {
            return new Oid("1.3.6.1.5.5.2");
        } catch (GSSException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static CallbackHandler noOpCallbackHandler() {
        return callbacks -> {
            for (Callback callback : callbacks) {
                throw new UnsupportedCallbackException(callback);
            }
        };
    }

    private abstract static class AbstractJaasConf extends Configuration {
        private final String  userPrincipalName;
        private final boolean enableDebugLogs;

        AbstractJaasConf(String userPrincipalName, boolean enableDebugLogs) {
            this.userPrincipalName = userPrincipalName;
            this.enableDebugLogs   = enableDebugLogs;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options = new HashMap<>();

            options.put("principal", userPrincipalName);
            options.put("isInitiator", Boolean.TRUE.toString());
            options.put("storeKey", Boolean.TRUE.toString());
            options.put("debug", Boolean.toString(enableDebugLogs));

            addOptions(options);

            return new AppConfigurationEntry[] {
                    new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                            Collections.unmodifiableMap(options))
            };
        }

        abstract void addOptions(Map<String, String> options);
    }

    private static final class KeytabJaasConf extends AbstractJaasConf {
        private final String keytabFilePath;

        KeytabJaasConf(String userPrincipalName, String keytabFilePath, boolean enableDebugLogs) {
            super(userPrincipalName, enableDebugLogs);
            this.keytabFilePath = keytabFilePath;
        }

        @Override
        void addOptions(Map<String, String> options) {
            options.put("useKeyTab", Boolean.TRUE.toString());
            options.put("keyTab", keytabFilePath);
            options.put("doNotPrompt", Boolean.TRUE.toString());
            options.put("useTicketCache", Boolean.FALSE.toString());
            options.put("refreshKrb5Config", Boolean.TRUE.toString());
        }
    }
}
