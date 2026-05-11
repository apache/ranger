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
    private static final Oid SPNEGO_OID = getSpnegoOid();

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
            final Subject    subject    = login(config, principal, keytab);
            final GSSName    userName   = gssManager.createName(principal, GSSName.NT_USER_NAME);
            final GSSCredential credential = Subject.doAs(subject, (PrivilegedExceptionAction<GSSCredential>) () ->
                    gssManager.createCredential(userName, GSSCredential.DEFAULT_LIFETIME, SPNEGO_OID, GSSCredential.INITIATE_ONLY));

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthSchemes.SPNEGO),
                    new KerberosCredentials(credential));

            Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create()
                    .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(
                            config.isKerberosSpnegoStripPort(),
                            config.isKerberosSpnegoUseCanonicalHostname()))
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

    private static Subject login(RangerRemoteAuthzConfig config, String principal, String keytab) throws LoginException, RangerAuthzException {
        Subject subject = new Subject(false, Collections.singleton(new KerberosPrincipal(principal)), Collections.emptySet(), Collections.emptySet());
        Configuration jaasConfig = new KeytabJaasConf(
                config.getKerberosJaasLoginModuleClass(),
                principal,
                config.isKerberosDebugEnabled(),
                config.isKerberosJaasStoreKey(),
                config.isKerberosJaasIsInitiator(),
                keytab,
                config.isKerberosJaasDoNotPrompt(),
                config.isKerberosJaasUseTicketCache(),
                config.isKerberosJaasRefreshKrb5Config());
        LoginContext loginContext = new LoginContext(config.getKerberosJaasContextName(), subject, noOpCallbackHandler(), jaasConfig);

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
        private final String  loginModuleClassName;
        private final String  userPrincipalName;
        private final boolean enableDebugLogs;
        private final boolean storeKey;
        private final boolean isInitiator;

        AbstractJaasConf(String loginModuleClassName, String userPrincipalName, boolean enableDebugLogs, boolean storeKey, boolean isInitiator) {
            this.loginModuleClassName = loginModuleClassName;
            this.userPrincipalName    = userPrincipalName;
            this.enableDebugLogs      = enableDebugLogs;
            this.storeKey             = storeKey;
            this.isInitiator          = isInitiator;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options = new HashMap<>();

            options.put("principal", userPrincipalName);
            options.put("isInitiator", Boolean.toString(isInitiator));
            options.put("storeKey", Boolean.toString(storeKey));
            options.put("debug", Boolean.toString(enableDebugLogs));

            addOptions(options);

            return new AppConfigurationEntry[] {
                    new AppConfigurationEntry(loginModuleClassName,
                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                            Collections.unmodifiableMap(options))
            };
        }

        abstract void addOptions(Map<String, String> options);
    }

    private static final class KeytabJaasConf extends AbstractJaasConf {
        private final String  keytabFilePath;
        private final boolean doNotPrompt;
        private final boolean useTicketCache;
        private final boolean refreshKrb5Config;

        KeytabJaasConf(String loginModuleClassName,
                       String userPrincipalName,
                       boolean enableDebugLogs,
                       boolean storeKey,
                       boolean isInitiator,
                       String keytabFilePath,
                       boolean doNotPrompt,
                       boolean useTicketCache,
                       boolean refreshKrb5Config) {
            super(loginModuleClassName, userPrincipalName, enableDebugLogs, storeKey, isInitiator);
            this.keytabFilePath     = keytabFilePath;
            this.doNotPrompt        = doNotPrompt;
            this.useTicketCache     = useTicketCache;
            this.refreshKrb5Config = refreshKrb5Config;
        }

        @Override
        void addOptions(Map<String, String> options) {
            options.put("useKeyTab", Boolean.TRUE.toString());
            options.put("keyTab", keytabFilePath);
            options.put("doNotPrompt", Boolean.toString(doNotPrompt));
            options.put("useTicketCache", Boolean.toString(useTicketCache));
            options.put("refreshKrb5Config", Boolean.toString(refreshKrb5Config));
        }
    }
}
