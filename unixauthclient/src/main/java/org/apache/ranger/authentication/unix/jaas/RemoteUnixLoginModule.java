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

package org.apache.ranger.authentication.unix.jaas;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class RemoteUnixLoginModule implements LoginModule {
    private static final String DEBUG_PARAM                          = "ranger.unixauth.debug";
    private static final String REMOTE_LOGIN_HOST_PARAM              = "ranger.unixauth.service.hostname";
    private static final String REMOTE_LOGIN_AUTH_SERVICE_PORT_PARAM = "ranger.unixauth.service.port";
    private static final String SSL_KEYSTORE_PATH_PARAM              = "ranger.unixauth.keystore";
    private static final String SSL_KEYSTORE_PATH_PASSWORD_PARAM     = "ranger.unixauth.keystore.password";
    private static final String SSL_TRUSTSTORE_PATH_PARAM            = "ranger.unixauth.truststore";
    private static final String SSL_TRUSTSTORE_PATH_PASSWORD_PARAM   = "ranger.unixauth.truststore.password";
    private static final String SSL_ENABLED_PARAM                    = "ranger.unixauth.ssl.enabled";
    private static final String SERVER_CERT_VALIDATION_PARAM         = "ranger.unixauth.server.cert.validation";
    private static final String JAAS_ENABLED_PARAM                   = "ranger.unixauth.remote.login.enabled";
    private static final String SSL_ALGORITHM                        = "TLSv1.2";

    private String userName;
    private String remoteHostName;
    private String loginGroups;
    private String keyStorePath;
    private String keyStorePathPassword;
    private String trustStorePath;
    private String trustStorePathPassword;
    private char[] password;
    private int remoteHostAuthServicePort;
    private Subject subject;
    private CallbackHandler callbackHandler;
    private boolean ssEnabled;
    private boolean loginSuccessful;
    private boolean                debug = true;
    private boolean serverCertValidation = true;
    private boolean   remoteLoginEnabled = true;

    public RemoteUnixLoginModule() {
        log("Created RemoteUnixLoginModule");
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject         = subject;
        this.callbackHandler = callbackHandler;

        log("RemoteUnixLoginModule::initialize() has been called with callbackhandler: " + this.callbackHandler);

        if (this.callbackHandler == null) {
            this.callbackHandler = new ConsolePromptCallbackHandler();
        }

        Properties config = new Properties();
        config.putAll(options);
        initParams(config);
    }

    @Override
    public boolean login() throws LoginException {
        if (remoteLoginEnabled && this.callbackHandler != null) {
            NameCallback     nameCallback     = new NameCallback("UserName:");
            PasswordCallback passwordCallback = new PasswordCallback("Password:", false);
            try {
                callbackHandler.handle(new Callback[] {nameCallback, passwordCallback});
            } catch (IOException | UnsupportedCallbackException e) {
                throw new LoginException("Unable to get username/password due to exception: " + e);
            }

            userName = nameCallback.getName();
            String modifiedUserName = userName;

            if (userName != null) {
                int atStartsAt = userName.indexOf("@");
                if (atStartsAt > -1) {
                    modifiedUserName = userName.substring(0, atStartsAt);
                }
            }

            password = passwordCallback.getPassword();

            log("userName:" + userName);
            log("modified UserName:" + modifiedUserName);

            char[] modifiedPasschar;
            if (password != null) {
                modifiedPasschar = Arrays.copyOf(password, password.length);
            } else {
                modifiedPasschar = new char[0];
            }

            doLogin(modifiedUserName, modifiedPasschar);

            Arrays.fill(password, ' ');
            Arrays.fill(modifiedPasschar, ' ');
            loginSuccessful = true;
        }

        return loginSuccessful;
    }

    @Override
    public boolean commit() throws LoginException {
        log("RemoteUnixLoginModule::commit() has been called. -> isLoginSuccess [" + loginSuccessful + "]");
        if (loginSuccessful) {
            if (subject != null) {
                subject.getPrincipals().add(new UnixUserPrincipal(userName.trim()));
                if (loginGroups != null) {
                    loginGroups = loginGroups.trim();
                    for (String group : loginGroups.split(",")) {
                        subject.getPrincipals().add(new UnixGroupPrincipal(group.trim()));
                    }
                }
            }
        } else {
            if (subject != null) {
                subject.getPrincipals().clear();
            }
        }
        return loginSuccessful;
    }

    @Override
    public boolean abort() throws LoginException {
        log("RemoteUnixLoginModule::abort() has been called.");
        loginSuccessful = false;
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        if (subject != null) {
            subject.getPrincipals().clear();
        }
        return true;
    }

    public void initParams(Properties options) {
        String val = (String) options.get(JAAS_ENABLED_PARAM);

        if (val != null) {
            remoteLoginEnabled = val.trim().equalsIgnoreCase("true");
            if (!remoteLoginEnabled) {
                log("Skipping RemoteLogin - [" + JAAS_ENABLED_PARAM + "] => [" + val + "]");
                return;
            }
        } else {
            remoteLoginEnabled = true;
        }

        val = (String) options.get(DEBUG_PARAM);
        debug = val != null && (!val.equalsIgnoreCase("false"));

        remoteHostName = (String) options.get(REMOTE_LOGIN_HOST_PARAM);
        log("RemoteHostName:" + remoteHostName);

        val = (String) options.get(REMOTE_LOGIN_AUTH_SERVICE_PORT_PARAM);
        if (val != null) {
            remoteHostAuthServicePort = Integer.parseInt(val.trim());
        }
        log("remoteHostAuthServicePort:" + remoteHostAuthServicePort);

        val        = (String) options.get(SSL_ENABLED_PARAM);
        ssEnabled = (val != null) && val.trim().equalsIgnoreCase("true");
        log("ssEnabled: " + ssEnabled);

        if (ssEnabled) {
            trustStorePath = (String) options.get(SSL_TRUSTSTORE_PATH_PARAM);
            log("trustStorePath: " + trustStorePath);

            if (trustStorePath != null) {
                trustStorePathPassword = (String) options.get(SSL_TRUSTSTORE_PATH_PASSWORD_PARAM);
                if (trustStorePathPassword == null) {
                    trustStorePathPassword = "";
                }
                log("trustStorePathPassword:*****");
            }

            keyStorePath = (String) options.get(SSL_KEYSTORE_PATH_PARAM);
            log("keyStorePath:" + keyStorePath);
            if (keyStorePath != null) {
                keyStorePathPassword = (String) options.get(SSL_KEYSTORE_PATH_PASSWORD_PARAM);
                if (keyStorePathPassword == null) {
                    keyStorePathPassword = "";
                }
                log("keyStorePathPassword:*****");
            }

            String certValidationFlag = (String) options.get(SERVER_CERT_VALIDATION_PARAM);
            serverCertValidation = (!(certValidationFlag != null && ("false".equalsIgnoreCase(certValidationFlag.trim()))));
            log("Server Cert Validation : " + serverCertValidation);
        }
    }

    public void doLogin(String aUserName, char[] modifiedPasschar) throws LoginException {
        /* Possible values
        * null
        * OK: group1, group2, group3
        * FAILED: Invalid Password
         */
        String ret = getLoginReplyFromAuthService(aUserName, modifiedPasschar);

        if (ret == null) {
            throw new LoginException("FAILED: unable to authenticate to AuthenticationService: " + remoteHostName + ":" + remoteHostAuthServicePort);
        } else if (ret.startsWith("OK:")) {
            loginSuccessful = true;
            if (ret.length() > 3) {
                this.loginGroups = ret.substring(3);
            }
        } else if (ret.startsWith("FAILED:")) {
            loginSuccessful = false;
            throw new LoginException("FAILED: unable to authenticate to AuthenticationService: " + remoteHostName + ":" + remoteHostAuthServicePort);
        } else {
            throw new LoginException("FAILED: unable to authenticate to AuthenticationService: " + remoteHostName + ":" + remoteHostAuthServicePort + ", msg:" + ret);
        }
    }

    private String getLoginReplyFromAuthService(String aUserName, char[] modifiedPasschar) throws LoginException {
        String ret = null;
        Socket sslsocket = null;

        char[] prefix    = ("LOGIN:" + aUserName + " ").toCharArray();
        char[] tail      = "\n".toCharArray();
        char[] loginData = new char[prefix.length + modifiedPasschar.length + tail.length];
        System.arraycopy(prefix, 0, loginData, 0, prefix.length);
        System.arraycopy(modifiedPasschar, 0, loginData, prefix.length, modifiedPasschar.length);
        System.arraycopy(tail, 0, loginData, prefix.length + modifiedPasschar.length, tail.length);
        try {
            try {
                if (ssEnabled) {
                    SSLContext context = SSLContext.getInstance(SSL_ALGORITHM);

                    KeyManager[] km = null;

                    if (keyStorePath != null) {
                        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());

                        try (InputStream in = getFileInputStream(keyStorePath)) {
                            ks.load(in, keyStorePathPassword.toCharArray());
                        }

                        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        kmf.init(ks, keyStorePathPassword.toCharArray());
                        km = kmf.getKeyManagers();
                    }

                    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

                    TrustManager[] tm = null;

                    if (serverCertValidation) {
                        KeyStore trustStoreKeyStore;

                        if (trustStorePath != null) {
                            trustStoreKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());

                            try (InputStream in = getFileInputStream(trustStorePath)) {
                                trustStoreKeyStore.load(in, trustStorePathPassword.toCharArray());

                                trustManagerFactory.init(trustStoreKeyStore);

                                tm = trustManagerFactory.getTrustManagers();
                            }
                        }
                    } else {
                        TrustManager ignoreValidationTM = new X509TrustManager() {
                            public void checkClientTrusted(X509Certificate[] chain, String authType) {
                                // Ignore Server Certificate Validation
                            }

                            public void checkServerTrusted(X509Certificate[] chain, String authType) {
                                // Ignore Server Certificate Validation
                            }

                            public X509Certificate[] getAcceptedIssuers() {
                                return new X509Certificate[0];
                            }
                        };

                        tm = new TrustManager[] {ignoreValidationTM};
                    }

                    SecureRandom random = new SecureRandom();

                    context.init(km, tm, random);

                    SSLSocketFactory sf = context.getSocketFactory();

                    sslsocket = sf.createSocket(remoteHostName, remoteHostAuthServicePort);
                } else {
                    sslsocket = new Socket(remoteHostName, remoteHostAuthServicePort);
                }

                OutputStreamWriter writer = new OutputStreamWriter(sslsocket.getOutputStream());

                writer.write(loginData);
                writer.flush();

                BufferedReader reader = new BufferedReader(new InputStreamReader(sslsocket.getInputStream()));

                ret = reader.readLine();

                reader.close();
                writer.close();
            } finally {
                if (sslsocket != null) {
                    sslsocket.close();
                }
            }
        } catch (Throwable t) {
            throw new LoginException("FAILED: unable to authenticate to AuthenticationService: " + remoteHostName + ":" + remoteHostAuthServicePort + ", Exception: [" + t + "]");
        } finally {
            log("Login of user String: {" + aUserName + "}, return from AuthServer: {" + ret + "}");
            Arrays.fill(loginData, ' ');
            Arrays.fill(modifiedPasschar, ' ');
        }

        return ret;
    }

    private InputStream getFileInputStream(String path) throws FileNotFoundException {
        InputStream ret;

        File f = new File(path);

        if (f.exists()) {
            ret = new FileInputStream(f);
        } else {
            ret = getClass().getResourceAsStream(path);

            if (ret == null) {
                if (!path.startsWith("/")) {
                    ret = getClass().getResourceAsStream("/" + path);
                }
            }

            if (ret == null) {
                ret = ClassLoader.getSystemClassLoader().getResourceAsStream(path);
                if (ret == null) {
                    if (!path.startsWith("/")) {
                        ret = ClassLoader.getSystemResourceAsStream("/" + path);
                    }
                }
            }
        }

        return ret;
    }

    private void log(String msg) {
        if (debug) {
            System.err.println("RemoteUnixLoginModule: " + msg);
        }
    }
}
