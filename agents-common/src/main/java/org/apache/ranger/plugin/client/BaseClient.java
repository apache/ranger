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

package org.apache.ranger.plugin.client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class BaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(BaseClient.class);

    protected static final String DEFAULT_ERROR_MESSAGE = " You can still save the repository and start creating policies, but you would not be able to use autocomplete for resource names. Check ranger_admin.log for more info.";
    private static final   String DEFAULT_NAME_RULE     = "DEFAULT";

    protected Map<String, String> connectionProperties;

    private final String             serviceName;
    private final String             defaultConfigFile;
    private       Subject            loginSubject;
    private       HadoopConfigHolder configHolder;

    public BaseClient(String svcName, Map<String, String> connectionProperties) {
        this(svcName, connectionProperties, null);
    }

    public BaseClient(String serviceName, Map<String, String> connectionProperties, String defaultConfigFile) {
        this.serviceName          = serviceName;
        this.connectionProperties = connectionProperties;
        this.defaultConfigFile    = defaultConfigFile;

        init();
        login();
    }

    public static void generateResponseDataMap(boolean connectivityStatus, String message, String description, Long objectId, String fieldName, Map<String, Object> responseData) {
        responseData.put("connectivityStatus", connectivityStatus);
        responseData.put("message", message);
        responseData.put("description", description);
        responseData.put("objectId", objectId);
        responseData.put("fieldName", fieldName);
    }

    public static String getMessage(Throwable excp) {
        List<String> errList = new ArrayList<>();

        while (excp != null) {
            String message = excp.getMessage();

            if (StringUtils.isNotEmpty(message) && !errList.contains(message + ". \n")) {
                errList.add(message + ". \n");
            }

            excp = excp.getCause();
        }

        return StringUtils.join(errList, "");
    }

    public String getSerivceName() {
        return serviceName;
    }

    protected void login() {
        ClassLoader prevCl = Thread.currentThread().getContextClassLoader();

        try {
            //Thread.currentThread().setContextClassLoader(configHolder.getClassLoader());
            String lookupPrincipal = SecureClientLogin.getPrincipal(configHolder.getLookupPrincipal(), java.net.InetAddress.getLocalHost().getCanonicalHostName());
            String lookupKeytab    = configHolder.getLookupKeytab();
            String nameRules       = configHolder.getNameRules();

            if (StringUtils.isEmpty(nameRules)) {
                LOG.debug("Name Rule is empty. Setting Name Rule as 'DEFAULT'");

                nameRules = DEFAULT_NAME_RULE;
            }

            String userName = configHolder.getUserName();

            if (StringUtils.isEmpty(lookupPrincipal) || StringUtils.isEmpty(lookupKeytab)) {
                if (userName == null) {
                    throw createException("Unable to find login username for hadoop environment, [" + serviceName + "]", null);
                }

                String keyTabFile = configHolder.getKeyTabFile();

                if (keyTabFile != null) {
                    if (configHolder.isKerberosAuthentication()) {
                        LOG.info("Init Login: security enabled, using username/keytab");

                        loginSubject = SecureClientLogin.loginUserFromKeytab(userName, keyTabFile, nameRules);
                    } else {
                        LOG.info("Init Login: using username");

                        loginSubject = SecureClientLogin.login(userName);
                    }
                } else {
                    String encryptedPwd = configHolder.getPassword();
                    String password     = null;

                    if (encryptedPwd != null) {
                        try {
                            password = PasswordUtils.decryptPassword(encryptedPwd);
                        } catch (Exception ex) {
                            LOG.info("Password decryption failed; trying connection with received password string");

                            password = null;
                        } finally {
                            if (password == null) {
                                password = encryptedPwd;
                            }
                        }
                    } else {
                        LOG.info("Password decryption failed: no password was configured");
                    }

                    if (configHolder.isKerberosAuthentication()) {
                        LOG.info("Init Login: using username/password");

                        loginSubject = SecureClientLogin.loginUserWithPassword(userName, password);
                    } else {
                        LOG.info("Init Login: security not enabled, using username");

                        loginSubject = SecureClientLogin.login(userName);
                    }
                }
            } else {
                if (configHolder.isKerberosAuthentication()) {
                    LOG.info("Init Lookup Login: security enabled, using lookupPrincipal/lookupKeytab");

                    loginSubject = SecureClientLogin.loginUserFromKeytab(lookupPrincipal, lookupKeytab, nameRules);
                } else {
                    LOG.info("Init Login: security not enabled, using username");

                    loginSubject = SecureClientLogin.login(userName);
                }
            }
        } catch (IOException | SecurityException ex) {
            throw createException(ex);
        } finally {
            Thread.currentThread().setContextClassLoader(prevCl);
        }
    }

    protected Subject getLoginSubject() {
        return loginSubject;
    }

    protected HadoopConfigHolder getConfigHolder() {
        return configHolder;
    }

    private void init() {
        if (connectionProperties == null) {
            configHolder = HadoopConfigHolder.getInstance(serviceName);
        } else {
            configHolder = HadoopConfigHolder.getInstance(serviceName, connectionProperties, defaultConfigFile);
        }
    }

    private HadoopException createException(Exception exp) {
        return createException("Unable to login to Hadoop environment [" + serviceName + "]", exp);
    }

    private HadoopException createException(String msgDesc, Exception exp) {
        HadoopException hdpException    = new HadoopException(msgDesc, exp);
        final String    fullDescription = exp != null ? getMessage(exp) : msgDesc;

        hdpException.generateResponseDataMap(false, fullDescription + DEFAULT_ERROR_MESSAGE, msgDesc + DEFAULT_ERROR_MESSAGE, null, null);

        return hdpException;
    }
}
