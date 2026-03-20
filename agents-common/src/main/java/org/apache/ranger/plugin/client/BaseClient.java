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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

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

    protected void validateSqlIdentifier(String identifier, String identifierType) throws HadoopException {
        if (StringUtils.isBlank(identifier)) {
            return;
        }
        if (identifier.contains("..") || identifier.contains("//") || identifier.contains("\\")) {
            String msgDesc = "Invalid " + identifierType + ": [" + identifier + "]. Path traversal patterns are not allowed.";
            HadoopException hdpException = new HadoopException(msgDesc);
            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + DEFAULT_ERROR_MESSAGE, null, null);
            LOG.error(msgDesc);
            throw hdpException;
        }
        if (!identifier.matches("^[a-zA-Z0-9_*?\\[\\]\\-\\$%\\{\\}\\=\\/\\.]+$")) {
            String msgDesc = "Invalid " + identifierType + ": [" + identifier + "]. Only alphanumeric characters along with ( ., _, -, *, ?, [], {}, %, $, = / ) are allowed.";
            HadoopException hdpException = new HadoopException(msgDesc);
            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + DEFAULT_ERROR_MESSAGE, null, null);
            LOG.error(msgDesc);
            throw hdpException;
        }
    }

    protected String convertToSqlPattern(String pattern) throws HadoopException {
        if (pattern == null || pattern.isEmpty() || pattern.equals("*")) {
            return "%";
        }
        return pattern.replace("*", "%");
    }

    protected boolean matchesSqlPattern(String value, String pattern) throws HadoopException {
        if (pattern == null || pattern.equals("%")) {
            return true;
        }

        String regex = convertSqlPatternToRegex(pattern);
        try {
            return value.matches(regex);
        } catch (PatternSyntaxException pe) {
            String msgDesc = "Invalid " + pattern + ": [" + value + "]. Only alphanumeric characters along with ( ., _, -, *, ?, [], {}, %, $, = / ) are allowed.";
            HadoopException hdpException = new HadoopException(msgDesc);
            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + DEFAULT_ERROR_MESSAGE, null, null);
            LOG.error(msgDesc);
            throw hdpException;
        }
    }

    protected void validateUrlResourceName(String resourceName, String resourceType) throws HadoopException {
        if (resourceName == null) {
            return;
        }
        if (resourceName.contains("..") || resourceName.contains("//") || resourceName.contains("\\")) {
            String msgDesc = "Invalid " + resourceType + ": [" + resourceName + "]. Path traversal patterns are not allowed.";
            HadoopException hdpException = new HadoopException(msgDesc);
            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + DEFAULT_ERROR_MESSAGE, null, null);
            LOG.error(msgDesc);
            throw hdpException;
        }
        if (!resourceName.matches("^[a-zA-Z0-9_.*\\-]+$")) {
            String msgDesc = "Invalid " + resourceType + ": [" + resourceName + "]. Only alphanumeric characters with ( ., _, *, -) are allowed.";
            HadoopException hdpException = new HadoopException(msgDesc);
            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + DEFAULT_ERROR_MESSAGE, null, null);
            LOG.error(msgDesc);
            throw hdpException;
        }
    }

    protected void validateWildcardPattern(String pattern, String patternType) throws HadoopException {
        if (pattern == null || pattern.isEmpty()) {
            return;
        }
        if (pattern.contains("..") || pattern.contains("//") || pattern.contains("\\")) {
            String msgDesc = "Invalid " + patternType + ": [" + pattern + "]. Path traversal patterns are not allowed.";
            HadoopException hdpException = new HadoopException(msgDesc);
            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + DEFAULT_ERROR_MESSAGE, null, null);
            LOG.error(msgDesc);
            throw hdpException;
        }
        if (!pattern.matches("^[a-zA-Z0-9_.*?\\[\\]\\-\\$%\\{\\}\\=\\/]+$")) {
            String msgDesc = "Invalid " + patternType + ": [" + pattern + "]. Only alphanumeric characters along with ( ., _, -, *, ?, [], {}, %, $, = / ) are allowed.";
            HadoopException hdpException = new HadoopException(msgDesc);
            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + DEFAULT_ERROR_MESSAGE, null, null);
            LOG.error(msgDesc);
            throw hdpException;
        }
    }

    protected String convertSqlPatternToRegex(String pattern) {
        StringBuilder regexBuilder = new StringBuilder("^");

        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '%':
                    // SQL LIKE wildcard: zero or more characters
                    regexBuilder.append(".*");
                    break;
                case '_':
                    // SQL LIKE wildcard: exactly one character
                    regexBuilder.append('.');
                    break;
                case '.':
                case '^':
                case '$':
                case '+':
                case '?':
                case '{':
                case '}':
                case '[':
                case ']':
                case '(':
                case ')':
                case '|':
                case '\\':
                    // Escape regex metacharacters so they are treated literally
                    regexBuilder.append('\\').append(c);
                    break;
                default:
                    regexBuilder.append(c);
                    break;
            }
        }

        return regexBuilder.toString();
    }

    protected String convertWildcardToRegex(String wildcard) {
        if (wildcard == null || wildcard.isEmpty()) {
            return ".*";
        }
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);
            switch (c) {
                case '*':
                    regex.append(".*");
                    break;
                case '?':
                    regex.append(".");
                    break;
                case '.':
                case '\\':
                case '^':
                case '$':
                case '|':
                    regex.append('\\').append(c);
                    break;
                case '{':
                case '}':
                case '[':
                case ']':
                    regex.append('\\').append(c);
                    break;
                default:
                    regex.append(c);
            }
        }
        regex.append('$');
        return regex.toString();
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
