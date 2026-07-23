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

package org.apache.ranger.authentication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

public class PasswordValidator implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(PasswordValidator.class);
    private static final String GENERIC_FAILURE_RESPONSE = "FAILED: Authentication failed.";
    private static final String LOCKED_OUT_RESPONSE = "FAILED: Too many attempts. Try again later.";

    private static List<String> adminUserList;
    private static String adminRoleNames;
    private static String validatorProgram;

    private final LoginAttemptTracker loginAttemptTracker;
    private Socket client;

    public PasswordValidator(Socket client, LoginAttemptTracker loginAttemptTracker) {
        this.client = client;
        this.loginAttemptTracker = loginAttemptTracker;
    }

    public static String getValidatorProgram() {
        return validatorProgram;
    }

    public static void setValidatorProgram(String validatorProgram) {
        PasswordValidator.validatorProgram = validatorProgram;
    }

    public static List<String> getAdminUserList() {
        return adminUserList;
    }

    public static void setAdminUserList(List<String> adminUserList) {
        PasswordValidator.adminUserList = adminUserList;
    }

    public static String getAdminRoleNames() {
        return adminRoleNames;
    }

    public static void setAdminRoleNames(String adminRoleNames) {
        PasswordValidator.adminRoleNames = adminRoleNames;
    }

    @Override
    public void run() {
        BufferedReader reader;
        PrintWriter    writer = null;

        String userName = null;

        String remoteIp = client.getInetAddress() != null ? client.getInetAddress().getHostAddress() : "unknown";
        try {
            reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
            writer = new PrintWriter(new OutputStreamWriter(client.getOutputStream()));
            String request = reader.readLine();
            userName = parseUserName(request);

            if (loginAttemptTracker.isBlocked(remoteIp)) {
                LOG.warn("Rejected UnixAuth attempt from [{}] for user [{}] - temporarily locked out due to repeated failures from this source.", remoteIp, userName);
                writer.println(LOCKED_OUT_RESPONSE);
                writer.flush();
                return;
            }
            long accountDelayMs = loginAttemptTracker.getAccountDelayMs(accountKey(userName), remoteIp);
            if (accountDelayMs > 0) {
                LOG.warn("Slowing UnixAuth attempt from [{}] for user [{}] by {} ms due to failed attempts from multiple source IPs for this account.", remoteIp, userName, accountDelayMs);
                try {
                    Thread.sleep(accountDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Account delay interrupted for user [{}] from [{}]", userName, remoteIp);
                }
            }
            if (request == null) {
                loginAttemptTracker.recordFailure(remoteIp, accountKey(userName));
                LOG.warn("Rejected UnixAuth attempt from [{}] - connection closed before sending any data.", remoteIp);
                writer.println(GENERIC_FAILURE_RESPONSE);
                writer.flush();
                return;
            }

            if (validatorProgram == null) {
                loginAttemptTracker.recordFailure(remoteIp, accountKey(userName));
                LOG.error("Response [{}] for user: {} from [{}] as ValidatorProgram is not defined in configuration.", GENERIC_FAILURE_RESPONSE, userName, remoteIp);
                writer.println(GENERIC_FAILURE_RESPONSE);
                writer.flush();
            } else {
                BufferedReader pReader;
                PrintWriter    pWriter;
                Process        p       = null;

                try {
                    p = Runtime.getRuntime().exec(validatorProgram);

                    pReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    pWriter = new PrintWriter(new OutputStreamWriter(p.getOutputStream()));

                    pWriter.println(request);
                    pWriter.flush();

                    String res = pReader.readLine();

                    if (res != null && res.startsWith("OK")) {
                        loginAttemptTracker.recordSuccess(remoteIp, accountKey(userName));
                        if (adminRoleNames != null && adminUserList != null) {
                            if (adminUserList.contains(userName)) {
                                res = res + " " + adminRoleNames;
                            }
                        }
                    } else {
                        loginAttemptTracker.recordFailure(remoteIp, accountKey(userName));
                        res = GENERIC_FAILURE_RESPONSE;
                    }

                    LOG.info("Response [{}] for user: {} from [{}]", res, userName, remoteIp);

                    writer.println(res);
                    writer.flush();
                } finally {
                    if (p != null) {
                        p.destroy();
                    }
                }
            }
        } catch (Throwable t) {
            loginAttemptTracker.recordFailure(remoteIp, accountKey(userName));
            if (userName != null && writer != null) {
                LOG.error("Response [{}] for user: {} from [{}], {}", GENERIC_FAILURE_RESPONSE, userName, remoteIp, t.getMessage());
                writer.println(GENERIC_FAILURE_RESPONSE);
                writer.flush();
            }
        } finally {
            try {
                if (client != null) {
                    client.close();
                }
            } catch (IOException ioe) {
                LOG.debug("Close socket failure. Detail: ", ioe);
            } finally {
                client = null;
            }
        }
    }

    private static String accountKey(String userName) {
        if (userName == null || userName.isEmpty()) {
            return null;
        }
        if (adminUserList != null && adminUserList.contains(userName)) {
            return null;
        }
        return userName;
    }

    private static String parseUserName(String request) {
        if (request == null || !request.startsWith("LOGIN:")) {
            return null;
        }
        String line       = request.substring(6).trim();
        int    passwordAt = line.indexOf(' ');
        if (passwordAt == -1) {
            return null;
        }
        return line.substring(0, passwordAt).trim();
    }
}
