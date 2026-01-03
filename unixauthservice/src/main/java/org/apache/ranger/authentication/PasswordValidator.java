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

    private static List<String> adminUserList;
    private static String adminRoleNames;
    private static String validatorProgram;

    private Socket client;

    public PasswordValidator(Socket client) {
        this.client = client;
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

        try {
            reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
            writer = new PrintWriter(new OutputStreamWriter(client.getOutputStream()));
            String request = reader.readLine();

            if (request.startsWith("LOGIN:")) {
                String line       = request.substring(6).trim();
                int    passwordAt = line.indexOf(' ');
                if (passwordAt != -1) {
                    userName = line.substring(0, passwordAt).trim();
                }
            }

            if (validatorProgram == null) {
                String res = "FAILED: Unable to validate credentials.";
                writer.println(res);
                writer.flush();
                LOG.error("Response [{}] for user: {} as ValidatorProgram is not defined in configuration", res, userName);
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
                        if (adminRoleNames != null && adminUserList != null) {
                            if (adminUserList.contains(userName)) {
                                res = res + " " + adminRoleNames;
                            }
                        }
                    }

                    LOG.info("Response [{}] for user: {}", res, userName);

                    writer.println(res);
                    writer.flush();
                } finally {
                    if (p != null) {
                        p.destroy();
                    }
                }
            }
        } catch (Throwable t) {
            if (userName != null && writer != null) {
                String res = "FAILED: unable to validate due to error " + t.getMessage();
                writer.println(res);
                LOG.error("Response [{}] for user: {} message: {}", res, userName, t.getMessage());
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
}
