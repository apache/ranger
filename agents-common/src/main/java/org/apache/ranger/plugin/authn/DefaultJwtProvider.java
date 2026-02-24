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

package org.apache.ranger.plugin.authn;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.authorization.hadoop.utils.RangerCredentialProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class DefaultJwtProvider implements JwtProvider {
    private static final Logger LOG           = LoggerFactory.getLogger(DefaultJwtProvider.class);
    public static final String JWT_SOURCE     = ".jwt.source";
    public static final String JWT_ENV        = ".jwt.env";
    public static final String JWT_FILE       = ".jwt.file";
    public static final String JWT_CRED_FILE  = ".jwt.cred.file";
    public static final String JWT_CRED_ALIAS = ".jwt.cred.alias";

    private final String propertyPrefix;
    private final Configuration config;
    private String jwt;

    public DefaultJwtProvider(String propertyPrefix, Configuration config){
        this.propertyPrefix = propertyPrefix;
        this.config = config;
    }

    @Override
    public String getJwt() {
        final String jwtSrc = config.get(propertyPrefix + JWT_SOURCE);

        if (StringUtils.isNotEmpty(jwtSrc)) {
            switch (jwtSrc) {
                case "env":
                    String jwtEnvVar = config.get(propertyPrefix + JWT_ENV);
                    if (StringUtils.isNotEmpty(jwtEnvVar)) {
                        String jwtFromEnv = System.getenv(jwtEnvVar);
                        if (StringUtils.isNotBlank(jwtFromEnv)) {
                            jwt = jwtFromEnv;
                        }
                    }
                    break;
                case "file":
                    String jwtFilePath = config.get(propertyPrefix + JWT_FILE);
                    if (StringUtils.isNotEmpty(jwtFilePath)) {
                        File jwtFile = new File(jwtFilePath);
                        if (jwtFile.exists()) {
                            try (BufferedReader reader = new BufferedReader(new FileReader(jwtFile))) {
                                String line = null;
                                while ((line = reader.readLine()) != null) {
                                    if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
                                        jwt = line;
                                        break;
                                    }
                                }
                            } catch (IOException e) {
                                LOG.error("Failed to read JWT from file: {}", jwtFilePath, e);
                            }
                        }
                    }
                    break;
                case "cred":
                    String credFilePath = config.get(propertyPrefix + JWT_CRED_FILE);
                    String credAlias = config.get(propertyPrefix + JWT_CRED_ALIAS);
                    if (StringUtils.isNotEmpty(credFilePath) && StringUtils.isNotEmpty(credAlias)) {
                        String jwtFromCredFile = RangerCredentialProvider.getInstance().getCredentialString(credFilePath, credAlias);
                        if (StringUtils.isNotBlank(jwtFromCredFile)) {
                            jwt = jwtFromCredFile;
                        }
                    }
                    break;
            }
        } else {
            LOG.info("JWT source not configured, proceeding without JWT");
        }
        return jwt;
    }
}
