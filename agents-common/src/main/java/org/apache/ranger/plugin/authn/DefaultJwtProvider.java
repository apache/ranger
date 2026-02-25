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
    private static final Logger LOG = LoggerFactory.getLogger(DefaultJwtProvider.class);

    public static final String JWT_SOURCE     = ".jwt.source";
    public static final String JWT_ENV        = ".jwt.env";
    public static final String JWT_FILE       = ".jwt.file";
    public static final String JWT_CRED_FILE  = ".jwt.cred.file";
    public static final String JWT_CRED_ALIAS = ".jwt.cred.alias";

    private final String jwtEnvVar;
    private final String jwtFilePath;
    private final String jwtCredFilePath;
    private final String jwtCredAlias;
    private       long   jwtFileLastModified;
    private       long   jwtCredFileLastCheckedAt;

    private volatile String jwt;

    public DefaultJwtProvider(String propertyPrefix, Configuration config) {
        String jwtSrc = config.get(propertyPrefix + JWT_SOURCE);

        if (jwtSrc == null) {
            jwtSrc = "";
        }

        switch (jwtSrc) {
            case "env":
                this.jwtEnvVar       = config.get(propertyPrefix + JWT_ENV);
                this.jwtFilePath     = null;
                this.jwtCredFilePath = null;
                this.jwtCredAlias    = null;
                break;

            case "file":
                this.jwtEnvVar       = null;
                this.jwtFilePath     = config.get(propertyPrefix + JWT_FILE);
                this.jwtCredFilePath = null;
                this.jwtCredAlias    = null;
                break;

            case "cred":
                this.jwtEnvVar       = null;
                this.jwtFilePath     = null;
                this.jwtCredFilePath = config.get(propertyPrefix + JWT_CRED_FILE);
                this.jwtCredAlias    = config.get(propertyPrefix + JWT_CRED_ALIAS);
                break;

            default:
                this.jwtEnvVar       = null;
                this.jwtFilePath     = null;
                this.jwtCredFilePath = null;
                this.jwtCredAlias    = null;
                break;
        }
    }

    @Override
    public String getJwt() {
        if (StringUtils.isNotEmpty(jwtEnvVar)) {
            jwt = System.getenv(jwtEnvVar);
        } else if (StringUtils.isNotEmpty(jwtFilePath)) {
            File jwtFile = new File(jwtFilePath);

            if (jwtFile.lastModified() != jwtFileLastModified && jwtFile.canRead()) {
                try (BufferedReader reader = new BufferedReader(new FileReader(jwtFile))) {
                    String line;

                    while ((line = reader.readLine()) != null) {
                        if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
                            break;
                        }
                    }

                    jwt                 = line;
                    jwtFileLastModified = jwtFile.lastModified();
                } catch (IOException e) {
                    LOG.error("Failed to read JWT from file: {}", jwtFilePath, e);
                }
            }
        } else if (StringUtils.isNotEmpty(jwtCredFilePath) && StringUtils.isNotEmpty(jwtCredAlias)) {
            long currentTime = System.currentTimeMillis();

            if ((currentTime - jwtCredFileLastCheckedAt) > 60_000) { // last check should be more than 1 minute ago
                jwt                      = RangerCredentialProvider.getInstance().getCredentialString(jwtCredFilePath, jwtCredAlias);
                jwtCredFileLastCheckedAt = currentTime;
            }
        }

        return jwt;
    }
}
