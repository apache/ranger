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

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.remote.RangerRemoteAuthzConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.FAILED_TO_READ_JWT_FROM_FILE;
import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.INVALID_PROPERTY_VALUE;
import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.MISSING_MANDATORY_CONFIGURATION;

public class RangerRemoteJwtProvider {
    private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String JWT_HEADER_PREFIX    = "Bearer ";

    private static final String SOURCE_ENV  = "env";
    private static final String SOURCE_FILE = "file";

    private final String source;
    private final String envVar;
    private final String filePath;

    private       long    fileLastModified;
    private volatile String jwt;

    private RangerRemoteJwtProvider(String source, String envVar, String filePath) {
        this.source   = source;
        this.envVar   = envVar;
        this.filePath = filePath;
    }

    public static RangerRemoteJwtProvider create(RangerRemoteAuthzConfig config) throws RangerAuthzException {
        String source = config.getJwtSource().trim().toLowerCase();

        switch (source) {
            case SOURCE_ENV:
                return new RangerRemoteJwtProvider(source, config.getJwtEnvVar(), null);

            case SOURCE_FILE:
                return new RangerRemoteJwtProvider(source, null, config.getJwtFile());

            default:
                throw new RangerAuthzException(INVALID_PROPERTY_VALUE, RangerRemoteAuthzConfig.PROP_REMOTE_AUTH_JWT_SOURCE, source);
        }
    }

    public String getJwt() throws RangerAuthzException {
        switch (source) {
            case SOURCE_ENV:
                jwt = System.getenv(envVar);
                break;

            case SOURCE_FILE:
                refreshJwtFromFile();
                break;

            default:
                throw new RangerAuthzException(INVALID_PROPERTY_VALUE, RangerRemoteAuthzConfig.PROP_REMOTE_AUTH_JWT_SOURCE, source);
        }

        if (StringUtils.isBlank(jwt)) {
            throw new RangerAuthzException(MISSING_MANDATORY_CONFIGURATION, getSourcePropertyName());
        }

        return jwt.trim();
    }

    public void setAuthnHeader(HttpRequestBase request) throws RangerAuthzException {
        request.setHeader(HEADER_AUTHORIZATION, JWT_HEADER_PREFIX + getJwt());
    }

    private void refreshJwtFromFile() throws RangerAuthzException {
        File file = new File(filePath);

        if (file.lastModified() != fileLastModified && file.canRead()) {
            try (BufferedReader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
                String line;

                while ((line = reader.readLine()) != null) {
                    if (StringUtils.isNotBlank(line) && !line.trim().startsWith("#")) {
                        jwt = line.trim();
                        break;
                    }
                }

                fileLastModified = file.lastModified();
            } catch (IOException e) {
                throw new RangerAuthzException(FAILED_TO_READ_JWT_FROM_FILE, e, filePath);
            }
        }
    }

    private String getSourcePropertyName() {
        switch (source) {
            case SOURCE_ENV:
                return RangerRemoteAuthzConfig.PROP_REMOTE_AUTH_JWT_ENV;

            case SOURCE_FILE:
                return RangerRemoteAuthzConfig.PROP_REMOTE_AUTH_JWT_FILE;

            default:
                return RangerRemoteAuthzConfig.PROP_REMOTE_AUTH_JWT_SOURCE;
        }
    }
}
