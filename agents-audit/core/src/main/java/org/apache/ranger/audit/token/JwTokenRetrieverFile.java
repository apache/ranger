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

package org.apache.ranger.audit.token;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Optional;

public class JwTokenRetrieverFile implements TokenRetriever<String> {
    private static final Logger LOG                      = LoggerFactory.getLogger(JwTokenRetrieverFile.class);
    private static final String RANGER_PROP_JWT_FILENAME = "ranger.auth.jwt.file.name";
    private final Optional<String> cachedJWT    = Optional.empty();
    private       File             jwtFile;
    private       Long             lastModified = -1L;

    public JwTokenRetrieverFile(Configuration config) {
        init(config);
    }

    @Override
    public Optional<String> retrieve() {
        LOG.debug("==> JwTokenRetrieverFile.retrieve()");

        Optional<String> ret = cachedJWT;

        if (jwtFile != null && jwtFile.lastModified() > this.lastModified) {
            try (BufferedReader reader = new BufferedReader(new FileReader(jwtFile))) {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
                        ret               = Optional.of(line);
                        this.lastModified = jwtFile.lastModified();
                        break;
                    }
                }
            } catch (IOException e) {
                LOG.error("JwTokenRetrieverFile.retrieve(): Failed to get JWT token.", e);
            }
        }

        LOG.debug("<== JwTokenRetrieverFile.retrieve(): isJwTokenPresent={}", ret.isPresent());

        return ret;
    }

    private void init(Configuration config) {
        LOG.debug("==> JwTokenRetrieverFile.init()");

        String fileName = config.get(RANGER_PROP_JWT_FILENAME, "");
        if (StringUtils.isNotBlank(fileName)) {
            jwtFile = new File(fileName);
        }

        LOG.debug("<== JwTokenRetrieverFile.init(): fileName={}", fileName);
    }
}
