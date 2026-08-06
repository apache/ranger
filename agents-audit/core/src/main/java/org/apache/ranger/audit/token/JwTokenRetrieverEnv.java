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

import java.util.Optional;

public class JwTokenRetrieverEnv implements TokenRetriever<String> {
    private static final Logger LOG                          = LoggerFactory.getLogger(JwTokenRetrieverEnv.class);
    private static final String DEFAULT_JWT_ENV_VAR_NAME     = "RANGER_AUTH_JWT";
    private static final String RANGER_PROP_JWT_ENV_VAR_NAME = "ranger.auth.jwt.env.var.name";

    private String jwtEnvVarName = DEFAULT_JWT_ENV_VAR_NAME;

    public JwTokenRetrieverEnv(Configuration config) {
        init(config);
    }

    @Override
    public Optional<String> retrieve() {
        LOG.debug("==> JwTokenRetrieverEnv.retrieve()");

        Optional<String> ret = Optional.empty();

        try {
            String jwt = System.getenv(jwtEnvVarName);
            if (StringUtils.isNotBlank(jwt)) {
                ret = Optional.of(jwt);
            }
        } catch (Exception e) {
            LOG.error("JwTokenRetrieverEnv.retrieve(): Failed to get JWT token.", e);
        }

        LOG.debug("<== JwTokenRetrieverEnv.retrieve(): isJwTokenPresent={}", ret.isPresent());
        return ret;
    }

    private void init(Configuration config) {
        LOG.debug("==> JwTokenRetrieverEnv.init()");

        this.jwtEnvVarName = config.get(RANGER_PROP_JWT_ENV_VAR_NAME, DEFAULT_JWT_ENV_VAR_NAME);

        LOG.debug("<== JwTokenRetrieverEnv.init(): jwtEnvVarName={}", jwtEnvVarName);
    }
}
