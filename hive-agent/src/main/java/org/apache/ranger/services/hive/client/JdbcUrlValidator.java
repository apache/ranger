/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.services.hive.client;

import org.apache.ranger.plugin.client.HadoopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class JdbcUrlValidator {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcUrlValidator.class);
    private static final Set<String> BLOCKED_PARAMS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    "socketfactory", "socketfactoryarg", "sslfactory", "sslfactoryarg",
                    "sslhostnameverifier", "authenticationpluginclassname", "loggerclassname",
                    "kerberosservername", "gssdelegatecred", "sslpasswordcallback")));

    private JdbcUrlValidator() {
    }

    public static void validate(String jdbcUrl) throws HadoopException {
        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            HadoopException e = new HadoopException("jdbc.url must not be null or empty");
            e.generateResponseDataMap(false, "Validation failed", "jdbc.url is required",
                    null, "jdbc.url");
            throw e;
        }
        String trimmed = jdbcUrl.trim();
        int queryStart = trimmed.indexOf('?');
        if (queryStart == -1) {
            queryStart = trimmed.indexOf(';');
        }
        if (queryStart != -1) {
            String queryString = trimmed.substring(queryStart + 1);
            validateQueryString(queryString, trimmed);
        }
        LOG.debug("jdbc.url passed validation: {}", sanitizeForLog(trimmed));
    }

    private static void validateQueryString(String queryString, String fullUrl) throws HadoopException {
        String[] tokens = queryString.split("[&;]");
        for (String token : tokens) {
            if (token.trim().isEmpty()) {
                continue;
            }
            int eqIdx = token.indexOf('=');
            String paramName = (eqIdx >= 0 ? token.substring(0, eqIdx) : token).trim();
            String decodedParamName = paramName;
            try {
                decodedParamName = URLDecoder.decode(paramName, "UTF-8");
            } catch (Exception e) {
                LOG.warn("Failed to decode parameter name: {}", paramName);
            }

            String normalized = decodedParamName.toLowerCase().trim().replaceAll("[._-]", "");
            if (BLOCKED_PARAMS.contains(normalized)) {
                logAndThrow("blocked parameter", normalized, paramName, fullUrl);
            }
            String[] dangerPatterns = {"socketfactory", "sslfactory", "autodeserialize"};
            for (String danger : dangerPatterns) {
                if (normalized.contains(danger)) {
                    logAndThrow("dangerous pattern '" + danger + "'", normalized, paramName, fullUrl);
                }
            }
            if (normalized.contains("factory") && (normalized.contains("socket") || normalized.contains("ssl") ||
                    normalized.contains("connection") || normalized.contains("auth") ||
                    normalized.contains("driver") || normalized.contains("datasource"))) {
                logAndThrow("potentially dangerous factory parameter", normalized, paramName, fullUrl);
            }
        }
    }

    static String sanitizeForLog(String url) {
        if (url == null) {
            return "<null>";
        }
        int q = url.indexOf('?');
        return q >= 0 ? url.substring(0, q) + "?<params_redacted>" : url;
    }

    private static void logAndThrow(String reason, String normalized, String originalParam, String fullUrl) {
        LOG.warn("Rejected jdbc.url containing {} '{}' (param='{}'): {}", reason, normalized, originalParam, sanitizeForLog(fullUrl));
        HadoopException e = new HadoopException("jdbc.url contains a prohibited parameter: '" + originalParam +
                "'. This parameter is not permitted for security reasons.");
        e.generateResponseDataMap(false, "Invalid jdbc.url parameter", "Parameter '" +
                originalParam + "' is blocked", null, "jdbc.url");
        throw e;
    }
}
