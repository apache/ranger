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

package org.apache.ranger.plugin.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
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
    private static final String[] DANGEROUS_PATTERNS = {"socketfactory", "sslfactory", "autodeserialize"};

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
        rejectBlockedParameters(trimmed, trimmed);
        // Hive decodes percent-escapes before splitting session variables, so a
        // separator written as %3B or %3F is invisible to a scan of the raw URL.
        if (trimmed.indexOf('%') >= 0) {
            rejectBlockedParameters(percentDecode(trimmed), trimmed);
        }
        LOG.debug("jdbc.url passed validation: {}", sanitizeForLog(trimmed));
    }

    public static void validate(String jdbcUrl, Collection<String> allowedUrlPrefixes) throws HadoopException {
        validate(jdbcUrl);

        boolean isAllowed = false;

        if (allowedUrlPrefixes != null) {
            for (String prefix : allowedUrlPrefixes) {
                if (prefix != null && !prefix.isEmpty() && jdbcUrl.startsWith(prefix)) {
                    isAllowed = true;
                    break;
                }
            }
        }

        if (!isAllowed) {
            LOG.warn("Rejected jdbc.url with unsupported scheme: {}", sanitizeForLog(jdbcUrl));

            HadoopException e = new HadoopException("jdbc.url must start with one of " + allowedUrlPrefixes);
            e.generateResponseDataMap(false, "Invalid jdbc.url", "jdbc.url must start with one of " + allowedUrlPrefixes, null, "jdbc.url");
            throw e;
        }
    }

    public static void validateDriverClassName(String driverClassName, Collection<String> allowedDriverClassNames) throws HadoopException {
        if (driverClassName == null) {
            return;
        }

        if (allowedDriverClassNames == null || !allowedDriverClassNames.contains(driverClassName)) {
            LOG.warn("Rejected jdbc.driverClassName not in allowed list {}", allowedDriverClassNames);

            HadoopException e = new HadoopException("jdbc.driverClassName must be one of " + allowedDriverClassNames);
            e.generateResponseDataMap(false, "Invalid jdbc.driverClassName", "jdbc.driverClassName must be one of " + allowedDriverClassNames, null, "jdbc.driverClassName");
            throw e;
        }
    }

    private static void rejectBlockedParameters(String candidate, String urlForLog) throws HadoopException {
        int queryStart = findQueryStart(candidate);
        if (queryStart != -1) {
            validateQueryString(candidate.substring(queryStart + 1), urlForLog);
        }
    }

    private static String percentDecode(String value) throws HadoopException {
        StringBuilder decoded = new StringBuilder(value.length());

        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);

            if (current != '%') {
                decoded.append(current);
                continue;
            }

            if (i + 2 >= value.length()) {
                throw invalidPercentEncoding(value);
            }

            int high = Character.digit(value.charAt(i + 1), 16);
            int low = Character.digit(value.charAt(i + 2), 16);

            if (high < 0 || low < 0) {
                throw invalidPercentEncoding(value);
            }

            decoded.append((char) ((high << 4) + low));
            i += 2;
        }

        return decoded.toString();
    }

    private static HadoopException invalidPercentEncoding(String url) {
        LOG.warn("Rejected jdbc.url with invalid percent-encoding: {}", sanitizeForLog(url));

        HadoopException e = new HadoopException("jdbc.url contains invalid percent-encoding");
        e.generateResponseDataMap(false, "Invalid jdbc.url", "jdbc.url contains invalid percent-encoding", null, "jdbc.url");
        return e;
    }

    private static void validateQueryString(String queryString, String fullUrl) throws HadoopException {
        String[] tokens = queryString.split("[&;?]");
        for (String token : tokens) {
            if (token.trim().isEmpty()) {
                continue;
            }
            int eqIdx = token.indexOf('=');
            String paramName = (eqIdx >= 0 ? token.substring(0, eqIdx) : token).trim();
            String decodedParamName = paramName;
            try {
                decodedParamName = URLDecoder.decode(paramName, StandardCharsets.UTF_8);
            } catch (Exception e) {
                LOG.warn("Failed to decode parameter name: {}", paramName);
            }

            String normalized = decodedParamName.toLowerCase().trim().replaceAll("[._-]", "");
            if (BLOCKED_PARAMS.contains(normalized)) {
                logAndThrow("blocked parameter", normalized, paramName, fullUrl);
            }
            for (String danger : DANGEROUS_PATTERNS) {
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
        int idx = findQueryStart(url);
        return idx >= 0 ? url.substring(0, idx) + "?<params_redacted>" : url;
    }

    private static int findQueryStart(String url) {
        int qIdx = url.indexOf('?');
        int sIdx = url.indexOf(';');
        if (qIdx >= 0 && sIdx >= 0) {
            return Math.min(qIdx, sIdx);
        } else if (qIdx >= 0) {
            return qIdx;
        } else if (sIdx >= 0) {
            return sIdx;
        }
        return -1;
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
