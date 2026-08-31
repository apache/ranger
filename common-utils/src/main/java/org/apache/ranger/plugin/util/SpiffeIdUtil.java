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

package org.apache.ranger.plugin.util;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper for validating a SPIFFE ID that is used as a service identity.
 *
 * <p>A SPIFFE ID is expected in the general form defined by the SPIFFE-ID specification,
 * {@code spiffe://<trust-domain>/<path>}, where {@code <path>} is one or more {@code /}-separated
 * segments. For service-to-service authentication the receiving server trusts the workload identity
 * carried in a header (e.g. {@code x-service-spiffe-id}) and uses the SPIFFE ID itself as the
 * authenticated principal. No particular path layout (such as the SPIRE {@code /ns/<namespace>/sa/<service-account>}
 * convention) is required.
 */
public final class SpiffeIdUtil {
    private static final String  HEADER_NAMES_SEP  = ",";

    /**
     * Matches {@code spiffe://<trust-domain>/<path>} using the character sets from the SPIFFE-ID
     * specification:
     * <ul>
     *   <li>trust-domain: lowercase letters, digits, {@code .}, {@code -}, {@code _} (the spec requires
     *       the trust domain to be lowercase);
     *   <li>path: one or more {@code /}-separated segments of ASCII letters (any case), digits,
     *       {@code .}, {@code -}, {@code _} (SPIFFE path segments are case-sensitive).
     * </ul>
     * At least one path segment is required and every segment must be non-empty. This is an allow-list
     * of safe characters, so whitespace and control characters are excluded, keeping the principal
     * well-formed. Per the spec, no path segment may be the relative modifier {@code .} or {@code ..}
     * (enforced by the per-segment negative look-ahead).
     */
    private static final Pattern SPIFFE_ID_PATTERN = Pattern.compile(
            "^spiffe://[a-z0-9._-]+(?:/(?!\\.{1,2}(?:/|$))[A-Za-z0-9._-]+)+$");

    private SpiffeIdUtil() {
        // to block instantiation
    }

    /**
     * Parses a configured, comma-separated list of SPIFFE header names into an ordered list of
     * trimmed, non-empty names. Supports a single header name as a list of length one.
     *
     * @param configValue the raw config value (e.g. {@code X-Spiffe-Id, X-Workload-Id})
     * @return an ordered, immutable list of header names; empty when the input is blank
     */
    public static List<String> parseHeaderNames(String configValue) {
        if (StringUtils.isBlank(configValue)) {
            return Collections.emptyList();
        }

        List<String> ret = new ArrayList<>();

        for (String name : configValue.split(HEADER_NAMES_SEP)) {
            String trimmed = StringUtils.trimToNull(name);

            if (trimmed != null) {
                ret.add(trimmed);
            }
        }

        return Collections.unmodifiableList(ret);
    }

    /**
     * Validates that the given value is a well-formed SPIFFE ID in the exact form
     * {@code spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>} with non-empty segments.
     *
     * @param spiffeId the raw SPIFFE ID value from the request header
     * @return {@code true} if the value matches the expected SPIFFE ID format; {@code false} otherwise
     */
    public static boolean isValidSpiffeId(String spiffeId) {
        return matchSpiffeId(spiffeId) != null;
    }

    /**
     * Matches the given value against the expected SPIFFE ID format, computing the {@link Matcher} once.
     *
     * @param spiffeId the raw SPIFFE ID value from the request header
     * @return a matched {@link Matcher} if the value is a well-formed SPIFFE ID;
     *         {@code null} if the value is blank or does not match
     */
    private static Matcher matchSpiffeId(String spiffeId) {
        if (StringUtils.isBlank(spiffeId)) {
            return null;
        }

        Matcher matcher = SPIFFE_ID_PATTERN.matcher(spiffeId.trim());

        return matcher.matches() ? matcher : null;
    }
}
