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
package org.apache.ranger.security.web.filter;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.util.StreamUtils;

import javax.servlet.http.HttpServletRequest;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * security="none" on grant/revoke must match the path with and without a trailing slash.
 * A single-segment wildcard does not match the trailing-slash form, which then falls
 * through to the authenticated filter chain.
 */
public class TestGrantRevokeSecurityNonePatterns {
    @Test
    public void pluginAndAssetGrantRevokePatternsMatchTrailingSlash() throws Exception {
        String xml = StreamUtils.copyToString(new ClassPathResource("conf.dist/security-applicationContext.xml").getInputStream(), StandardCharsets.UTF_8);

        assertSecurityNoneCovers(xml, "/service/plugins/services/grant/", "/service/plugins/services/grant/dev_hive");
        assertSecurityNoneCovers(xml, "/service/plugins/services/revoke/", "/service/plugins/services/revoke/dev_hive");
        assertSecurityNoneCovers(xml, "/service/assets/resources/grant", "/service/assets/resources/grant");
        assertSecurityNoneCovers(xml, "/service/assets/resources/revoke", "/service/assets/resources/revoke");

        // A single-segment wildcard matches the service path and misses the trailing slash, which is why these patterns use /**
        assertFalse(matches("/service/plugins/services/grant/*", "/service/plugins/services/grant/dev_hive/"));
    }

    private static void assertSecurityNoneCovers(String xml, String pathPrefix, String path) {
        String pattern = securityNonePattern(xml, pathPrefix);

        assertTrue(matches(pattern, path), pattern + " should match " + path);
        assertTrue(matches(pattern, path + "/"), pattern + " should match " + path + "/");
    }

    // Assumes pattern="…" is the first attribute and is written with no extra whitespace.
    private static String securityNonePattern(String xml, String pathPrefix) {
        String marker = "pattern=\"" + pathPrefix;
        int start = xml.indexOf(marker);

        assertTrue(start >= 0, "missing security=\"none\" pattern for " + pathPrefix);

        int valueStart = start + "pattern=\"".length();
        int valueEnd = xml.indexOf('"', valueStart);

        return xml.substring(valueStart, valueEnd);
    }

    private static boolean matches(String pattern, String path) {
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(request.getServletPath()).thenReturn(path);
        when(request.getPathInfo()).thenReturn(null);
        when(request.getMethod()).thenReturn("POST");

        return new AntPathRequestMatcher(pattern).matches(request);
    }
}
