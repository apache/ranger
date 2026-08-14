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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.common.PropertiesUtil;

/**
 * Ranger Admin JWT (Bearer token) authentication configuration.
 *
 * JWT authentication is provided by {@link RangerJwtAuthFilter} / {@link RangerJwtAuthWrapper}
 * (RANGER-3739). Configuration uses the {@code ranger.admin.jwt.*} namespace. For backward
 * compatibility with deployments configured before the Knox SSO filter was removed, the
 * legacy {@code ranger.sso.*} property names are still honored as a fallback.
 */
public final class RangerJwtAuthConfig {
    public static final String PROVIDER_URL              = "ranger.admin.jwt.providerurl";
    public static final String PUBLIC_KEY                = "ranger.admin.jwt.publickey";
    public static final String AUDIENCES                 = "ranger.admin.jwt.audiences";
    public static final String ISSUER                    = "ranger.admin.jwt.issuer";
    public static final String BROWSER_USERAGENT         = "ranger.admin.jwt.browser.useragent";
    public static final String DEFAULT_BROWSER_USERAGENT = "ranger.default.browser-useragents";
    public static final String HEALTH_CHECK_URI          = "/actuator/health";

    // Deprecated legacy property names (pre RANGER-685 Knox SSO removal). Retained as fallbacks only.
    private static final String LEGACY_PROVIDER_URL      = "ranger.sso.providerurl";
    private static final String LEGACY_PUBLIC_KEY        = "ranger.sso.publicKey";
    private static final String LEGACY_AUDIENCES         = "ranger.sso.audiences";
    private static final String LEGACY_ISSUER            = "ranger.sso.issuer";
    private static final String LEGACY_BROWSER_USERAGENT = "ranger.sso.browser.useragent";

    private RangerJwtAuthConfig() {
    }

    public static String getProviderUrl() {
        return resolve(PROVIDER_URL, LEGACY_PROVIDER_URL);
    }

    public static String getPublicKey() {
        return resolve(PUBLIC_KEY, LEGACY_PUBLIC_KEY);
    }

    public static String getAudiences() {
        return resolve(AUDIENCES, LEGACY_AUDIENCES);
    }

    public static String getIssuer() {
        return resolve(ISSUER, LEGACY_ISSUER);
    }

    public static String getBrowserUserAgent() {
        return resolve(BROWSER_USERAGENT, LEGACY_BROWSER_USERAGENT);
    }

    private static String resolve(String preferredKey, String legacyKey) {
        String value = PropertiesUtil.getProperty(preferredKey);

        if (StringUtils.isBlank(value)) {
            value = PropertiesUtil.getProperty(legacyKey);
        }

        return value;
    }
}
