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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

/**
 * Resolves a workload SPIFFE ID from plugin/site configuration.
 *
 * <p>Resolution order: explicit {@code authn.spiffe.value}, identity file
 * ({@code authn.spiffe.file} or the default SPIRE path), then {@code SPIFFE_ID}
 * environment variable.
 */
public final class SpiffeIdentityResolver {
    public static final String PROP_SPIFFE_VALUE = "authn.spiffe.value";
    public static final String PROP_SPIFFE_FILE  = "authn.spiffe.file";
    public static final String ENV_SPIFFE_ID     = "SPIFFE_ID";
    public static final String DEFAULT_SPIFFE_IDENTITY_FILE =
            "/var/run/secrets/spiffe.io/identity/spiffe";

    private static final Logger LOG =
            LoggerFactory.getLogger(SpiffeIdentityResolver.class);

    private SpiffeIdentityResolver() {
        // to block instantiation
    }

    /**
     * Resolves the SPIFFE ID for the given config prefix.
     *
     * @param props        plugin or site configuration properties
     * @param configPrefix prefix such as {@code ranger.hive}
     * @return the resolved SPIFFE ID, or {@code null} when unavailable
     */
    public static String resolve(final Properties props, final String configPrefix) {
        String ret = null;

        if (props != null && StringUtils.isNotBlank(configPrefix)) {
            ret = StringUtils.trimToNull(
                    props.getProperty(configPrefix + "." + PROP_SPIFFE_VALUE));

            if (ret == null) {
                String filePath = StringUtils.trimToNull(
                        props.getProperty(configPrefix + "." + PROP_SPIFFE_FILE));

                if (filePath == null) {
                    filePath = DEFAULT_SPIFFE_IDENTITY_FILE;
                }

                ret = readFirstLine(filePath);

                if (ret == null) {
                    ret = StringUtils.trimToNull(System.getenv(ENV_SPIFFE_ID));
                }
            }
        }

        LOG.debug("resolve(configPrefix={}): ret={}", configPrefix, ret);

        return ret;
    }

    static String readFirstLine(final String filePath) {
        String ret = null;

        if (StringUtils.isNotBlank(filePath)) {
            try {
                Path path = Paths.get(filePath.trim());

                if (Files.isRegularFile(path)) {
                    List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);

                    for (String line : lines) {
                        String trimmed = StringUtils.trimToNull(line);

                        if (trimmed != null) {
                            ret = trimmed;

                            break;
                        }
                    }
                }
            } catch (IOException ex) {
                LOG.debug("Unable to read SPIFFE identity from file {}", filePath, ex);
            }
        }

        return ret;
    }
}
