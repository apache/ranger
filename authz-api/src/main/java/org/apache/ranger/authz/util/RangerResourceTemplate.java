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

package org.apache.ranger.authz.util;

import org.apache.ranger.authz.api.RangerAuthzException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_RESOURCE_TEMPLATE_UNEXPECTED_MARKER_AT;

public class RangerResourceTemplate {
    private static final Logger LOG = LoggerFactory.getLogger(RangerResourceTemplate.class);

    private static final char RESOURCE_START_CHAR = '{';
    private static final char RESOURCE_END_CHAR   = '}';
    private static final char ESCAPE_CHAR         = '\\';

    private final String   template;
    private final String[] resources;
    private final String   prefix;
    private final String[] separators;
    private final String   suffix;

    public RangerResourceTemplate(String template) throws RangerAuthzException {
        List<String>  resources    = new ArrayList<>();
        List<String>  separators   = new ArrayList<>();
        String        prefix       = null;
        boolean       isInEscape   = false;
        boolean       isInResource = false;
        StringBuilder currentToken = new StringBuilder();

        for (int i = 0; i < template.length(); i++) {
            char c = template.charAt(i);

            if (isInEscape) {
                currentToken.append(c);

                isInEscape = false;
            } else if (c == ESCAPE_CHAR) {
                isInEscape = true;
            } else if (c == RESOURCE_START_CHAR) {
                if (isInResource) {
                    throw new RangerAuthzException(INVALID_RESOURCE_TEMPLATE_UNEXPECTED_MARKER_AT, template, RESOURCE_START_CHAR, i);
                }

                if (prefix == null) {
                    prefix = currentToken.toString();
                } else {
                    separators.add(currentToken.toString());
                }

                isInResource = true;

                currentToken.setLength(0);
            } else if (c == RESOURCE_END_CHAR) {
                if (!isInResource) {
                    throw new RangerAuthzException(INVALID_RESOURCE_TEMPLATE_UNEXPECTED_MARKER_AT, template, RESOURCE_END_CHAR, i);
                }

                resources.add(currentToken.toString());

                isInResource = false;

                currentToken.setLength(0);
            } else {
                currentToken.append(c);
            }
        }

        this.template   = template;
        this.resources  = resources.toArray(new String[0]);
        this.separators = separators.toArray(new String[0]);
        this.prefix     = prefix;
        this.suffix     = currentToken.toString();
    }

    public String getTemplate() {
        return template;
    }

    public Map<String, String> parse(String resource) {
        Map<String, String> ret = null;

        if (resource == null || resource.isEmpty()) {
            LOG.debug("parse(resource='{}', template='{}'): empty or null resource", resource, template);
        } else if (!resource.startsWith(prefix)) {
            LOG.debug("parse(resource='{}', template='{}'): resource does not start with prefix {}", resource, template, prefix);
        } else if (!resource.endsWith(suffix)) {
            LOG.debug("parse(resource='{}', template='{}'): resource does not end with suffix {}", resource, template, suffix);
        } else {
            ret = new HashMap<>();

            int tokenStartPos = prefix.length();

            for (int i = 0; i < this.separators.length; i++) {
                String sep    = this.separators[i];
                int    idxSep = resource.indexOf(sep, tokenStartPos);

                LOG.debug("Separator '{}' found at {}: tokenStartPos={}", sep, idxSep, tokenStartPos);

                if (idxSep == -1) {
                    ret = null;

                    break;
                } else {
                    String value = resource.substring(tokenStartPos, idxSep);

                    ret.put(this.resources[i], value);
                }

                tokenStartPos = idxSep + sep.length();
            }

            if (ret != null && tokenStartPos != -1) {
                String name  = this.resources[this.resources.length - 1];
                String value = resource.substring(tokenStartPos, resource.length() - suffix.length());

                ret.put(name, value);
            }
        }

        LOG.debug("parse(resource='{}', template='{}'): ret={}", resource, template, ret);

        return ret;
    }

    public String formatResource(Map<String, String> resourceMap) {
        StringBuilder ret = new StringBuilder(prefix);

        for (int i = 0; i < resources.length; i++) {
            String name  = resources[i];
            String value = resourceMap.get(name);

            if (value == null) {
                value = "";
            }

            ret.append(value);

            if (i < separators.length) {
                ret.append(separators[i]);
            }
        }

        ret.append(suffix);

        LOG.debug("createResource(resourceMap={}, template='{}'): ret='{}'", resourceMap, template, ret);

        return ret.toString();
    }

    @Override
    public String toString() {
        return "RangerResourceTemplate{" +
                "template=" + template +
                ", resources='" + String.join(",", resources) + "'" +
                ", prefix='" + prefix + "'" +
                ", separators='" + String.join(",", separators) + "'" +
                ", suffix='" + suffix + "'" +
                "}";
    }
}
