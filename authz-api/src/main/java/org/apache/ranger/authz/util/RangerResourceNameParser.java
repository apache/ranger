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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_RESOURCE_EMPTY_VALUE;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_RESOURCE_TEMPLATE_EMPTY_VALUE;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_RESOURCE_VALUE;

public class RangerResourceNameParser {
    private static final Logger LOG = LoggerFactory.getLogger(RangerResourceNameParser.class);

    public static final char ESCAPE_CHAR    = '\\';
    public static final char SEPARATOR_CHAR = '/';

    private static final String   SEPARATOR_STRING  = String.valueOf(SEPARATOR_CHAR);
    private static final String   ESCAPED_SEPARATOR = "\\\\" + SEPARATOR_STRING;
    private static final Pattern  SEPARATOR_PATTERN = Pattern.compile(SEPARATOR_STRING);
    private static final String[] EMPTY_ARRAY       = new String[0];

    private final String   template;   // examples: database/table/column, bucket/volume/path
    private final String[] resources;  // examples: [database, table, column],   [bucket, volume, path]

    public RangerResourceNameParser(String template) throws RangerAuthzException {
        if (StringUtils.isBlank(template)) {
            throw new RangerAuthzException(INVALID_RESOURCE_TEMPLATE_EMPTY_VALUE);
        }

        this.template  = template;
        this.resources = template.split(SEPARATOR_STRING); // assumption: '/' is not a valid character in resource names
    }

    public String getTemplate() {
        return template;
    }

    public String getResourceType() {
        return resources[resources.length - 1];
    }

    public int count() {
        return resources.length;
    }

    public String resourceAt(int index) {
        return resources[index];
    }

    public String[] parseToArray(final String resourceName) throws RangerAuthzException {
        if (StringUtils.isBlank(resourceName)) {
            throw new RangerAuthzException(INVALID_RESOURCE_EMPTY_VALUE);
        }

        final String[]      ret         = new String[resources.length];
        final int           nameLen     = resourceName.length();
        final StringBuilder token       = new StringBuilder();
        int                 idxToken    = 0;
        boolean             isLastToken = resources.length == 1;
        boolean             isInEscape  = false;

        for (int i = 0; i < nameLen; i++) {
            char c = resourceName.charAt(i);

            if (c == ESCAPE_CHAR) {
                if (!isInEscape) {
                    isInEscape = true;

                    continue;
                }
            } else if (c == SEPARATOR_CHAR) {
                if (!isInEscape) {
                    if (!isLastToken) { // for last token, '/' is not a separator
                        ret[idxToken++] = token.toString();

                        token.setLength(0);

                        isLastToken = idxToken == (resources.length - 1);

                        continue;
                    }
                }
            }

            token.append(c);

            isInEscape = false;
        }

        ret[idxToken] = token.toString();

        if (!isLastToken) {
            throw new RangerAuthzException(INVALID_RESOURCE_VALUE, resourceName, template);
        }

        LOG.debug("parseToArray(resource='{}', template='{}'): ret={}", resourceName, template, ret);

        return ret;
    }

    public Map<String, String> parseToMap(final String resourceName) throws RangerAuthzException {
        final String[]            arr = parseToArray(resourceName);
        final Map<String, String> ret = new HashMap<>(arr.length);

        for (int i = 0; i < arr.length; i++) {
            ret.put(resources[i], arr[i]);
        }

        LOG.debug("parseToMap(resourceName='{}', template='{}'): ret={}", resourceName, template, ret);

        return ret;
    }

    public String toResourceName(String[] values) {
        StringBuilder ret = new StringBuilder();

        if (values == null) {
            values = EMPTY_ARRAY;
        }

        for (int i = 0; i < resources.length; i++) {
            String  value  = values.length > i ? values[i] : null;
            boolean isLast = i == (resources.length - 1);

            if (value == null) {
                value = "";
            }

            if (!isLast) { // escape '/' in all but the last resource
                value = escapeIfNeeded(value);
            }

            if (i > 0) {
                ret.append(SEPARATOR_CHAR);
            }

            ret.append(value);
        }

        LOG.debug("toResourceName(values={}, template='{}'): ret='{}'", values, template, ret);

        return ret.toString();
    }

    public String toResourceName(Map<String, String> values) {
        StringBuilder ret = new StringBuilder();

        if (values == null) {
            values = Collections.emptyMap();
        }

        for (int i = 0; i < resources.length; i++) {
            String  value  = values.get(resources[i]);
            boolean isLast = i == (resources.length - 1);

            if (value == null) {
                value = "";
            }

            if (!isLast) { // escape '/' in all but the last resource
                value = escapeIfNeeded(value);
            }

            if (i > 0) {
                ret.append(SEPARATOR_CHAR);
            }

            ret.append(value);
        }

        LOG.debug("toResourceName(values={}, template='{}'): ret='{}'", values, template, ret);

        return ret.toString();
    }

    @Override
    public String toString() {
        return "RangerResourceTemplate{" +
                "template=" + template +
                ", resources='" + String.join(SEPARATOR_STRING, resources) + "'" +
                "}";
    }

    private String escapeIfNeeded(String value) {
        if (value.contains(SEPARATOR_STRING)) {
            return SEPARATOR_PATTERN.matcher(value).replaceAll(ESCAPED_SEPARATOR);
        } else {
            return value;
        }
    }
}
