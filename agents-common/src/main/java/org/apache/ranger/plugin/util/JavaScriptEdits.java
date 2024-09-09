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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaScriptEdits {
    private static final Logger LOG = LoggerFactory.getLogger(JavaScriptEdits.class);

    private static final String  DOUBLE_BRACKET_START   = "[[";
    private static final String  DOUBLE_BRACKET_END     = "]]";
    private static final String  DOUBLE_BRACKET_REGEX   = "\\[\\[([}{\\$\"a-zA-Z0-9_.\\[\\]]+)(\\,['\\\"](.+?)['\\\"])*\\]\\]"; // regex: /\[\[([a-zA-Z0-9_.\[\]]+)(\,['"](.+)['"])*\]\]/g;
    private static final Pattern DOUBLE_BRACKET_PATTERN = Pattern.compile(DOUBLE_BRACKET_REGEX);

    public static boolean hasDoubleBrackets(String str) {
        return StringUtils.contains(str, DOUBLE_BRACKET_START) && StringUtils.contains(str, DOUBLE_BRACKET_END);
    }

    /* some examples:
    tag-based access policy:
        original: [[TAG.value]].intersects([[USER[TAG._type]]])
        replaced: TAG.value.split(",").intersects(USER[TAG._type].split(","))
    Row-filter policy:
        original: ${{[["$USER.eventType",'|']]}}.includes(eventType)
        replaced: ${{"$USER.eventType".split("|")}}.includes(jsonAttr.eventType)
    */
    public static String replaceDoubleBrackets(String str) {
        // Besides trivial inputs, re has been tested on ${{USER.x}} and multiple [[]]'s
        String ret = str;

        for (Matcher m = DOUBLE_BRACKET_PATTERN.matcher(str); m.find(); ) {
            String tokenToReplace = m.group(0);
            String expr           = m.group(1);
            String delimiterSpec  = m.group(2);
            String delimiter      = m.group(3);

            if (delimiter == null) {
                delimiter = ",";
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("replaceDoubleBrackets({}): tokenToReplace={} expr={} delimiterSpec={} delimiter={}", str, tokenToReplace, expr, delimiterSpec, delimiter);
            }

            ret = ret.replace(tokenToReplace, expr + ".split(\"" + delimiter + "\")");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== replaceDoubleBrackets({}): ret={}", str, ret);
        }

        return ret;
    }
}

