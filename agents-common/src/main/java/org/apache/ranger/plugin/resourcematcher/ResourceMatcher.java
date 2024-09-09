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

package org.apache.ranger.plugin.resourcematcher;

import org.apache.commons.io.IOCase;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceElementMatchType;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceElementMatchingScope;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerRequestExprResolver;
import org.apache.ranger.plugin.util.StringTokenReplacer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

abstract class ResourceMatcher {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceMatcher.class);

    protected final String                    value;
    protected final RangerRequestExprResolver exprResolver;
    protected       StringTokenReplacer       tokenReplacer;

    static final int DYNAMIC_EVALUATION_PENALTY = 8;

    ResourceMatcher(String value, Map<String, String> options) {
        this.value = value;

        if (RangerAbstractResourceMatcher.getOptionReplaceReqExpressions(options) && RangerRequestExprResolver.hasExpressions(value)) {
            exprResolver = new RangerRequestExprResolver(value, null); // TODO: serviceType
        } else {
            exprResolver = null;
        }
    }

    abstract boolean isMatch(String resourceValue, Map<String, Object> evalContext);

    abstract boolean isPrefixMatch(String resourceValue, Map<String, Object> evalContext);

    abstract boolean isChildMatch(String resourceValue, Map<String, Object> evalContext);

    final boolean isMatch(String resourceValue, ResourceElementMatchingScope matchingScope, Map<String, Object> evalContext) {
        final ResourceElementMatchType matchType = getMatchType(resourceValue, matchingScope, evalContext);

        return isMatch(matchType, matchingScope);
    }

    final ResourceElementMatchType getMatchType(String resourceValue, ResourceElementMatchingScope matchingScope, Map<String, Object> evalContext) {
        ResourceElementMatchType ret = ResourceElementMatchType.NONE;

        if (isMatch(resourceValue, evalContext)) {
            ret = ResourceElementMatchType.SELF;
        } else {
            if (matchingScope == ResourceElementMatchingScope.SELF_OR_PREFIX) {
                if (isPrefixMatch(resourceValue, evalContext)) {
                    ret = ResourceElementMatchType.PREFIX;
                }
            } else if (matchingScope == ResourceElementMatchingScope.SELF_OR_CHILD) {
                if (isChildMatch(resourceValue, evalContext)) {
                    ret = ResourceElementMatchType.CHILD;
                }
            }
        }

        return ret;
    }

    abstract int getPriority();

    boolean isMatchAny() { return value != null && value.length() == 0; }

    boolean getNeedsDynamicEval() {
        return exprResolver != null || tokenReplacer != null;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "(" + this.value + ")";
    }

    void setDelimiters(char startDelimiterChar, char endDelimiterChar, char escapeChar, String tokenPrefix) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> setDelimiters(value= " + value + ", startDelimiter=" + startDelimiterChar +
                    ", endDelimiter=" + endDelimiterChar + ", escapeChar=" + escapeChar + ", prefix=" + tokenPrefix);
        }

        if(exprResolver != null || StringTokenReplacer.hasToken(value, startDelimiterChar, endDelimiterChar, escapeChar)) {
            tokenReplacer = new StringTokenReplacer(startDelimiterChar, endDelimiterChar, escapeChar, tokenPrefix);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== setDelimiters(value= " + value + ", startDelimiter=" + startDelimiterChar +
                    ", endDelimiter=" + endDelimiterChar + ", escapeChar=" + escapeChar + ", prefix=" + tokenPrefix);
        }
    }

    String getExpandedValue(Map<String, Object> evalContext) {
        String ret = value;

        if (exprResolver != null) {
            RangerAccessRequest accessRequest = RangerAccessRequestUtil.getRequestFromContext(evalContext);

            if (accessRequest != null) {
                ret = exprResolver.resolveExpressions(accessRequest);
            }
        }

        if (tokenReplacer != null) {
            ret = tokenReplacer.replaceTokens(ret, evalContext);
        }

        return ret;
    }

    public static boolean startsWithAnyChar(String value, String startChars) {
        boolean ret = false;

        if (value != null && value.length() > 0 && startChars != null) {
            ret = StringUtils.contains(startChars, value.charAt(0));
        }

        return ret;
    }

    public static boolean isMatch(ResourceElementMatchType matchType, ResourceElementMatchingScope matchingScope) {
        final boolean ret;

        switch (matchType) {
            case SELF:
                ret = true;
                break;

            case CHILD:
                ret = matchingScope == ResourceElementMatchingScope.SELF_OR_CHILD;
                break;

            case PREFIX:
                ret = matchingScope == ResourceElementMatchingScope.SELF_OR_PREFIX;
                break;

            case NONE:
                ret = false;
                break;

            default:
                LOG.error("invalid ResourceElementMatchType: {}}", matchType);

                ret = false;
        }

        return ret;
    }

    // modified version of FilenameUtils.wildcardMatch(), to check if value is a prefix match for wildcardMatcher
    public static boolean wildcardPrefixMatch(String value, String wildcardMatcher, IOCase caseSensitivity) {
        if (value == null && wildcardMatcher == null) {
            return true;
        } else if (value == null || wildcardMatcher == null) {
            return false;
        }

        if (caseSensitivity == null) {
            caseSensitivity = IOCase.SENSITIVE;
        }

        List<String> wcsTokens = splitOnTokens(wildcardMatcher);
        boolean      anyChars  = false;
        int          textIdx   = 0;
        int          wcsIdx    = 0;
        Stack<int[]> backtrack = new Stack<>();

        do {
            if (backtrack.size() > 0) {
                int[] array = backtrack.pop();

                wcsIdx   = array[0];
                textIdx  = array[1];
                anyChars = true;
            }

            for(; wcsIdx < wcsTokens.size(); ++wcsIdx) {
                String wcsToken = wcsTokens.get(wcsIdx);

                if (wcsToken.equals("?")) {
                    ++textIdx;

                    if (textIdx > value.length()) {
                        break;
                    }

                    anyChars = false;
                } else if (wcsToken.equals("*")) {
                    anyChars = true;

                    if (wcsIdx == wcsTokens.size() - 1) {
                        textIdx = value.length();
                    }
                } else {
                    // changes from FilenameUtils.wildcardMatch(): added following 3 lines to check if value is a prefix match for wildcardMatcher
                    if (wcsToken.length() > (value.length() - textIdx)) {
                        wcsToken = wcsToken.substring(0, value.length() - textIdx);
                    }

                    if (anyChars) {
                        textIdx = caseSensitivity.checkIndexOf(value, textIdx, wcsToken);

                        if (textIdx == -1) {
                            break;
                        }

                        int repeat = caseSensitivity.checkIndexOf(value, textIdx + 1, wcsToken);

                        if (repeat >= 0) {
                            backtrack.push(new int[]{wcsIdx, repeat});
                        }
                    } else if (!caseSensitivity.checkRegionMatches(value, textIdx, wcsToken)) {
                        break;
                    }

                    textIdx += wcsToken.length();

                    anyChars = false;
                }
            }

            // changes from FilenameUtils.wildcardMatch(): replaced the condition in 'if' below to check if value is a prefix match for wildcardMatcher
            //   original if: if (wcsIdx == wcsTokens.size() && textIdx == value.length())
            if (wcsIdx == wcsTokens.size() || textIdx == value.length()) {
                return true;
            }
        } while (backtrack.size() > 0);

        return anyChars;
    }

    static List<String> splitOnTokens(String text) {
        if (text.indexOf(63) == -1 && text.indexOf(42) == -1) {
            return Collections.singletonList(text);
        } else {
            char[]        array    = text.toCharArray();
            List<String>  list     = new ArrayList<>(2);
            StringBuilder buffer   = new StringBuilder();
            char          prevChar = 0;
            char[]        arr$     = array;
            int           len$     = array.length;

            for(int i$ = 0; i$ < len$; ++i$) {
                char ch = arr$[i$];

                if (ch != '?' && ch != '*') {
                    buffer.append(ch);
                } else {
                    if (buffer.length() != 0) {
                        list.add(buffer.toString());
                        buffer.setLength(0);
                    }

                    if (ch == '?') {
                        list.add("?");
                    } else if (prevChar != '*') {
                        list.add("*");
                    }
                }

                prevChar = ch;
            }

            if (buffer.length() != 0) {
                list.add(buffer.toString());
            }

            return list;
        }
    }

    public static class PriorityComparator implements Comparator<ResourceMatcher>, Serializable {
        @Override
        public int compare(ResourceMatcher me, ResourceMatcher other) {
            return Integer.compare(me.getPriority(), other.getPriority());
        }
    }
}
