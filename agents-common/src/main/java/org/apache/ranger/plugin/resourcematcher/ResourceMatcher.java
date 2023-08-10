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
import java.util.Comparator;
import java.util.Map;

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
        final boolean                  ret;

        if (matchType == ResourceElementMatchType.SELF) {
            ret = true;
        } else if (matchType == ResourceElementMatchType.PREFIX) {
            ret = matchingScope == ResourceElementMatchingScope.SELF_OR_PREFIX;
        } else if (matchType == ResourceElementMatchType.CHILD) {
            ret = matchingScope == ResourceElementMatchingScope.SELF_OR_CHILD;
        } else {
            ret = false;
        }

        return ret;
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

    public static class PriorityComparator implements Comparator<ResourceMatcher>, Serializable {
        @Override
        public int compare(ResourceMatcher me, ResourceMatcher other) {
            return Integer.compare(me.getPriority(), other.getPriority());
        }
    }
}
