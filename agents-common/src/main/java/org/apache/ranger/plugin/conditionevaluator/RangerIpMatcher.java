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

package org.apache.ranger.plugin.conditionevaluator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Credits: Large parts of this file have been lifted as is from org.apache.ranger.pdp.knox.URLBasedAuthDB.  Credits for those are due to Dilli Arumugam.
 *
 * @author alal
 */
public class RangerIpMatcher extends RangerAbstractConditionEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(RangerIpMatcher.class);

    static final Pattern allWildcards         = Pattern.compile("^((\\*(\\.\\*)*)|(\\*(:\\*)*))$"); // "*", "*.*", "*.*.*", "*:*", "*:*:*", etc.
    static final Pattern trailingWildcardsIp4 = Pattern.compile("(\\.\\*)+$"); // "blah.*", "blah.*.*", etc.
    static final Pattern trailingWildcardsIp6 = Pattern.compile("(:\\*)+$");   // "blah:*", "blah:*:*", etc.

    private final List<String> exactIps    = new ArrayList<>();
    private final List<String> wildCardIps = new ArrayList<>();
    private       boolean      allowAny;

    @Override
    public void init() {
        LOG.debug("==> RangerIpMatcher.init({})", condition);

        super.init();

        // NOTE: this evaluator does not use conditionDef!
        if (condition == null) {
            LOG.debug("init: null policy condition! Will match always!");

            allowAny = true;
        } else if (CollectionUtils.isEmpty(condition.getValues())) {
            LOG.debug("init: empty conditions collection on policy condition!  Will match always!");

            allowAny = true;
        } else if (condition.getValues().contains("*")) {
            allowAny = true;

            LOG.debug("init: wildcard value found.  Will match always.");
        } else {
            for (String ip : condition.getValues()) {
                String digestedIp = digestPolicyIp(ip);

                if (digestedIp.isEmpty()) {
                    LOG.debug("init: digested ip was empty! Will match always");

                    allowAny = true;
                } else if (digestedIp.equals(ip)) {
                    exactIps.add(ip);
                } else {
                    wildCardIps.add(digestedIp);
                }
            }
        }

        LOG.debug("<== RangerIpMatcher.init({}): exact-ips[{}], wildcard-ips[{}]", condition, exactIps, wildCardIps);
    }

    @Override
    public boolean isMatched(final RangerAccessRequest request) {
        LOG.debug("==> RangerIpMatcher.isMatched({})", request);

        boolean ipMatched = true;

        if (allowAny) {
            LOG.debug("isMatched: allowAny flag is true.  Matched!");
        } else {
            String requestIp = extractIp(request);

            if (requestIp == null) {
                LOG.debug("isMatched: couldn't get ip address from request.  Ok.  Implicitly matched!");
            } else {
                ipMatched = isWildcardMatched(wildCardIps, requestIp) || isExactlyMatched(exactIps, requestIp);
            }
        }

        LOG.debug("<== RangerIpMatcher.isMatched({}): {}", request, ipMatched);

        return ipMatched;
    }

    String digestPolicyIp(final String policyIp) {
        LOG.debug("==> RangerIpMatcher.digestPolicyIp({})", policyIp);

        String  result;
        Matcher matcher = allWildcards.matcher(policyIp);

        if (matcher.matches()) {
            LOG.debug("digestPolicyIp: policyIP[{}] all wildcards.", policyIp);

            result = "";
        } else if (policyIp.contains(".")) {
            matcher = trailingWildcardsIp4.matcher(policyIp);

            result  = matcher.replaceFirst(".");
        } else {
            matcher = trailingWildcardsIp6.matcher(policyIp);

            // also lower cases the ipv6 items
            result = matcher.replaceFirst(":").toLowerCase();
        }

        LOG.debug("<== RangerIpMatcher.digestPolicyIp({}): {}", policyIp, result);

        return result;
    }

    boolean isWildcardMatched(final List<String> ips, final String requestIp) {
        LOG.debug("==> RangerIpMatcher.isWildcardMatched({}, {})", ips, requestIp);

        boolean          matchFound = false;
        Iterator<String> iterator   = ips.iterator();

        while (iterator.hasNext() && !matchFound) {
            String ip = iterator.next();

            if (requestIp.contains(".") && requestIp.startsWith(ip)) {
                LOG.debug("Wildcard Policy IP[{}] matches request IPv4[{}].", ip, requestIp);

                matchFound = true;
            } else if (requestIp.toLowerCase().startsWith(ip)) {
                LOG.debug("Wildcard Policy IP[{}] matches request IPv6[{}].", ip, requestIp);

                matchFound = true;
            } else {
                LOG.debug("Wildcard policy IP[{}] did not match request IP[{}].", ip, requestIp);
            }
        }

        LOG.debug("<== RangerIpMatcher.isWildcardMatched({}, {}): {}", ips, requestIp, matchFound);

        return matchFound;
    }

    boolean isExactlyMatched(final List<String> ips, final String requestIp) {
        LOG.debug("==> RangerIpMatcher.isExactlyMatched({}, {})", ips, requestIp);

        final boolean matchFound;

        if (requestIp.contains(".")) {
            matchFound = ips.contains(requestIp);
        } else {
            matchFound = ips.contains(requestIp.toLowerCase());
        }

        LOG.debug("<== RangerIpMatcher.isExactlyMatched({}, {}): {}", ips, requestIp, matchFound);

        return matchFound;
    }

    /**
     * Extracts and returns the ip address from the request.  Returns null if one can't be obtained out of the request.
     *
     * @param request
     * @return
     */
    String extractIp(final RangerAccessRequest request) {
        LOG.debug("==> RangerIpMatcher.extractIp({})", request);

        String ip = null;

        if (request == null) {
            LOG.debug("isMatched: Unexpected: null request object!");
        } else {
            ip = request.getClientIPAddress();

            if (ip == null) {
                LOG.debug("isMatched: Unexpected: Client ip in request object is null!");
            }
        }

        LOG.debug("<== RangerIpMatcher.extractIp({}): {}", request, ip);

        return ip;
    }
}
