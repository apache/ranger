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

import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RangerIpMatcherTest {
    @BeforeEach
    public void setUp() throws Exception {
    }

    @AfterEach
    public void tearDown() throws Exception {
    }

    @Test
    public void testUnexpected() {
        // this documents some unexpected behavior of the ip matcher
        RangerIpMatcher ipMatcher = createMatcher(new String[] {"1.2.3.*"});

        // NOTE: absurd and downright illegal ipv4 address would match too!
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.2.3.123567")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.2.3..123567")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.2.3.boo")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.2.3.")));

        // wildcard match happens only at the end
        ipMatcher = createMatcher(new String[] {"1.*.3.4"});

        Assertions.assertFalse(ipMatcher.isMatched(createRequest("1.3.3.4")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("1.1.3.4")));
        // it becomes a literal match!
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.*.3.4")));

        // same is true of ipv6
        ipMatcher = createMatcher(new String[] {"99:a9:b9:c9:*"});
        // NOTE: absurd and downright illegal ipv4 address would match too!
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("99:a9:b9:c9:*")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("99:a9:b9:c9:1.3.4")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("99:a9:b9:c9: <:-) ")));
    }

    @Test
    public void test_isWildcardMatched() {
        List<String>    ips     = Arrays.asList("1.2.3.", "1.3.", "2.", "a0:b0:c0:", "a0:b1:", "a2:");
        RangerIpMatcher matcher = new RangerIpMatcher();

        Assertions.assertTrue(matcher.isWildcardMatched(ips, "1.2.3.4"));
        Assertions.assertTrue(matcher.isWildcardMatched(ips, "1.3.3.4"));
        Assertions.assertTrue(matcher.isWildcardMatched(ips, "2.3.3.4"));

        Assertions.assertTrue(matcher.isWildcardMatched(ips, "A0:B0:C0:D0:E0:F0"));
        Assertions.assertTrue(matcher.isWildcardMatched(ips, "A0:B1:C0:D0:E0:F0"));
        Assertions.assertTrue(matcher.isWildcardMatched(ips, "A2:B0:C1:D2:E3:F4"));

        Assertions.assertFalse(matcher.isWildcardMatched(ips, "1.2.33.4"));
        Assertions.assertFalse(matcher.isWildcardMatched(ips, "1.33.3.4"));
        Assertions.assertFalse(matcher.isWildcardMatched(ips, "22.3.3.4"));

        Assertions.assertFalse(matcher.isWildcardMatched(ips, "A0:B0:00:D0:E0:F0"));
        Assertions.assertFalse(matcher.isWildcardMatched(ips, "A0:B2:C0:D0:E0:F0"));
        Assertions.assertFalse(matcher.isWildcardMatched(ips, "22:B0:C1:D2:E3:F4"));
    }

    @Test
    public void test_isExactlyMatched() {
        List<String>    ips     = Arrays.asList("1.2.3.1", "1.2.3.11", "1.2.3.111", "a0:b0:c0:d0:e0:f0", "a0:b0:c0:d0:e0:f1", "a0:b0:c0:d0:e0:f2");
        RangerIpMatcher matcher = new RangerIpMatcher();

        Assertions.assertTrue(matcher.isExactlyMatched(ips, "1.2.3.1"));
        Assertions.assertTrue(matcher.isExactlyMatched(ips, "1.2.3.111"));
        Assertions.assertTrue(matcher.isExactlyMatched(ips, "A0:B0:C0:D0:E0:F1"));
        Assertions.assertTrue(matcher.isExactlyMatched(ips, "a0:b0:c0:d0:e0:f1"));

        Assertions.assertFalse(matcher.isExactlyMatched(ips, "1.2.3.2"));
        Assertions.assertFalse(matcher.isExactlyMatched(ips, "a0:b0:c0:d0:e0:f3"));
    }

    @Test
    public void test_extractIp() {
        RangerIpMatcher matcher = new RangerIpMatcher();

        Assertions.assertNull(matcher.extractIp(null));

        RangerAccessRequest request = mock(RangerAccessRequest.class);

        when(request.getClientIPAddress()).thenReturn(null);
        Assertions.assertNull(matcher.extractIp(request));

        when(request.getClientIPAddress()).thenReturn("anIp"); // note ip address is merely a string.  It can be any string.
        Assertions.assertEquals("anIp", matcher.extractIp(request));
    }

    @Test
    public void test_digestIp() {
        // comlete wildcards get reduced to empty string.
        RangerIpMatcher matcher = new RangerIpMatcher();

        Assertions.assertEquals("", matcher.digestPolicyIp("*"));
        Assertions.assertEquals("", matcher.digestPolicyIp("*.*"));
        Assertions.assertEquals("", matcher.digestPolicyIp("*.*.*"));
        Assertions.assertEquals("", matcher.digestPolicyIp("*.*.*.*"));
        Assertions.assertEquals("", matcher.digestPolicyIp("*:*:*:*"));
        Assertions.assertEquals("", matcher.digestPolicyIp("*:*:*:*:*:*"));

        // wildcard parts get dropped; retails trailing ./: to avoid doing partial number match
        Assertions.assertEquals("10.", matcher.digestPolicyIp("10.*"));
        Assertions.assertEquals("10.", matcher.digestPolicyIp("10.*.*"));
        Assertions.assertEquals("10.", matcher.digestPolicyIp("10.*.*.*"));
        Assertions.assertEquals("10.20.", matcher.digestPolicyIp("10.20.*"));
        Assertions.assertEquals("10.20.", matcher.digestPolicyIp("10.20.*.*"));
        Assertions.assertEquals("10.20.30.", matcher.digestPolicyIp("10.20.30.*"));

        // ipv6 digested values are also lower cased to ensure sensible comparison later
        Assertions.assertEquals("a0:", matcher.digestPolicyIp("A0:*"));
        Assertions.assertEquals("a0:", matcher.digestPolicyIp("a0:*:*"));
        Assertions.assertEquals("a0:", matcher.digestPolicyIp("A0:*:*:*"));
        Assertions.assertEquals("a0:b0:c0:", matcher.digestPolicyIp("A0:B0:C0:*"));
    }

    @Test
    public void test_integration() {
        RangerIpMatcher ipMatcher = createMatcher(null);

        // Matcher initialized with null policy should behave sensibly!  It matches everything!
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.2.3.4")));

        // empty ip-address list is same as null, i.e. matchs anything!
        ipMatcher = createMatcher(new String[] {});

        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.2.3.4")));

        // wildcard address will match anything -- ipv4 and ipv6 addresses
        ipMatcher = createMatcher(new String[] {"*"});

        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.2.3.4")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1:2:3:4:5:6")));

        // partial wildcard matches work as expected.
        ipMatcher = createMatcher(new String[] {"1.2.3.*"});

        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.2.3.4")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.2.3.123")));
        // NOTE: absurd ipv4 address but it should match too!
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.2.3.123567")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.2.3..123567")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("1.2.3.boo")));
        // mismatches caught correctly
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("1.2.31.123567")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("1.2.33.123567")));
        // no address has special meaning
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("1.2.0.0")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("1.2.255.255")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("1.2.254.254")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("0.0.0.0")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("255.255.255.255")));

        // wild card can be more than one octets
        ipMatcher = createMatcher(new String[] {"11.22.*.*"});

        Assertions.assertTrue(ipMatcher.isMatched(createRequest("11.22.33.4")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("11.22.33.44")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("11.22.253.190")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("11.22.253.190")));
        // mismatches
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("11.222.253.190")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("11.21.253.190")));

        // one need't provide all the octets; missing ones are treaetd as wild cards
        ipMatcher = createMatcher(new String[] {"193.214.*"}); // note just 3 octets in pattern

        Assertions.assertTrue(ipMatcher.isMatched(createRequest("193.214.3.4")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("193.21.253.190")));
        // can't match ipv6 address using a ipv4 policy
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("193:214:3:4")));

        // same holds for ipv6 addresses
        ipMatcher = createMatcher(new String[] {"193:214:*"});
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("193:214:3:4:5:6")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("193:214:13:94:a90:b4f")));
        // mismatches work as expected
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("193:215:13:94:a90:b4f")));
        // can't match ipv4 address against ipv6 policy
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("193.214.3.4")));

        // direct match works as expected
        ipMatcher = createMatcher(new String[] {"99:a9:b9:c9:d9:e9"});

        Assertions.assertTrue(ipMatcher.isMatched(createRequest("99:a9:b9:c9:d9:e9")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("99:a9:b9:c9:d0:e9")));

        // Matcher can support multiple patterns of different domains - a mix of ipv4 and ipv6 addresses
        ipMatcher = createMatcher(new String[] {"10.20.30.*", "99:a9:b9:c9:d9:*"});
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("99:a9:b9:c9:d9:e9")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("99:a9:b9:c9:d9:e9")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("99:a9:b9:c9:dd:e9")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("89:a9:b9:c9:d9:e9")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("10.20.30.10")));
        Assertions.assertTrue(ipMatcher.isMatched(createRequest("10.20.30.20")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("10.20.3.10")));
        Assertions.assertFalse(ipMatcher.isMatched(createRequest("10.20.33.10")));
    }

    RangerIpMatcher createMatcher(String[] ipArray) {
        RangerIpMatcher matcher = new RangerIpMatcher();

        if (ipArray == null) {
            matcher.setConditionDef(null);
            matcher.setPolicyItemCondition(null);
        } else {
            RangerPolicyItemCondition condition = mock(RangerPolicyItemCondition.class);
            List<String>              addresses = Arrays.asList(ipArray);

            when(condition.getValues()).thenReturn(addresses);
            matcher.setConditionDef(null);
            matcher.setPolicyItemCondition(condition);
        }

        matcher.init();

        return matcher;
    }

    RangerAccessRequest createRequest(String requestIp) {
        RangerAccessRequest request = mock(RangerAccessRequest.class);

        when(request.getClientIPAddress()).thenReturn(requestIp);

        return request;
    }
}
