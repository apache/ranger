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
package org.apache.ranger.authorization.hbase;

import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RangerAuthorizationCoprocessorTest {
    @Test
    public void test_canBeNewed() {
        RangerAuthorizationCoprocessor coprocessor = new RangerAuthorizationCoprocessor();
        assertNotNull(coprocessor);
    }

    @Test
    public void test_getColumnFamilies_happypath() {
    }

    @Test
    public void test_getColumnFamilies_firewalling() {
        // passing null collection should return back an empty map
        RangerAuthorizationCoprocessor coprocessor = new RangerAuthorizationCoprocessor();
        Map<String, Set<String>>       result       = coprocessor.getColumnFamilies(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
        // same for passing in an empty collection
        // result = coprocessor.getColumnFamilies(new HashMap<byte[], ? extends Collection<?>>());
    }
}
