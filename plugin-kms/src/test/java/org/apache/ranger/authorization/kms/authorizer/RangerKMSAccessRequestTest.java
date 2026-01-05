/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.authorization.kms.authorizer;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RangerKMSAccessRequestTest {
    @Test
    void testConstructor_SetsFieldsCorrectly() {
        String keyName = "abc";
        String accessType = "read";
        String clientIp = "1.1.1.1";
        String userName = "chegde";
        String[] groups = {"group1", "group2"};
        UserGroupInformation ugi = UserGroupInformation.createUserForTesting(userName, groups);

        RangerKMSAccessRequest request = new RangerKMSAccessRequest(keyName, accessType, ugi, clientIp);

        assertNotNull(request.getResource());
        assertEquals(keyName, request.getResource().getValue("keyname"));
        assertEquals(accessType, request.getAccessType());
        assertEquals(userName, request.getUser());
        Set<String> userGroups = request.getUserGroups();
        assertNotNull(userGroups);
        assertTrue(userGroups.contains("group1"));
        assertTrue(userGroups.contains("group2"));
        assertNotNull(request.getAccessTime());
        assertEquals(clientIp, request.getClientIPAddress());
        assertEquals(accessType, request.getAction());
    }
}
