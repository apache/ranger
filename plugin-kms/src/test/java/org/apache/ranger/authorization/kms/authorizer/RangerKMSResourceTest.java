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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RangerKMSResourceTest {
    @Test
    void testConstructor_WithNonNullKeyname() {
        String keyname = "key1";
        RangerKMSResource resource = new RangerKMSResource(keyname);
        assertEquals(keyname, resource.getValue("keyname"));
        assertTrue(resource.exists("keyname"));
    }

    @Test
    void testConstructor_WithNullKeyname() {
        RangerKMSResource resource = new RangerKMSResource(null);
        assertNull(resource.getValue("keyname"));
        assertFalse(resource.exists("keyname"));
    }

    @Test
    void testSetValue() {
        RangerKMSResource resource = new RangerKMSResource("initial");
        resource.setValue("key2", "updatedKey2");
        assertEquals("updatedKey2", resource.getValue("key2"));
        resource.setValue("key2", null);
        assertNull(resource.getValue("key2"));
        assertFalse(resource.exists("key2"));
    }
}
