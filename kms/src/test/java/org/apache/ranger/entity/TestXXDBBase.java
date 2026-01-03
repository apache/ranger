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
package org.apache.ranger.entity;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestXXDBBase {
    @Test
    public void testEquals_sameObject() {
        XXDBBaseImpl base = new XXDBBaseImpl();
        assertEquals(base, base);
    }

    @Test
    public void testEquals_null() {
        XXDBBaseImpl base = new XXDBBaseImpl();
        assertNotEquals(base, null);
    }

    @Test
    public void testEquals_sameObject1() {
        XXDBBaseImpl obj = new XXDBBaseImpl();
        assertEquals(obj, obj);
    }

    @Test
    public void testEquals_null1() {
        XXDBBaseImpl obj = new XXDBBaseImpl();
        assertNotEquals(obj, null);
    }

    @Test
    public void testEquals_differentClass() {
        XXDBBaseImpl obj = new XXDBBaseImpl();
        assertNotEquals(obj, "Some String");
    }

    @Test
    public void testEquals_allFieldsEqual() {
        Date now = new Date();

        XXDBBaseImpl obj1 = new XXDBBaseImpl();
        obj1.setCreateTime(now);
        obj1.setUpdateTime(now);
        obj1.setAddedByUserId(1L);
        obj1.setUpdatedByUserId(2L);

        XXDBBaseImpl obj2 = new XXDBBaseImpl();
        obj2.setCreateTime(now);
        obj2.setUpdateTime(now);
        obj2.setAddedByUserId(1L);
        obj2.setUpdatedByUserId(2L);

        assertEquals(obj1, obj2);
    }

    @Test
    public void testEquals_differentCreateTime() {
        XXDBBaseImpl obj1 = new XXDBBaseImpl();
        obj1.setCreateTime(new Date(100000L));

        XXDBBaseImpl obj2 = new XXDBBaseImpl();
        obj2.setCreateTime(new Date(200000L));

        assertNotEquals(obj1, obj2);
    }

    @Test
    public void testEquals_differentUpdateTime() {
        XXDBBaseImpl obj1 = new XXDBBaseImpl();
        obj1.setUpdateTime(new Date(100000L));

        XXDBBaseImpl obj2 = new XXDBBaseImpl();
        obj2.setUpdateTime(new Date(200000L));

        assertNotEquals(obj1, obj2);
    }

    @Test
    public void testEquals_differentAddedByUserId() {
        XXDBBaseImpl obj1 = new XXDBBaseImpl();
        obj1.setAddedByUserId(10L);

        XXDBBaseImpl obj2 = new XXDBBaseImpl();
        obj2.setAddedByUserId(20L);

        assertNotEquals(obj1, obj2);
    }

    @Test
    public void testEquals_differentUpdatedByUserId() {
        XXDBBaseImpl obj1 = new XXDBBaseImpl();
        obj1.setUpdatedByUserId(30L);

        XXDBBaseImpl obj2 = new XXDBBaseImpl();
        obj2.setUpdatedByUserId(40L);

        assertNotEquals(obj1, obj2);
    }

    @Test
    public void testToString_containsExpectedFields() {
        Date         now = new Date();
        XXDBBaseImpl obj = new XXDBBaseImpl();
        obj.setCreateTime(now);
        obj.setUpdateTime(now);
        obj.setAddedByUserId(5L);
        obj.setUpdatedByUserId(6L);

        String str = obj.toString();
        assertTrue(str.contains("createTime={" + now));
        assertTrue(str.contains("updateTime={" + now));
        assertTrue(str.contains("addedByUserId={5}"));
        assertTrue(str.contains("updatedByUserId={6}"));
    }

    @Test
    public void testHashCode_returnsConsistentValue() {
        XXDBBaseImpl obj   = new XXDBBaseImpl();
        int          hash1 = obj.hashCode();
        int          hash2 = obj.hashCode();
        assertEquals(hash1, hash2);
    }

    @Test
    public void testDefaultConstructor_setsCreateAndUpdateTime() {
        XXDBBaseImpl obj = new XXDBBaseImpl();
        assertNotNull(obj.getCreateTime());
        assertNotNull(obj.getUpdateTime());
    }

    @Test
    public void testGetEnumName_alwaysReturnsNull() {
        assertNull(XXDBBase.getEnumName("anyField"));
        assertNull(XXDBBase.getEnumName(null));
        assertNull(XXDBBase.getEnumName(""));
    }

    @Test
    public void testGetMyClassType_returnsDefault() {
        XXDBBaseImpl obj = new XXDBBaseImpl();
        assertEquals(XXDBBase.CLASS_TYPE_NONE, obj.getMyClassType());
    }

    @Test
    public void testGetMyDisplayValue_returnsNull() {
        XXDBBaseImpl obj = new XXDBBaseImpl();
        assertNull(obj.getMyDisplayValue());
    }

    @Test
    public void testGetAddedByUserId_returnsCorrectValue() {
        XXDBBaseImpl obj        = new XXDBBaseImpl();
        Long         expectedId = 100L;
        obj.setAddedByUserId(expectedId);
        assertEquals(expectedId, obj.getAddedByUserId());
    }

    @Test
    public void testGetUpdatedByUserId_returnsCorrectValue() {
        XXDBBaseImpl obj        = new XXDBBaseImpl();
        Long         expectedId = 200L;
        obj.setUpdatedByUserId(expectedId);
        assertEquals(expectedId, obj.getUpdatedByUserId());
    }

    @Test
    public void testNullCreateTimeAndUpdateTimeHandledInEquals() {
        XXDBBaseImpl obj1 = new XXDBBaseImpl();
        XXDBBaseImpl obj2 = new XXDBBaseImpl();

        obj1.setCreateTime(null);
        obj1.setUpdateTime(null);
        obj2.setCreateTime(null);
        obj2.setUpdateTime(null);

        obj1.setAddedByUserId(1L);
        obj2.setAddedByUserId(1L);
        obj1.setUpdatedByUserId(2L);
        obj2.setUpdatedByUserId(2L);

        assertEquals(obj1, obj2);
    }

    static class XXDBBaseImpl extends XXDBBase {
        private Long id;

        @Override
        public Long getId() {
            return id;
        }

        @Override
        public void setId(Long id) {
            this.id = id;
        }
    }
}
