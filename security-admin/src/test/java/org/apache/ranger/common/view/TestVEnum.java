/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.common.view;

import org.apache.ranger.common.RangerCommonEnums;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestVEnum {
    @Test
    public void testSetAndGetEnumName() {
        VEnum vEnum = new VEnum();
        vEnum.setEnumName("TestEnum");

        String result = vEnum.getEnumName();
        assertNotNull(result, "Enum name should not be null");
        assertEquals("TestEnum", result);
    }

    @Test
    public void testSetAndGetElementList() {
        VEnumElement elem1 = new VEnumElement();
        elem1.setElementName("e1");

        VEnumElement elem2 = new VEnumElement();
        elem2.setElementName("e2");

        List<VEnumElement> elementList = Arrays.asList(elem1, elem2);

        VEnum vEnum = new VEnum();
        vEnum.setElementList(elementList);

        List<VEnumElement> result = vEnum.getElementList();
        assertNotNull(result, "Element list should not be null");
        assertEquals(2, result.size());
        assertEquals("e1", result.get(0).getElementName());
        assertEquals("e2", result.get(1).getElementName());
    }

    @Test
    public void testGetMyClassType() {
        VEnum vEnum  = new VEnum();
        int   result = vEnum.getMyClassType();

        assertEquals(RangerCommonEnums.CLASS_TYPE_ENUM, result, "Class type should match CLASS_TYPE_ENUM");
    }

    @Test
    public void testToString() {
        VEnumElement elem = new VEnumElement();
        elem.setElementName("Element1");
        elem.setElementValue(1);

        VEnum vEnum = new VEnum();
        vEnum.setEnumName("SampleEnum");
        vEnum.setElementList(Collections.singletonList(elem));

        String str = vEnum.toString();
        assertNotNull(str, "toString result should not be null");
        assertTrue(str.contains("SampleEnum"));
        assertTrue(str.contains("Element1"));
    }
}
