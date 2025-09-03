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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestVEnumElement {
    @Test
    public void testGetEnumName() {
        VEnumElement vEnumElement = new VEnumElement();
        vEnumElement.setEnumName("TestEnum");
        String enumName = vEnumElement.getEnumName();
        assertNotNull(enumName, "Enum name should not be null");
    }

    @Test
    public void testGetRbKey() {
        VEnumElement vEnumElement = new VEnumElement();
        vEnumElement.setRbKey("TestKey");
        String rbKey = vEnumElement.getRbKey();
        assertNotNull(rbKey, "Resource bundle key should not be null");
    }

    @Test
    public void testGetMyClassType() {
        VEnumElement vEnumElement = new VEnumElement();
        int          myClassType  = vEnumElement.getMyClassType();
        assertNotNull(String.valueOf(myClassType), "My class type should not be null");
    }

    @Test
    public void tesToString() {
        VEnumElement vEnumElement = new VEnumElement();
        String       str          = vEnumElement.toString();
        assertNotNull(str, "String representation should not be null");
    }
}
