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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class TestVTrxLogAttr {
    @Test
    public void testIsEnum() {
        String      attribName             = "testAttribute";
        String      attribUserFriendlyName = "Test Attribute";
        VTrxLogAttr vTrxLogAttr            = new VTrxLogAttr(attribName, attribUserFriendlyName);

        boolean isEnum = vTrxLogAttr.isEnum();
        assertNotNull("isEnum should not be null", isEnum);
    }

    @Test
    public void testisObjName() {
        String      attribName             = "testAttribute";
        String      attribUserFriendlyName = "Test Attribute";
        VTrxLogAttr vTrxLogAttr            = new VTrxLogAttr(attribName, attribUserFriendlyName);

        boolean isObjName = vTrxLogAttr.isObjName();
        assertNotNull("isObjName should not be null", isObjName);
    }

    @Test
    public void testGetMyClassType() {
        VTrxLogAttr vTrxLogAttr = new VTrxLogAttr("testAttribute", "Test Attribute");
        int         myClassType = vTrxLogAttr.getMyClassType();
        assertNotNull("My class type should not be null", String.valueOf(myClassType));
    }

    @Test
    public void testToString() {
        VTrxLogAttr vTrxLogAttr = new VTrxLogAttr("testAttribute", "Test Attribute");
        String      str         = vTrxLogAttr.toString();
        assertNotNull("String representation should not be null", str);
    }
}
