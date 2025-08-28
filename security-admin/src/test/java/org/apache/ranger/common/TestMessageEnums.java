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
package org.apache.ranger.common;

import org.apache.ranger.view.VXMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestMessageEnums {
    @Test
    public void testDataNotFoundEnum() {
        MessageEnums messageEnum = MessageEnums.DATA_NOT_FOUND;

        assertEquals("xa.error.data_not_found", messageEnum.rbKey);
        assertEquals("Data not found", messageEnum.messageDesc);
    }

    @Test
    public void testOperNotAllowedForStateEnum() {
        MessageEnums messageEnum = MessageEnums.OPER_NOT_ALLOWED_FOR_STATE;

        assertEquals("xa.error.oper_not_allowed_for_state", messageEnum.rbKey);
        assertEquals("Operation not allowed in current state", messageEnum.messageDesc);
    }

    @Test
    public void testOperNotAllowedForEntityEnum() {
        MessageEnums messageEnum = MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY;

        assertEquals("xa.error.oper_not_allowed_for_state", messageEnum.rbKey);
        assertEquals("Operation not allowed for entity", messageEnum.messageDesc);
    }

    @Test
    public void testOperNoPermissionEnum() {
        MessageEnums messageEnum = MessageEnums.OPER_NO_PERMISSION;

        assertEquals("xa.error.oper_no_permission", messageEnum.rbKey);
        assertEquals("User doesn't have permission to perform this operation", messageEnum.messageDesc);
    }

    @Test
    public void testDataNotUpdatableEnum() {
        MessageEnums messageEnum = MessageEnums.DATA_NOT_UPDATABLE;

        assertEquals("xa.error.data_not_updatable", messageEnum.rbKey);
        assertEquals("Data not updatable", messageEnum.messageDesc);
    }

    @Test
    public void testErrorCreatingObjectEnum() {
        MessageEnums messageEnum = MessageEnums.ERROR_CREATING_OBJECT;

        assertEquals("xa.error.create_object", messageEnum.rbKey);
        assertEquals("Error creating object", messageEnum.messageDesc);
    }

    @Test
    public void testErrorDuplicateObjectEnum() {
        MessageEnums messageEnum = MessageEnums.ERROR_DUPLICATE_OBJECT;

        assertEquals("xa.error.duplicate_object", messageEnum.rbKey);
        assertEquals("Error creating duplicate object", messageEnum.messageDesc);
    }

    @Test
    public void testErrorDeleteObjectEnum() {
        MessageEnums messageEnum = MessageEnums.ERROR_DELETE_OBJECT;

        assertEquals("xa.error.delete_object", messageEnum.rbKey);
        assertEquals("Error deleting object", messageEnum.messageDesc);
    }

    @Test
    public void testErrorSystemEnum() {
        MessageEnums messageEnum = MessageEnums.ERROR_SYSTEM;

        assertEquals("xa.error.system", messageEnum.rbKey);
        assertEquals("System Error. Please try later.", messageEnum.messageDesc);
    }

    @Test
    public void testOperNoExportEnum() {
        MessageEnums messageEnum = MessageEnums.OPER_NO_EXPORT;

        assertEquals("xa.error.oper_no_export", messageEnum.rbKey);
        assertEquals("repository is disabled", messageEnum.messageDesc);
    }

    @Test
    public void testInvalidPasswordEnum() {
        MessageEnums messageEnum = MessageEnums.INVALID_PASSWORD;

        assertEquals("xa.validation.invalid_password", messageEnum.rbKey);
        assertEquals("Invalid password", messageEnum.messageDesc);
    }

    @Test
    public void testInvalidInputDataEnum() {
        MessageEnums messageEnum = MessageEnums.INVALID_INPUT_DATA;

        assertEquals("xa.validation.invalid_input_data", messageEnum.rbKey);
        assertEquals("Invalid input data", messageEnum.messageDesc);
    }

    @Test
    public void testNoInputDataEnum() {
        MessageEnums messageEnum = MessageEnums.NO_INPUT_DATA;

        assertEquals("xa.validation.no_input_data", messageEnum.rbKey);
        assertEquals("Input data is not provided", messageEnum.messageDesc);
    }

    @Test
    public void testInputDataOutOfBoundEnum() {
        MessageEnums messageEnum = MessageEnums.INPUT_DATA_OUT_OF_BOUND;

        assertEquals("xa.validation.data_out_of_bound", messageEnum.rbKey);
        assertEquals("Input data if out of bound", messageEnum.messageDesc);
    }

    @Test
    public void testGetMessageBasic() {
        MessageEnums messageEnum = MessageEnums.DATA_NOT_FOUND;

        VXMessage message = messageEnum.getMessage();

        assertNotNull(message);
        assertEquals("DATA_NOT_FOUND", message.getName());
        assertEquals("xa.error.data_not_found", message.getRbKey());
        assertEquals("Data not found", message.getMessage());
    }

    @Test
    public void testGetMessageWithObjectIdAndFieldName() {
        MessageEnums messageEnum = MessageEnums.INVALID_INPUT_DATA;
        Long         objectId    = 123L;
        String       fieldName   = "testField";

        VXMessage message = messageEnum.getMessage(objectId, fieldName);

        assertNotNull(message);
        assertEquals("INVALID_INPUT_DATA", message.getName());
        assertEquals("xa.validation.invalid_input_data", message.getRbKey());
        assertEquals("Invalid input data", message.getMessage());
        assertEquals(objectId, message.getObjectId());
        assertEquals(fieldName, message.getFieldName());
    }

    @Test
    public void testGetMessageWithNullObjectIdAndFieldName() {
        MessageEnums messageEnum = MessageEnums.ERROR_SYSTEM;

        VXMessage message = messageEnum.getMessage(null, null);

        assertNotNull(message);
        assertEquals("ERROR_SYSTEM", message.getName());
        assertEquals("xa.error.system", message.getRbKey());
        assertEquals("System Error. Please try later.", message.getMessage());
        assertNull(message.getObjectId());
        assertNull(message.getFieldName());
    }

    @Test
    public void testAllEnumValues() {
        // Test that all enum values are accessible and have proper structure
        MessageEnums[] allEnums = MessageEnums.values();

        for (MessageEnums messageEnum : allEnums) {
            assertNotNull(messageEnum.rbKey);
            assertNotNull(messageEnum.messageDesc);
            assertNotNull(messageEnum.toString());

            // Test that getMessage() works for all enums
            VXMessage message = messageEnum.getMessage();
            assertNotNull(message);
            assertNotNull(message.getName());
            assertNotNull(message.getRbKey());
            assertNotNull(message.getMessage());
        }
    }

    @Test
    public void testEnumToStringValues() {
        assertEquals("DATA_NOT_FOUND", MessageEnums.DATA_NOT_FOUND.toString());
        assertEquals("OPER_NOT_ALLOWED_FOR_STATE", MessageEnums.OPER_NOT_ALLOWED_FOR_STATE.toString());
        assertEquals("OPER_NOT_ALLOWED_FOR_ENTITY", MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY.toString());
        assertEquals("OPER_NO_PERMISSION", MessageEnums.OPER_NO_PERMISSION.toString());
        assertEquals("DATA_NOT_UPDATABLE", MessageEnums.DATA_NOT_UPDATABLE.toString());
        assertEquals("ERROR_CREATING_OBJECT", MessageEnums.ERROR_CREATING_OBJECT.toString());
        assertEquals("ERROR_DUPLICATE_OBJECT", MessageEnums.ERROR_DUPLICATE_OBJECT.toString());
        assertEquals("ERROR_DELETE_OBJECT", MessageEnums.ERROR_DELETE_OBJECT.toString());
        assertEquals("ERROR_SYSTEM", MessageEnums.ERROR_SYSTEM.toString());
        assertEquals("OPER_NO_EXPORT", MessageEnums.OPER_NO_EXPORT.toString());
        assertEquals("INVALID_PASSWORD", MessageEnums.INVALID_PASSWORD.toString());
        assertEquals("INVALID_INPUT_DATA", MessageEnums.INVALID_INPUT_DATA.toString());
        assertEquals("NO_INPUT_DATA", MessageEnums.NO_INPUT_DATA.toString());
        assertEquals("INPUT_DATA_OUT_OF_BOUND", MessageEnums.INPUT_DATA_OUT_OF_BOUND.toString());
    }

    @Test
    public void testEnumValueOf() {
        assertEquals(MessageEnums.DATA_NOT_FOUND, MessageEnums.valueOf("DATA_NOT_FOUND"));
        assertEquals(MessageEnums.INVALID_INPUT_DATA, MessageEnums.valueOf("INVALID_INPUT_DATA"));
        assertEquals(MessageEnums.ERROR_SYSTEM, MessageEnums.valueOf("ERROR_SYSTEM"));
    }
}
