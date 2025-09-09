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

package org.apache.ranger.authz.api;

import org.junit.jupiter.api.Test;

import static org.apache.ranger.authz.api.RangerAuthorizerFactory.DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.AUTHORIZER_CREATION_FAILED;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_PERMISSION_NOT_FOUND;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_RESOURCE_TYPE_NOT_FOUND;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_RESOURCE_VALUE_FOR_TYPE;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_SERVICE_NAME_OR_TYPE_MANDATORY;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_SERVICE_NOT_FOUND;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_SERVICE_TYPE_NOT_FOUND;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_RESOURCE_TEMPLATE_UNEXPECTED_MARKER_AT;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAuthzApiErrorCode {
    @Test
    public void testAuthzApiErrorCodeMessages() {
        assertEquals("AUTHZ-500-00-001: failed to create authorizer of type " + DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS, AUTHORIZER_CREATION_FAILED.getFormattedMessage(DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS));

        assertEquals("AUTHZ-400-00-001: dev_myapp: service not found", INVALID_REQUEST_SERVICE_NOT_FOUND.getFormattedMessage("dev_myapp"));
        assertEquals("AUTHZ-400-00-002: myapp: service type not found", INVALID_REQUEST_SERVICE_TYPE_NOT_FOUND.getFormattedMessage("myapp"));
        assertEquals("AUTHZ-400-00-003: myresource: resource type not found", INVALID_REQUEST_RESOURCE_TYPE_NOT_FOUND.getFormattedMessage("myresource"));
        assertEquals("AUTHZ-400-00-004: mypath: invalid resource value for type path", INVALID_REQUEST_RESOURCE_VALUE_FOR_TYPE.getFormattedMessage("mypath", "path"));
        assertEquals("AUTHZ-400-00-005: mypermission: permission not found", INVALID_REQUEST_PERMISSION_NOT_FOUND.getFormattedMessage("mypermission"));
        assertEquals("AUTHZ-400-00-006: service name or service type is mandatory", INVALID_REQUEST_SERVICE_NAME_OR_TYPE_MANDATORY.getFormattedMessage());
        assertEquals("AUTHZ-400-00-007: invalid resource template: {database}.{table}}. Unexpected marker \"}\" at position 18", INVALID_RESOURCE_TEMPLATE_UNEXPECTED_MARKER_AT.getFormattedMessage("{database}.{table}}", "}", 18));
    }
}
