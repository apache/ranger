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
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_ACCESS_CONTEXT_MISSING;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_ACCESS_INFO_MISSING;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_NAME_MATCH_SCOPE_INVALID;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_PERMISSIONS_EMPTY;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_PERMISSION_NOT_FOUND;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_RESOURCE_INFO_MISSING;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_RESOURCE_NAME_MISSING;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_RESOURCE_TYPE_NOT_FOUND;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_RESOURCE_VALUE_FOR_TYPE;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_SERVICE_NAME_OR_TYPE_MANDATORY;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_SERVICE_NOT_FOUND;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_SERVICE_TYPE_NOT_FOUND;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_USER_INFO_MISSING;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_RESOURCE_TEMPLATE_EMPTY_VALUE;
import static org.apache.ranger.authz.model.RangerResourceInfo.ResourceMatchScope.SELF;
import static org.apache.ranger.authz.model.RangerResourceInfo.ResourceMatchScope.SELF_OR_ANY_CHILD;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAuthzApiErrorCode {
    @Test
    public void testAuthzApiErrorCodeMessages() {
        assertEquals("AUTHZ-500-00-001: failed to create authorizer of type " + DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS, AUTHORIZER_CREATION_FAILED.getFormattedMessage(DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS));

        assertEquals("AUTHZ-400-00-001: dev_myapp: service not found", INVALID_REQUEST_SERVICE_NOT_FOUND.getFormattedMessage("dev_myapp"));
        assertEquals("AUTHZ-400-00-002: myapp: service type not found", INVALID_REQUEST_SERVICE_TYPE_NOT_FOUND.getFormattedMessage("myapp"));
        assertEquals("AUTHZ-400-00-003: missing user info", INVALID_REQUEST_USER_INFO_MISSING.getFormattedMessage());
        assertEquals("AUTHZ-400-00-004: missing access info", INVALID_REQUEST_ACCESS_INFO_MISSING.getFormattedMessage());
        assertEquals("AUTHZ-400-00-005: missing resource info", INVALID_REQUEST_RESOURCE_INFO_MISSING.getFormattedMessage());
        assertEquals("AUTHZ-400-00-006: missing resource name", INVALID_REQUEST_RESOURCE_NAME_MISSING.getFormattedMessage());
        assertEquals("AUTHZ-400-00-007: invalid name match scope SELF_OR_ANY_CHILD. For resource with sub-resources, valid scopes are [SELF]", INVALID_REQUEST_NAME_MATCH_SCOPE_INVALID.getFormattedMessage(SELF_OR_ANY_CHILD, "resource with sub-resources", SELF));
        assertEquals("AUTHZ-400-00-008: missing access context", INVALID_REQUEST_ACCESS_CONTEXT_MISSING.getFormattedMessage());
        assertEquals("AUTHZ-400-00-009: myresource: resource type not found", INVALID_REQUEST_RESOURCE_TYPE_NOT_FOUND.getFormattedMessage("myresource"));
        assertEquals("AUTHZ-400-00-010: mypath: invalid resource value for type path", INVALID_REQUEST_RESOURCE_VALUE_FOR_TYPE.getFormattedMessage("mypath", "path"));
        assertEquals("AUTHZ-400-00-011: mypermission: permission not found", INVALID_REQUEST_PERMISSION_NOT_FOUND.getFormattedMessage("mypermission"));
        assertEquals("AUTHZ-400-00-012: permissions is empty. Nothing to authorize", INVALID_REQUEST_PERMISSIONS_EMPTY.getFormattedMessage());
        assertEquals("AUTHZ-400-00-013: service name or service type is mandatory", INVALID_REQUEST_SERVICE_NAME_OR_TYPE_MANDATORY.getFormattedMessage());
        assertEquals("AUTHZ-400-00-014: invalid resource template - empty", INVALID_RESOURCE_TEMPLATE_EMPTY_VALUE.getFormattedMessage());

        assertEquals("AUTHZ-400-00-015: invalid resource \"mytype:myresource\" - unknown type \"mytype\"", RangerAuthzApiErrorCode.INVALID_RESOURCE_TYPE_NOT_VALID.getFormattedMessage("mytype:myresource", "mytype"));
        assertEquals("AUTHZ-400-00-016: invalid resource - empty", RangerAuthzApiErrorCode.INVALID_RESOURCE_EMPTY_VALUE.getFormattedMessage());
        assertEquals("AUTHZ-400-00-017: invalid resource \"mytype:myresource\" - does not match template \"{res1}/{res2}\"", RangerAuthzApiErrorCode.INVALID_RESOURCE_VALUE.getFormattedMessage("mytype:myresource", "{res1}/{res2}"));
    }
}
