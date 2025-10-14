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

public enum RangerAuthzApiErrorCode implements RangerAuthzErrorCode {
    AUTHORIZER_CREATION_FAILED(500, "00-001", "failed to create authorizer of type {0}"),

    INVALID_REQUEST_SERVICE_NOT_FOUND(400, "00-001", "{0}: service not found"),
    INVALID_REQUEST_SERVICE_TYPE_NOT_FOUND(400, "00-002", "{0}: service type not found"),
    INVALID_REQUEST_USER_INFO_MISSING(400, "00-003", "missing user info"),
    INVALID_REQUEST_ACCESS_INFO_MISSING(400, "00-004", "missing access info"),
    INVALID_REQUEST_RESOURCE_INFO_MISSING(400, "00-005", "missing resource info"),
    INVALID_REQUEST_RESOURCE_NAME_MISSING(400, "00-006", "missing resource name"),
    INVALID_REQUEST_NAME_MATCH_SCOPE_INVALID(400, "00-007", "invalid name match scope {0}. For {1}, valid scopes are [{2}]"),
    INVALID_REQUEST_ACCESS_CONTEXT_MISSING(400, "00-008", "missing access context"),
    INVALID_REQUEST_RESOURCE_TYPE_NOT_FOUND(400, "00-009", "{0}: resource type not found"),
    INVALID_REQUEST_RESOURCE_VALUE_FOR_TYPE(400, "00-010", "{0}: invalid resource value for type {1}"),
    INVALID_REQUEST_PERMISSION_NOT_FOUND(400, "00-011", "{0}: permission not found"),
    INVALID_REQUEST_PERMISSIONS_EMPTY(400, "00-012", "permissions is empty. Nothing to authorize"),
    INVALID_REQUEST_SERVICE_NAME_OR_TYPE_MANDATORY(400, "00-013", "service name or service type is mandatory"),
    INVALID_RESOURCE_TEMPLATE_EMPTY_VALUE(400, "00-014", "invalid resource template - empty"),

    INVALID_RESOURCE_TYPE_NOT_VALID(400, "00-015", "invalid resource \"{0}\" - unknown type \"{1}\""),
    INVALID_RESOURCE_EMPTY_VALUE(400, "00-016", "invalid resource - empty"),
    INVALID_RESOURCE_VALUE(400, "00-017", "invalid resource \"{0}\" - does not match template \"{1}\"");

    private static final String ERROR_CODE_MODULE_PREFIX = "AUTHZ";

    private final int    httpStatusCode;
    private final String code;
    private final String message;

    RangerAuthzApiErrorCode(int httpStatusCode, String code, String message) {
        this.httpStatusCode = httpStatusCode;
        this.code           = String.format("%s-%3d-%s", ERROR_CODE_MODULE_PREFIX, httpStatusCode, code);
        this.message        = message;
    }

    public int getHttpStatusCode() {
        return httpStatusCode;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "RangerAuthzBaseErrorCode{" +
                "httpStatusCode=" + httpStatusCode +
                ", code='" + code + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
