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
    AUTHZ_FACTORY_NOT_INITIALIZED(500, "00-001", "RangerAuthorizerFactory not initialized"),
    AUTHZ_FACTORY_INITIALIZATION_FAILED(500, "00-002", "Initialization of authorizer factory failed"),

    INVALID_REQUEST_SERVICE_NOT_FOUND(400, "00-001", "{0}: service not found"),
    INVALID_REQUEST_SERVICE_TYPE_NOT_FOUND(400, "00-002", "{0}: service type not found"),
    INVALID_REQUEST_RESOURCE_TYPE_NOT_FOUND(400, "00-003", "{0}: resource type not found"),
    INVALID_REQUEST_PERMISSION_NOT_FOUND(400, "00-004", "{0}: permission not found"),
    INVALID_REQUEST_SERVICE_NAME_OR_TYPE_MANDATORY(400, "00-005", "Service name or service type is mandatory"),
    INVALID_RESOURCE_TEMPLATE_UNEXPECTED_MARKER_AT(400, "00-006", "Invalid resource template: {0}. Unexpected marker '{1}' at position {2}");

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
