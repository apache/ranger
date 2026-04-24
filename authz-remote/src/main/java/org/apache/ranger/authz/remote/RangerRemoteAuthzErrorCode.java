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

package org.apache.ranger.authz.remote;

import org.apache.ranger.authz.api.RangerAuthzErrorCode;

public enum RangerRemoteAuthzErrorCode implements RangerAuthzErrorCode {
    MISSING_PDP_URL(500, "00-001", "missing property {0}"),
    INVALID_PROPERTY_VALUE(500, "00-002", "invalid value for property {0}: {1}"),
    REMOTE_REQUEST_FAILED(500, "00-003", "failed to call Ranger PDP endpoint {0}"),
    REMOTE_CALL_UNSUCCESSFUL(502, "00-004", "Ranger PDP endpoint {0} returned HTTP {1}: {2}"),
    MISSING_AUTH_CONFIG(500, "00-005", "missing property {0}"),
    UNSUPPORTED_AUTH_TYPE(500, "00-006", "unsupported auth type {0}"),
    TLS_CONFIGURATION_FAILED(500, "00-007", "failed to initialize TLS for Ranger PDP endpoint {0}"),
    KERBEROS_LOGIN_FAILED(500, "00-008", "failed to initialize Kerberos credentials for Ranger PDP endpoint {0}"),
    FAILED_TO_SERIALIZE_REQUEST(500, "00-009", "failed to serialize request: {0}"),
    FAILED_TO_DESERIALIZE_RESPONSE(500, "00-010", "failed to deserialize response from Ranger PDP endpoint {0}"),
    REMOTE_CLIENT_CLOSE_FAILED(500, "00-011", "failed to close Ranger PDP client: {0}");

    private static final String ERROR_CODE_MODULE_PREFIX = "R_AUTHZ-";

    private final int    httpStatusCode;
    private final String code;
    private final String message;

    RangerRemoteAuthzErrorCode(int httpStatusCode, String code, String message) {
        this.httpStatusCode = httpStatusCode;
        this.code           = String.format("%s-%3d-%s", ERROR_CODE_MODULE_PREFIX, httpStatusCode, code);
        this.message        = message;
    }

    @Override
    public int getHttpStatusCode() {
        return httpStatusCode;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
