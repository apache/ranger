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

package org.apache.ranger.authz.embedded;

import org.apache.ranger.authz.api.RangerAuthzErrorCode;

public enum RangerEmbeddedAuthzErrorCode implements RangerAuthzErrorCode {
    NO_SERVICE_TYPE_FOR_SERVICE(400, "00-001", "No service type found for service {0}"),
    NO_DEFAULT_SERVICE_FOR_SERVICE_TYPE(400, "00-002", "No default service found for service type {0}"),

    FAILED_TO_CONTACT_AUTHZ_SERVICE(404, "00-001", "Failed to contact authorization service"),
    FAILED_TO_GET_SERVICE_POLICIES(404, "00-002", "Failed to retrieve policies for service {0}");

    private static final String ERROR_CODE_MODULE_PREFIX = "E_AUTHZ-";

    private final int    httpStatusCode;
    private final String code;
    private final String message;

    RangerEmbeddedAuthzErrorCode(int httpStatusCode, String code, String message) {
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
        return "RangerEmbeddedAuthzErrorCode{" +
                "httpStatusCode=" + httpStatusCode +
                ", code='" + code + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
