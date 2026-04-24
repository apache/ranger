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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.apache.ranger.authz.model.RangerResourcePermissions;
import org.apache.ranger.authz.model.RangerResourcePermissionsRequest;

import java.io.IOException;

import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.REMOTE_RESPONSE_INVALID;

final class RangerRemoteJson {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private RangerRemoteJson() {
    }

    static String writeAuthzRequest(RangerAuthzRequest request) throws RangerAuthzException {
        return writeValue(request);
    }

    static String writeMultiAuthzRequest(RangerMultiAuthzRequest request) throws RangerAuthzException {
        return writeValue(request);
    }

    static String writeResourcePermissionsRequest(RangerResourcePermissionsRequest request) throws RangerAuthzException {
        return writeValue(request);
    }

    static RangerAuthzResult readAuthzResult(String json, String endpoint) throws RangerAuthzException {
        return readValue(json, endpoint, RangerAuthzResult.class);
    }

    static RangerMultiAuthzResult readMultiAuthzResult(String json, String endpoint) throws RangerAuthzException {
        return readValue(json, endpoint, RangerMultiAuthzResult.class);
    }

    static RangerResourcePermissions readResourcePermissions(String json, String endpoint) throws RangerAuthzException {
        return readValue(json, endpoint, RangerResourcePermissions.class);
    }

    static String readErrorMessage(String json) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }

        try {
            JsonNode root = OBJECT_MAPPER.readTree(json);
            JsonNode message = root != null ? root.get("message") : null;

            return message != null && !message.isNull() ? message.asText() : null;
        } catch (IOException e) {
            return null;
        }
    }

    private static String writeValue(Object value) throws RangerAuthzException {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new RangerAuthzException(REMOTE_RESPONSE_INVALID, e, "request-body");
        }
    }

    private static <T> T readValue(String json, String endpoint, Class<T> valueType) throws RangerAuthzException {
        try {
            return OBJECT_MAPPER.readValue(json, valueType);
        } catch (IOException e) {
            throw new RangerAuthzException(REMOTE_RESPONSE_INVALID, e, endpoint);
        }
    }
}
