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

package org.apache.ranger.pdp.security;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Properties;

/**
 * Contract for PDP authentication handlers.
 *
 * <p>Each handler is responsible for a single credential type (Kerberos, JWT, HTTP Header).
 * The {@link RangerPdpAuthNFilter} tries handlers in configured order and uses the first one
 * that returns {@link Result.Status#AUTHENTICATED}.
 */
public interface PdpAuthNHandler {
    /**
     * Initializes the handler with filter init parameters.
     *
     * @param config filter init parameters
     * @throws Exception on initialization failure
     */
    void init(Properties config) throws Exception;

    /**
     * Attempts to authenticate the incoming request.
     *
     * <p>Handlers must follow this contract:
     * <ul>
     *   <li>{@link Result.Status#AUTHENTICATED} - credentials were present and valid.
     *       The handler may write {@code WWW-Authenticate} response headers (e.g., for SPNEGO
     *       mutual authentication) but must NOT commit the response.
     *   <li>{@link Result.Status#CHALLENGE} - credentials were present but invalid, or a
     *       multi-round negotiation step was sent. The handler has already written a
     *       {@code 401} response with an appropriate challenge header; the filter must not
     *       continue processing.
     *   <li>{@link Result.Status#SKIP} - this handler cannot process the request (the expected
     *       credential type is absent). The filter should try the next handler.
     * </ul>
     *
     * @param request  the HTTP request
     * @param response the HTTP response (may be written to for challenges)
     * @return authentication result
     * @throws IOException on I/O error
     */
    Result authenticate(HttpServletRequest request, HttpServletResponse response) throws IOException;

    /**
     * Returns the {@code WWW-Authenticate} header value to include in a terminal 401
     * response when no handler can process the request.
     * Returns {@code null} if this handler does not contribute a challenge header.
     */
    String getChallengeHeader();

    class Result {
        public enum Status { AUTHENTICATED, CHALLENGE, SKIP }

        private final Status status;
        private final String userName;
        private final String authType;

        private Result(Status status, String userName, String authType) {
            this.status   = status;
            this.userName = userName;
            this.authType = authType;
        }

        public static Result authenticated(String userName, String authType) {
            return new Result(Status.AUTHENTICATED, userName, authType);
        }

        public static Result challenge() {
            return new Result(Status.CHALLENGE, null, null);
        }

        public static Result skip() {
            return new Result(Status.SKIP, null, null);
        }

        public Status getStatus() {
            return status;
        }

        public String getUserName() {
            return userName;
        }

        public String getAuthType() {
            return authType;
        }
    }
}
