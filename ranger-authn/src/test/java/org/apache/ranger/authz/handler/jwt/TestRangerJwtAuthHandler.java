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
package org.apache.ranger.authz.handler.jwt;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import org.apache.ranger.authz.handler.RangerAuth;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRangerJwtAuthHandler {
    static class TestHandler extends RangerJwtAuthHandler {
        @Override
        public ConfigurableJWTProcessor<SecurityContext> getJwtProcessor(JWSKeySelector<SecurityContext> keySelector) {
            return null;
        }

        @Override
        public RangerAuth authenticate(HttpServletRequest request) {
            return null;
        }

        boolean callValidateIssuer(SignedJWT jwt) {
            return validateIssuer(jwt);
        }
    }

    private static SignedJWT jwtWithIssuer(String issuer) {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .issuer(issuer)
                .subject("user")
                .expirationTime(new Date(System.currentTimeMillis() + 60_000))
                .build();

        // Header alg value doesn't matter for validateIssuer()
        return new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claims);
    }

    @Test
    void validateIssuerTrue_whenIssuerNotConfigured() {
        TestHandler handler = new TestHandler();
        handler.issuer = null; // StringUtils.isBlank(null) => true

        SignedJWT jwt = jwtWithIssuer("any-issuer");

        assertTrue(handler.callValidateIssuer(jwt));
    }

    @Test
    void validateIssuerTrue_whenIssuerMatches() {
        TestHandler handler = new TestHandler();
        handler.issuer = "expected-issuer";

        SignedJWT jwt = jwtWithIssuer("expected-issuer");

        assertTrue(handler.callValidateIssuer(jwt));
    }

    @Test
    void validateIssuerFalse_whenIssuerDoesNotMatch() {
        TestHandler handler = new TestHandler();
        handler.issuer = "expected-issuer";

        SignedJWT jwt = jwtWithIssuer("different-issuer");

        assertFalse(handler.callValidateIssuer(jwt));
    }

    @Test
    void validateIssuerFalse_whenJwtClaimsCannotBeParsed() throws Exception {
        TestHandler handler = new TestHandler();
        handler.issuer = "expected-issuer";

        String header = "eyJhbGciOiJIUzI1NiJ9"; // Header: {"alg":"HS256"}  (valid JWS header for SignedJWT)
        String payload = "buyevwv678";          // Payload: "not-json"      (NOT a JSON object => getJWTClaimsSet() will throw ParseException)
        String signature = "abcd";              // Signature: "sig"         (any base64url string works for parsing)

        SignedJWT badJwt = SignedJWT.parse(header + "." + payload + "." + signature);

        assertFalse(handler.callValidateIssuer(badJwt));
    }
}
