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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.X509CertUtils;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.JWTClaimsSetVerifier;
import com.nimbusds.jwt.proc.JWTProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authz.handler.RangerAuthHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class RangerJwtAuthHandler implements RangerAuthHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RangerJwtAuthHandler.class);

    public static final String      TYPE                = "ranger-jwt";        // Constant that identifies the authentication mechanism.
    public static final String      KEY_PROVIDER_URL    = "jwks.provider-url"; // JWKS provider URL
    public static final String      KEY_JWT_PUBLIC_KEY  = "jwt.public-key";    // JWT token provider public key
    public static final String      KEY_JWT_AUDIENCES   = "jwt.audiences";
    public static final String      KEY_JWT_ISS         = "jwt.issuer";
    public static final String      JWT_AUTHZ_PREFIX    = "Bearer ";

    /* only asymmetric signatures are accepted: a public key/JWKS must never double as an HMAC secret */
    protected static final Set<JWSAlgorithm> ACCEPTED_JWS_ALGORITHMS = acceptedJwsAlgorithms();
    protected static final Set<String>       REQUIRED_CLAIMS         = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("sub", "exp")));

    protected List<String>               audiences;
    protected String                     issuer;
    protected JWKSource<SecurityContext> keySource;
    private   JWTProcessor<SecurityContext> jwtProcessor;

    public static boolean shouldProceedAuth(final String authHeader) {
        return (StringUtils.isNotBlank(authHeader) && authHeader.startsWith(JWT_AUTHZ_PREFIX));
    }

    @Override
    public void initialize(final Properties config) throws Exception {
        LOG.debug("===>>> RangerJwtAuthHandler.initialize()");

        /* signing key: JWKS provider URL takes precedence over a pinned public key */
        String                          jwksProviderUrl = config.getProperty(KEY_PROVIDER_URL);
        String                          pemPublicKey    = config.getProperty(KEY_JWT_PUBLIC_KEY);
        JWSKeySelector<SecurityContext> keySelector;

        if (StringUtils.isNotBlank(jwksProviderUrl)) {
            keySource   = new RemoteJWKSet<>(new URL(jwksProviderUrl));
            keySelector = new JWSVerificationKeySelector<>(ACCEPTED_JWS_ALGORITHMS, keySource);
        } else if (StringUtils.isNotBlank(pemPublicKey)) {
            RSAKey publicKey = RSAKey.parse(X509CertUtils.parse(pemPublicKey));

            keySource   = new ImmutableJWKSet<>(new JWKSet(publicKey));
            keySelector = fixedKeySelector(publicKey);
        } else {
            throw new Exception("RangerJwtAuthHandler: Mandatory configs ('" + KEY_PROVIDER_URL + "' & '" + KEY_JWT_PUBLIC_KEY + "') are missing, must provide atleast one.");
        }

        /* accepted audiences: mandatory */
        String audiencesStr = config.getProperty(KEY_JWT_AUDIENCES);

        if (StringUtils.isNotBlank(audiencesStr)) {
            audiences = Arrays.stream(audiencesStr.split(",")).map(String::trim).filter(StringUtils::isNotEmpty).collect(Collectors.toList());
        }

        if (audiences == null || audiences.isEmpty()) {
            throw new Exception("RangerJwtAuthHandler: Mandatory config '" + KEY_JWT_AUDIENCES + "' is missing. Set the audience(s) the token provider issues for this service.");
        }

        /* expected issuer: mandatory */
        String issuerStr = config.getProperty(KEY_JWT_ISS);

        if (StringUtils.isNotBlank(issuerStr)) {
            issuer = issuerStr.trim();
        }

        if (StringUtils.isBlank(issuer)) {
            throw new Exception("RangerJwtAuthHandler: Mandatory config '" + KEY_JWT_ISS + "' is missing. Set the issuer (iss claim) of the token provider.");
        }

        /* one processor for the lifetime of the handler; it is thread-safe and caches JWKS lookups */
        jwtProcessor = getJwtProcessor(keySelector);

        LOG.debug("<<<=== RangerJwtAuthHandler.initialize()");
    }

    /**
     * Build the processor that verifies signature and claims of every token. Called once
     * from {@link #initialize(Properties)}; implementations should combine the given key
     * selector with {@link #createClaimsVerifier()}.
     *
     * @param keySelector selects the verification key(s) for a token from the configured JWKS or public key
     * @return the processor used for all subsequent token validation
     */
    public abstract ConfigurableJWTProcessor<SecurityContext> getJwtProcessor(JWSKeySelector<SecurityContext> keySelector);

    /**
     * Claims verifier enforcing the configured audiences and issuer, and requiring the
     * 'sub' and 'exp' claims. Expiry and not-before are checked by the verifier itself.
     *
     * @return claims verifier for the processor
     */
    protected JWTClaimsSetVerifier<SecurityContext> createClaimsVerifier() {
        JWTClaimsSet exactMatchClaims = new JWTClaimsSet.Builder().issuer(issuer).build();

        return new DefaultJWTClaimsVerifier<>(new HashSet<>(audiences), exactMatchClaims, REQUIRED_CLAIMS, null);
    }

    protected String authenticate(final String jwtAuthHeader) {
        LOG.debug("===>>> RangerJwtAuthHandler.authenticate()");

        if (shouldProceedAuth(jwtAuthHeader)) {
            String serializedJWT = getJWT(jwtAuthHeader);

            if (StringUtils.isNotBlank(serializedJWT)) {
                try {
                    final SignedJWT jwtToken = SignedJWT.parse(serializedJWT);
                    boolean         valid    = validateToken(jwtToken);

                    if (valid) {
                        String userName = jwtToken.getJWTClaimsSet().getSubject();

                        if (StringUtils.isBlank(userName)) {
                            LOG.warn("JWT token has a blank subject (sub) claim; rejecting ({})", safeJwtLogContext(jwtToken));

                            return null;
                        }

                        LOG.debug("RangerJwtAuthHandler.authenticate(): Issuing AuthenticationToken for user: [{}]", userName);

                        return userName;
                    }
                } catch (ParseException pe) {
                    LOG.warn("RangerJwtAuthHandler.authenticate(): Unable to parse the JWT token", pe);
                }
            } else {
                LOG.warn("RangerJwtAuthHandler.authenticate(): JWT token not found.");
            }
        }

        LOG.debug("<<<=== RangerJwtAuthHandler.authenticate()");

        return null;
    }

    protected String getJWT(final String jwtAuthHeader) {
        String serializedJWT = null;

        // try to fetch from AUTH header
        if (StringUtils.isNotBlank(jwtAuthHeader) && jwtAuthHeader.startsWith(JWT_AUTHZ_PREFIX)) {
            serializedJWT = jwtAuthHeader.substring(JWT_AUTHZ_PREFIX.length());
        }

        return serializedJWT;
    }

    /**
     * Validate the token with the processor built at initialization: signature against the
     * configured JWKS/public key, then audience, issuer, required claims, expiry and not-before.
     * Override this method in subclasses in order to customize the entire validation algorithm.
     *
     * @param jwtToken the token to validate
     * @return true if valid
     */
    protected boolean validateToken(final SignedJWT jwtToken) {
        if (jwtProcessor == null) {
            LOG.warn("JWT validation failed: handler is not initialized ({})", safeJwtLogContext(jwtToken));

            return false;
        }

        try {
            jwtProcessor.process(jwtToken, null);

            LOG.debug("JWT token has been successfully verified.");

            return true;
        } catch (BadJOSEException | JOSEException e) {
            LOG.warn("JWT validation failed: {} ({})", e.getMessage(), safeJwtLogContext(jwtToken));

            return false;
        }
    }

    /**
     * Build non-sensitive JWT metadata for operational logs.
     * Never log the raw bearer token.
     *
     * @param jwtToken parsed JWT used to extract claim metadata
     * @return safe diagnostic string for log output
     */
    protected String safeJwtLogContext(final SignedJWT jwtToken) {
        try {
            JWTClaimsSet claims = jwtToken.getJWTClaimsSet();
            JWSHeader header = jwtToken.getHeader();
            String keyId = header != null ? header.getKeyID() : null;
            List<String> tokenAudiences = claims.getAudience();
            String audience = tokenAudiences == null || tokenAudiences.isEmpty() ? null : StringUtils.join(tokenAudiences, ",");

            return String.format("subject=%s, audience=%s, issuer=%s, keyId=%s, jwtId=%s", claims.getSubject(), audience, claims.getIssuer(), keyId, claims.getJWTID());
        } catch (ParseException pe) {
            return "claims_unparseable";
        }
    }

    /* a pinned public key is used regardless of the token's 'kid', matching the previous RSASSAVerifier behaviour */
    private static JWSKeySelector<SecurityContext> fixedKeySelector(final RSAKey key) throws JOSEException {
        final RSAPublicKey publicKey = key.toRSAPublicKey();

        return (header, context) -> JWSAlgorithm.Family.RSA.contains(header.getAlgorithm()) ? Collections.singletonList(publicKey) : Collections.emptyList();
    }

    private static Set<JWSAlgorithm> acceptedJwsAlgorithms() {
        Set<JWSAlgorithm> ret = new LinkedHashSet<>(JWSAlgorithm.Family.RSA);

        ret.addAll(JWSAlgorithm.Family.EC);

        return Collections.unmodifiableSet(ret);
    }
}
