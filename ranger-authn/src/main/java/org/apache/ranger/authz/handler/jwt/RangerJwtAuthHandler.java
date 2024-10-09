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

import java.net.URL;
import java.text.ParseException;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.util.CertificateUtil;
import org.apache.ranger.authz.handler.RangerAuthHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;

public abstract class RangerJwtAuthHandler implements RangerAuthHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RangerJwtAuthHandler.class);

    private JWSVerifier        verifier            = null;
    protected SignedJWT        signedJWT           = null;
    private String             jwksProviderUrl     = null;
    public static final String TYPE                = "ranger-jwt";        // Constant that identifies the authentication mechanism.
    public static final String KEY_PROVIDER_URL    = "jwks.provider-url"; // JWKS provider URL
    public static final String KEY_JWT_PUBLIC_KEY  = "jwt.public-key";    // JWT token provider public key
    public static final String KEY_JWT_COOKIE_NAME = "jwt.cookie-name";   // JWT cookie name
    public static final String KEY_JWT_AUDIENCES   = "jwt.audiences";
    public static final String JWT_AUTHZ_PREFIX    = "Bearer ";
    public static final String CUSTOM_JWT_CLAIM_GROUP_KEY_PARAM         = "custom.jwt.claim.group.key";
    public static final String CUSTOM_JWT_CLAIM_GROUP_KEY_VALUE_DEFAULT = "knox.groups";

    public String CUSTOM_JWT_CLAIM_GROUP_KEY_VALUE = null;
    protected List<String>               audiences = null;
    protected JWKSource<SecurityContext> keySource = null;

    protected static String cookieName = "hadoop-jwt";

    @Override
    public void initialize(final Properties config) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("===>>> RangerJwtAuthHandler.initialize()");
        }

        // mandatory configurations
        jwksProviderUrl = config.getProperty(KEY_PROVIDER_URL);
        if (!StringUtils.isBlank(jwksProviderUrl)) {
	    keySource = new RemoteJWKSet<>(new URL(jwksProviderUrl));
        }

        // optional configurations
        String pemPublicKey = config.getProperty(KEY_JWT_PUBLIC_KEY);
        CUSTOM_JWT_CLAIM_GROUP_KEY_VALUE = config.getProperty(CUSTOM_JWT_CLAIM_GROUP_KEY_PARAM, CUSTOM_JWT_CLAIM_GROUP_KEY_VALUE_DEFAULT);

        // setup JWT provider public key if configured
        if (StringUtils.isNotBlank(pemPublicKey)) {
            verifier = new RSASSAVerifier(CertificateUtil.parseRSAPublicKey(pemPublicKey));
        } else if (StringUtils.isBlank(jwksProviderUrl)) {
	    throw new Exception("RangerJwtAuthHandler: Mandatory configs ('jwks.provider-url' & 'jwt.public-key') are missing, must provide atleast one.");
	}

        // setup custom cookie name if configured
        String customCookieName = config.getProperty(KEY_JWT_COOKIE_NAME);
        if (customCookieName != null) {
            cookieName = customCookieName;
        }

        // setup audiences if configured
        String audiencesStr = config.getProperty(KEY_JWT_AUDIENCES);
        if (StringUtils.isNotBlank(audiencesStr)) {
            audiences = Arrays.asList(audiencesStr.split(","));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<<<=== RangerJwtAuthHandler.initialize()");
        }
    }

    protected AuthenticationToken authenticate(final String jwtAuthHeader, final String jwtCookie, final String doAsUser) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("===>>> RangerJwtAuthHandler.authenticate()");
        }

        AuthenticationToken token = null;
        if (shouldProceedAuth(jwtAuthHeader, jwtCookie)) {
            String serializedJWT = getJWT(jwtAuthHeader, jwtCookie);

            if (StringUtils.isNotBlank(serializedJWT)) {
                try {
                    signedJWT = SignedJWT.parse(serializedJWT);
                    JWTClaimsSet claimsSet = getJWTClaimsSet();

                    if(LOG.isDebugEnabled()){
                        LOG.debug("RangerJwtAuthHandler.authenticate(): JWTClaimsSet - {}", claimsSet);
                    }

                    boolean         valid    = validateToken();
                    if (valid) {
                        String userName;

                        if (StringUtils.isNotBlank(doAsUser)) {
                            userName = doAsUser.trim();
                        } else {
                            userName = claimsSet.getSubject();
                        }

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("RangerJwtAuthHandler.authenticate(): Issuing AuthenticationToken for user: [{}]", userName);
                            LOG.debug("RangerJwtAuthHandler.authenticate(): Authentication successful for user [{}] and doAs user is [{}]", claimsSet.getSubject(), doAsUser);
                        }
                        token = new AuthenticationToken(userName, userName, TYPE);
                    } else {
                        LOG.warn("RangerJwtAuthHandler.authenticate(): Validation failed for JWT: [{}] ", signedJWT.serialize());
                    }
                } catch (ParseException | RuntimeException exp) {
                    LOG.warn("RangerJwtAuthHandler.authenticate(): Unable to parse the JWT", exp);
                }
            } else {
                LOG.warn("RangerJwtAuthHandler.authenticate(): JWT not found");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<<<=== RangerJwtAuthHandler.authenticate()");
        }

        return token;
    }
    
    protected JWTClaimsSet getJWTClaimsSet() throws ParseException {
        return signedJWT.getJWTClaimsSet();
    }

    public Set<String> getGroupsFromClaimSet() {
        List<String> groupsClaim = null;
        try {
            groupsClaim = (List<String>) getJWTClaimsSet().getClaim(CUSTOM_JWT_CLAIM_GROUP_KEY_VALUE);
        } catch (ParseException e) {
            LOG.error("Unable to parse JWT claim set", e);
        }

        if (groupsClaim == null) {
            LOG.warn("No group claim found!");
            return new HashSet<>();
        }

        Set<String> groups = new HashSet<>(groupsClaim);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Groups present in Claim [{}]: {}", CUSTOM_JWT_CLAIM_GROUP_KEY_VALUE, groups);
        }
        return groups;
    }


    protected String getJWT(final String jwtAuthHeader, final String jwtCookie) {
        String serializedJWT = null;

        // try to fetch from AUTH header
        if (StringUtils.isNotBlank(jwtAuthHeader) && jwtAuthHeader.startsWith(JWT_AUTHZ_PREFIX)) {
            serializedJWT = jwtAuthHeader.substring(JWT_AUTHZ_PREFIX.length());
        }

        // if not found in AUTH header, try to fetch from cookie
        if (StringUtils.isBlank(serializedJWT) && StringUtils.isNotBlank(jwtCookie)) {
            String[] cookie = jwtCookie.split("=");
            if (cookieName.equals(cookie[0])) {
                serializedJWT = cookie[1];
            }
        }

        return serializedJWT;
    }

    /**
     * This method provides a single method for validating the JWT for use in
     * request processing. It provides for the override of specific aspects of this
     * implementation through submethods used within but also allows for the
     * override of the entire token validation algorithm.
     *
     * @return true if valid
     */
    protected boolean validateToken() throws ParseException {
        boolean expValid = validateExpiration();
        boolean sigValid = false;
        boolean audValid = false;

        if (expValid) {
            sigValid = validateSignature();

            if (sigValid) {
                audValid = validateAudiences();
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("expValid={}, sigValid={}, audValid={}", expValid, sigValid, audValid);
        }

        return sigValid && audValid && expValid;
    }

    /**
     * Verify the signature of the JWT in this method. This method depends on
     * the public key that was established during init based upon the provisioned
     * public key. Override this method in subclasses in order to customize the
     * signature verification behavior.
     *
     * @return valid true if signature verifies successfully; false otherwise
     */
    protected boolean validateSignature() {
        boolean valid = false;

        if (JWSObject.State.SIGNED == signedJWT.getState()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("JWT is in a SIGNED state");
            }

            if (signedJWT.getSignature() != null) {
                try {
                    if (StringUtils.isNotBlank(jwksProviderUrl)) {
                        JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(signedJWT.getHeader().getAlgorithm(), keySource);

                        // Create a JWT processor for the access tokens
                        ConfigurableJWTProcessor<SecurityContext> jwtProcessor = getJwtProcessor(keySelector);

                        // Process the token
                        jwtProcessor.process(signedJWT, null);
                        valid = true;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("JWT has been successfully verified.");
                        }
                    } else if (verifier != null) {
                        if (signedJWT.verify(verifier)) {
                            valid = true;
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("JWT has been successfully verified.");
                            }
                        } else {
                            LOG.warn("JWT signature verification failed.");
                        }
                    } else {
                        LOG.warn("Cannot authenticate JWT token as neither JWKS provider URL nor public key provided.");
                    }
                } catch (JOSEException | BadJOSEException e) {
                    LOG.error("Error while validating signature.", e);
                }
            }
        }

        if (!valid) {
            LOG.warn("Signature could not be verified.");
        }

        return valid;
    }

    public abstract ConfigurableJWTProcessor<SecurityContext> getJwtProcessor(final JWSKeySelector<SecurityContext> keySelector);

    /**
     * Validate whether any of the accepted audience claims is present in the issued
     * token claims list for audience. Override this method in subclasses in order
     * to customize the audience validation behavior.
     *
     * @return true if an expected audience is present, otherwise false
     */
    protected boolean validateAudiences() throws ParseException {
        boolean valid = false;
        JWTClaimsSet claimsSet = getJWTClaimsSet();
        List<String> tokenAudienceList = claimsSet.getAudience();
        // if there were no expected audiences configured then just consider any audience acceptable
        if (audiences == null) {
            valid = true;
        } else {
            // if any of the configured audiences is found then consider it acceptable
            for (String aud : tokenAudienceList) {
                if (audiences.contains(aud)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("JWT audience has been successfully validated.");
                    }
                    valid = true;
                    break;
                }
            }
        }

        if (!valid) {
            LOG.warn("JWT audience validation failed.");
        }
        return valid;
    }

    /**
     * Validate that the expiration time of the JWT has not been violated. If
     * it has, then throw an AuthenticationException. Override this method in
     * subclasses in order to customize the expiration validation behavior.
     *
     * @return valid true if the token has not expired; false otherwise
     */
    protected boolean validateExpiration() throws ParseException {
        boolean valid = false;
        Date expires = getJWTClaimsSet().getExpirationTime();
        if (expires == null || new Date().before(expires)) {
            valid = true;
            if (LOG.isDebugEnabled()) {
                LOG.debug("JWT expiration date has been successfully validated.");
            }
        } else {
            LOG.warn("JWT provided has expired.");
        }

        return valid;
    }

    public static boolean shouldProceedAuth(final String authHeader, final String jwtCookie) {
        return (StringUtils.isNotBlank(authHeader) && authHeader.startsWith(JWT_AUTHZ_PREFIX)) || (StringUtils.isNotBlank(jwtCookie) && jwtCookie.startsWith(cookieName));
    }
}
