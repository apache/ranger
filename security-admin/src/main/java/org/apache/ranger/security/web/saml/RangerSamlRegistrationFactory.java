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
package org.apache.ranger.security.web.saml;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.credentialapi.CredentialReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.security.converter.RsaKeyConverters;
import org.springframework.security.saml2.core.Saml2X509Credential;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;
import org.springframework.security.saml2.provider.service.registration.Saml2MessageBinding;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;

public class RangerSamlRegistrationFactory {
    private static final Logger LOG = LoggerFactory.getLogger(RangerSamlRegistrationFactory.class);

    private RangerSamlRegistrationFactory() {
    }

    public static RelyingPartyRegistration buildWithSigningCredential(RelyingPartyRegistration.Builder builder, String privateKeyPath, String certPath) throws Exception {
        if (privateKeyPath == null || privateKeyPath.trim().isEmpty() || certPath == null || certPath.trim().isEmpty()) {
            LOG.info("SAML signing credentials not configured, skipping credential setup");
            throw new IllegalStateException("SAML is enabled but signing credentials are not configured. " +
                    "Set ranger.saml.sp.key and ranger.saml.sp.cert in ranger-admin-site.xml " +
                    "before starting Ranger with SAML authentication.");
        }
        RSAPrivateKey privateKey = RsaKeyConverters.pkcs8().convert(new FileSystemResource(privateKeyPath).getInputStream());
        X509Certificate certificate;
        try (FileInputStream fis = new FileInputStream(certPath)) {
            certificate = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(fis);
        }
        Saml2X509Credential signingCredential = Saml2X509Credential.signing(privateKey, certificate);
        return builder
                .signingX509Credentials(c -> c.add(signingCredential))
                .singleLogoutServiceLocation("{baseUrl}/logout/saml2/slo")
                .singleLogoutServiceResponseLocation("{baseUrl}/logout/saml2/slo")
                .singleLogoutServiceBinding(Saml2MessageBinding.POST)
                .build();
    }

    public static RelyingPartyRegistration buildFromKeystore(RelyingPartyRegistration.Builder builder, String keystorePath,
                                                             String alias, String password) throws Exception {
        if (keystorePath == null || keystorePath.trim().isEmpty()) {
            throw new IllegalArgumentException("ranger.service.https.attrib.keystore.file must not be null or empty");
        }
        if (alias == null || alias.trim().isEmpty()) {
            throw new IllegalArgumentException("ranger.service.https.attrib.keystore.keyalias must not be null or empty");
        }
        String resolvedPassword = resolvePassword(password);
        if (StringUtils.isBlank(resolvedPassword)) {
            throw new IllegalArgumentException("Could not resolve keystore password from credential store or " +
                    "ranger.service.https.attrib.keystore.pass");
        }
        KeyStore ks = KeyStore.getInstance("JKS");
        try (FileInputStream fis = new FileInputStream(keystorePath)) {
            ks.load(fis, resolvedPassword.toCharArray());
        }
        PrivateKey privateKey = (PrivateKey) ks.getKey(alias, resolvedPassword.toCharArray());
        if (privateKey == null) {
            throw new IllegalArgumentException("No key found in keystore for alias '" + alias + "'");
        }
        if (!(privateKey instanceof RSAPrivateKey)) {
            throw new IllegalArgumentException("Key for alias '" + alias + "' is not RSA. Found: " + privateKey.getClass().getName());
        }
        X509Certificate cert = (X509Certificate) ks.getCertificate(alias);
        if (cert == null) {
            throw new IllegalArgumentException("No certificate found in keystore for alias '" + alias + "'");
        }
        Saml2X509Credential signingCredential = Saml2X509Credential.signing((RSAPrivateKey) privateKey, cert);
        return builder
                .signingX509Credentials(c -> c.add(signingCredential))
                .singleLogoutServiceLocation("{baseUrl}/logout/saml2/slo")
                .singleLogoutServiceResponseLocation("{baseUrl}/logout/saml2/slo")
                .singleLogoutServiceBinding(Saml2MessageBinding.POST)
                .build();
    }

    private static String resolvePassword(String rawPassword) {
        try {
            String providerPath = PropertiesUtil.getProperty("ranger.credential.provider.path");
            String keyAlias = PropertiesUtil.getProperty("ranger.service.https.attrib.keystore.credential.alias",
                    "keyStoreCredentialAlias");
            if (StringUtils.isNotBlank(providerPath) && StringUtils.isNotBlank(keyAlias)) {
                String resolved = CredentialReader.getDecryptedString(providerPath.trim(), keyAlias.trim(),
                        PropertiesUtil.getProperty("ranger.keystore.file.type", "jks"));
                if (StringUtils.isNotBlank(resolved) && !"none".equalsIgnoreCase(resolved.trim())) {
                    return resolved;
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not resolve password from credential store, falling back to config value", e);
        }
        return rawPassword; // plain text fallback
    }
}
