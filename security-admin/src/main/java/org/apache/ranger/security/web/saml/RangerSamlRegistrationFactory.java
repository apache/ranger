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

import org.springframework.core.io.FileSystemResource;
import org.springframework.security.converter.RsaKeyConverters;
import org.springframework.security.saml2.core.Saml2X509Credential;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;
import org.springframework.security.saml2.provider.service.registration.Saml2MessageBinding;

import java.io.FileInputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;

public class RangerSamlRegistrationFactory {
    private RangerSamlRegistrationFactory(){}

    public static RelyingPartyRegistration buildWithSigningCredential(RelyingPartyRegistration.Builder builder, String privateKeyPath, String certPath) throws Exception {
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
                .singleLogoutServiceBinding(Saml2MessageBinding.REDIRECT)
                .build();
    }
}
