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
package org.apache.ranger.services.nifi.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.RangerJersey2ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.InputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * Client to communicate with NiFi and retrieve available resources.
 */
public class NiFiClient {
    private static final Logger LOG = LoggerFactory.getLogger(NiFiClient.class);

    static final String SUCCESS_MSG = "ConnectionTest Successful";
    static final String FAILURE_MSG = "Unable to retrieve any resources using given parameters. ";

    private final String           url;
    private final SSLContext       sslContext;
    private final HostnameVerifier hostnameVerifier;
    private final ObjectMapper     mapper = new ObjectMapper();
    private final Client client;

    public NiFiClient(final String url, final SSLContext sslContext) {
        this.url              = url;
        this.sslContext       = sslContext;
        this.hostnameVerifier = new NiFiHostnameVerifier();
        this.client           = buildClient();
    }

    protected Client buildClient() {
        // Use RangerJersey2ClientBuilder instead of unsafe ClientBuilder.newBuilder() to prevent MOXy usage
        if (sslContext != null) {
            return RangerJersey2ClientBuilder.createSecureClient(sslContext, hostnameVerifier, 30000, 30000);
        } else {
            return RangerJersey2ClientBuilder.createStandardClient();
        }
    }

    public HashMap<String, Object> connectionTest() {
        String                  errMsg       = "";
        boolean                 connectivityStatus;
        HashMap<String, Object> responseData = new HashMap<>();

        try {
            final WebTarget webTarget = getWebTarget();
            final Response response = getResponse(webTarget, MediaType.APPLICATION_JSON);

            LOG.debug("Got response from NiFi with status code {}", response.getStatus());

            if (Response.Status.OK.getStatusCode() == response.getStatus()) {
                connectivityStatus = true;
            } else {
                connectivityStatus = false;
                errMsg = "Status Code = " + response.getStatus();
                // Read the error message from the response entity
                try (InputStream is = response.readEntity(InputStream.class)) {
                    errMsg += ": " + IOUtils.toString(is);
                }
            }
        } catch (ProcessingException | WebApplicationException e) {
            LOG.error("Connection to NiFi failed due to {}", e.getMessage(), e);
            connectivityStatus = false;
            errMsg = Optional.ofNullable(e.getMessage()).orElse("Unknown error");
        } catch (Exception e) {
            LOG.error("Connection to NiFi failed due to {}", e.getMessage(), e);
            connectivityStatus = false;
            errMsg             = e.getMessage();
        }

        if (connectivityStatus) {
            BaseClient.generateResponseDataMap(true, SUCCESS_MSG, SUCCESS_MSG, null, null, responseData);
        } else {
            BaseClient.generateResponseDataMap(false, FAILURE_MSG, FAILURE_MSG + errMsg, null, null, responseData);
        }

        LOG.debug("Response Data - {}", responseData);

        return responseData;
    }

    public List<String> getResources(ResourceLookupContext context) throws Exception {
        final WebTarget webTarget = getWebTarget();
        final Response response = getResponse(webTarget, MediaType.APPLICATION_JSON);

        if (Response.Status.OK.getStatusCode() != response.getStatus()) {
            String errorMsg = response.readEntity(String.class);
            throw new Exception("Unable to retrieve resources from NiFi due to: " + errorMsg);
        }

        InputStream inputStream = response.readEntity(InputStream.class);
        JsonNode rootNode = mapper.readTree(inputStream);
        if (rootNode == null) {
            throw new Exception("Unable to retrieve resources from NiFi");
        }

        JsonNode     resourcesNode = rootNode.findValue("resources");
        List<String> identifiers   = resourcesNode.findValuesAsText("identifier");

        final String userInput = context.getUserInput();
        if (StringUtils.isBlank(userInput)) {
            return identifiers;
        } else {
            List<String> filteredIdentifiers = new ArrayList<>();

            for (String identifier : identifiers) {
                if (identifier.contains(userInput)) {
                    filteredIdentifiers.add(identifier);
                }
            }

            return filteredIdentifiers;
        }
    }

    public String getUrl() {
        return url;
    }

    public SSLContext getSslContext() {
        return sslContext;
    }

    public HostnameVerifier getHostnameVerifier() {
        return hostnameVerifier;
    }

    protected WebTarget getWebTarget() {
        return client.target(url);
    }

    protected Response getResponse(WebTarget webTarget, String accept) {
        return webTarget.request(accept).get();
    }

    /**
     * Custom hostname verifier that checks subject alternative names against the hostname of the URI.
     */
    private static class NiFiHostnameVerifier implements HostnameVerifier {
        @Override
        public boolean verify(final String hostname, final SSLSession ssls) {
            try {
                Certificate[] certificates = ssls.getPeerCertificates();
                if (certificates == null || certificates.length == 0) {
                    return false;
                }
                // verify hostname against server certificate[0]
                if (certificates[0] instanceof X509Certificate) {
                    final X509Certificate x509Cert = (X509Certificate) certificates[0];
                    final List<String> subjectAltNames = getSubjectAlternativeNames(x509Cert);
                    return subjectAltNames.contains(hostname.toLowerCase());
                }
            } catch (final SSLPeerUnverifiedException | CertificateParsingException ex) {
                LOG.warn("Hostname Verification encountered exception verifying hostname due to: {}", ex, ex);
            }

            return false;
        }

        private List<String> getSubjectAlternativeNames(final X509Certificate certificate) throws CertificateParsingException {
            final Collection<List<?>> altNames = certificate.getSubjectAlternativeNames();
            if (altNames == null) {
                return new ArrayList<>();
            }

            final List<String> result = new ArrayList<>();
            for (final List<?> generalName : altNames) {
                /**
                 * generalName has the name type as the first element a String or byte array for the second element. We return any general names that are String types.
                 * We don't inspect the numeric name type because some certificates incorrectly put IPs and DNS names under the wrong name types.
                 */
                if (generalName.size() > 1) {
                    final Object value = generalName.get(1);
                    if (value instanceof String) {
                        result.add(((String) value).toLowerCase());
                    }
                }
            }
            return result;
        }
    }
}
