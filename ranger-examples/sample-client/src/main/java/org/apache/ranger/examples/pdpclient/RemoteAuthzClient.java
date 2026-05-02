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
package org.apache.ranger.examples.pdpclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.authz.api.RangerAuthorizer;
import org.apache.ranger.authz.api.RangerAuthorizerFactory;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.authz.model.RangerAuthzResult;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public final class RemoteAuthzClient {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String DEFAULT_PROPERTIES_LOCATION = "ranger-authz-remote-authn-jwt.properties";
    private static final String USAGE                       = "Usage: RemoteAuthzClient <request-json-file> [properties-file|classpath:resource]";

    private RemoteAuthzClient() {
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            throw new IllegalArgumentException(USAGE);
        }

        String     requestLocation    = args[0];
        String     propertiesLocation = args.length > 1 ? args[1] : DEFAULT_PROPERTIES_LOCATION;
        Properties properties         = loadProperties(propertiesLocation);

        System.out.println("Loaded properties from: " + propertiesLocation);

        RangerAuthorizer authorizer = null;

        try {
            authorizer = RangerAuthorizerFactory.createAuthorizer(properties);

            authorizer.init();

            RangerAuthzRequest request = loadRequest(requestLocation);

            System.out.println("Loaded request from: " + requestLocation);

            RangerAuthzResult result = authorizer.authorize(request);

            System.out.println(OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(result));
        } finally {
            if (authorizer != null) {
                authorizer.close();
            }
        }
    }

    static Properties loadProperties(String location) throws Exception {
        Properties properties = new Properties();

        try (InputStream input = openInputStream(location)) {
            properties.load(input);
        }

        return properties;
    }

    static RangerAuthzRequest loadRequest(String location) throws Exception {
        try (InputStream input = openInputStream(location)) {
            return OBJECT_MAPPER.readValue(input, RangerAuthzRequest.class);
        }
    }

    private static InputStream openInputStream(String location) throws Exception {
        if (location != null && location.startsWith("classpath:")) {
            String resourcePath = location.substring("classpath:".length());

            if (!resourcePath.startsWith("/")) {
                resourcePath = "/" + resourcePath;
            }

            InputStream input = RemoteAuthzClient.class.getResourceAsStream(resourcePath);

            if (input == null) {
                throw new IllegalArgumentException("Classpath resource not found: " + resourcePath);
            }

            return input;
        }

        return new FileInputStream(location);
    }
}
