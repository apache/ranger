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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.authz.model.RangerAccessContext;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.apache.ranger.authz.model.RangerResourceInfo;
import org.apache.ranger.authz.model.RangerResourcePermissions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestEmbeddedAuthorizer {
    private static final TypeReference<List<TestAuthzData>>               TYPE_LIST_TEST_AUTHZ_DATA                = new TypeReference<List<TestAuthzData>>() {};
    private static final TypeReference<List<TestMultiAuthzData>>          TYPE_LIST_TEST_MULTI_AUTHZ_DATA          = new TypeReference<List<TestMultiAuthzData>>() {};
    private static final TypeReference<List<TestResourcePermissionsData>> TYPE_LIST_TEST_RESOURCE_PERMISSIONS_DATA = new TypeReference<List<TestResourcePermissionsData>>() {};

    @Test
    public void testAuthzS3() throws Exception {
        runAuthzTest("test_s3");
    }

    @Test
    public void testMultiAuthzS3() throws Exception {
        runMultiAuthzTest("test_s3");
    }

    @Test
    public void testResourcePermissionsS3() throws Exception {
        runResourcePermissionsTest("test_s3");
    }

    @Test
    public void testAuthzHive() throws Exception {
        runAuthzTest("test_hive");
    }

    @Test
    public void testResourcePermissionsHive() throws Exception {
        runResourcePermissionsTest("test_hive");
    }

    private void runResourcePermissionsTest(String testName) throws Exception {
        String propertiesPath = "/" + testName + "/ranger-embedded-authz.properties";
        String testsPath      = "/" + testName + "/tests_resource_permissions.json";

        RangerEmbeddedAuthorizer authorizer = null;

        try {
            authorizer = new RangerEmbeddedAuthorizer(loadProperties(propertiesPath));

            authorizer.init();

            for (TestResourcePermissionsData test : loadTestResourcePermissionsData(testsPath)) {
                if (test.resource == null || test.context == null || test.permissions == null) {
                    continue;
                }

                RangerResourcePermissions permissions = authorizer.getResourcePermissions(test.resource, test.context);

                assertEquals(test.permissions, permissions, "Resource permissions do not match for resource=" + test.resource);
            }
        } finally {
            if (authorizer != null) {
                authorizer.close();
            }
        }
    }

    private void runAuthzTest(String testName) throws Exception {
        String propertiesPath = "/" + testName + "/ranger-embedded-authz.properties";
        String testsPath      = "/" + testName + "/tests_authz.json";

        RangerEmbeddedAuthorizer authorizer = null;

        try {
            authorizer = new RangerEmbeddedAuthorizer(loadProperties(propertiesPath));

            authorizer.init();

            for (TestAuthzData test : loadTestAuthzData(testsPath)) {
                if (test.request == null || test.result == null) {
                    continue;
                }

                RangerAuthzRequest request = test.request;
                RangerAuthzResult  expected = test.result;
                RangerAuthzResult  result   = authorizer.authorize(request);

                assertEquals(expected, result);
            }
        } finally {
            if (authorizer != null) {
                authorizer.close();
            }
        }
    }

    private void runMultiAuthzTest(String testName) throws Exception {
        String propertiesPath = "/" + testName + "/ranger-embedded-authz.properties";
        String testsPath      = "/" + testName + "/tests_multi_authz.json";

        RangerEmbeddedAuthorizer authorizer = null;

        try {
            authorizer = new RangerEmbeddedAuthorizer(loadProperties(propertiesPath));

            authorizer.init();

            for (TestMultiAuthzData test : loadTestMultiAuthzData(testsPath)) {
                if (test.request == null || test.result == null) {
                    continue;
                }

                RangerMultiAuthzRequest request  = test.request;
                RangerMultiAuthzResult  expected = test.result;
                RangerMultiAuthzResult  result   = authorizer.authorize(request);

                assertEquals(expected, result);
            }
        } finally {
            if (authorizer != null) {
                authorizer.close();
            }
        }
    }

    private Properties loadProperties(String resourcePath) throws Exception {
        Properties properties = new Properties();

        try (InputStream in = getClass().getResourceAsStream(resourcePath)) {
            properties.load(in);
        }

        return properties;
    }

    private List<TestAuthzData> loadTestAuthzData(String resourcePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try (InputStream in = getClass().getResourceAsStream(resourcePath)) {
            return mapper.readValue(in, TYPE_LIST_TEST_AUTHZ_DATA);
        }
    }

    private List<TestMultiAuthzData> loadTestMultiAuthzData(String resourcePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try (InputStream in = getClass().getResourceAsStream(resourcePath)) {
            return in != null ? mapper.readValue(in, TYPE_LIST_TEST_MULTI_AUTHZ_DATA) : Collections.emptyList();
        }
    }

    private List<TestResourcePermissionsData> loadTestResourcePermissionsData(String resourcePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try (InputStream in = getClass().getResourceAsStream(resourcePath)) {
            return in != null ? mapper.readValue(in, TYPE_LIST_TEST_RESOURCE_PERMISSIONS_DATA) : Collections.emptyList();
        }
    }

    private static class TestAuthzData {
        public RangerAuthzRequest request;
        public RangerAuthzResult  result;
    }

    private static class TestMultiAuthzData {
        public RangerMultiAuthzRequest request;
        public RangerMultiAuthzResult  result;
    }

    private static class TestResourcePermissionsData {
        public RangerResourceInfo        resource;
        public RangerAccessContext       context;
        public RangerResourcePermissions permissions;
    }
}
