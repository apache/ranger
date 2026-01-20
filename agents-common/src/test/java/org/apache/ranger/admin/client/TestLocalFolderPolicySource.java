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

package org.apache.ranger.admin.client;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestLocalFolderPolicySource {
    @Test
    public void testLoadWithServiceNamePrefix() throws Exception {
        RangerPolicySource source = getPolicySource("s3", "dev_s3", null);

        assertNotNull(source.getServicePoliciesIfUpdated(-1, -1), "failed to load policies for service=dev_s3, appId=null");
        assertNotNull(source.getServiceTagsIfUpdated(-1, -1), "failed to load tags for service=dev_s3, appId=null");
        assertNotNull(source.getRolesIfUpdated(-1, -1), "failed to load roles for service=dev_s3, appId=null");
        assertNotNull(source.getUserStoreIfUpdated(-1, -1), "failed to load userStore for service=dev_s3, appId=null");
    }

    @Test
    public void testLoadWithServiceNamePrefixIgnoreAppId() throws Exception {
        RangerPolicySource source = getPolicySource("s3", "dev_s3", "s3proxy");

        assertNotNull(source.getServicePoliciesIfUpdated(-1, -1), "failed to load policies for service=dev_s3, appId=s3proxy");
        assertNotNull(source.getServiceTagsIfUpdated(-1, -1), "failed to load tags for service=dev_s3, appId=s3proxy");
        assertNotNull(source.getRolesIfUpdated(-1, -1), "failed to load roles for service=dev_s3, appId=s3proxy");
        assertNotNull(source.getUserStoreIfUpdated(-1, -1), "failed to load userStore for service=dev_s3, appId=s3proxy");
    }

    @Test
    public void testLoadWithAppIdServiceNamePrefix() throws Exception {
        RangerPolicySource source = getPolicySource("hive", "dev_hive", "hiveServer2");

        assertNotNull(source.getServicePoliciesIfUpdated(-1, -1), "failed to load policies for service=dev_hive, appId=hiveServer2");
        assertNotNull(source.getServiceTagsIfUpdated(-1, -1), "failed to load tags for service=dev_hive, appId=hiveServer2");
        assertNotNull(source.getRolesIfUpdated(-1, -1), "failed to load roles for service=dev_hive, appId=hiveServer2");
        assertNotNull(source.getUserStoreIfUpdated(-1, -1), "failed to load userStore for service=dev_hive, appId=hiveServer2");
    }

    @Test
    public void testLoadFailureWithNonExistingServiceName() throws Exception {
        RangerPolicySource source = getPolicySource("s3", "test_s3", null);

        assertThrows(Exception.class, () -> source.getServicePoliciesIfUpdated(-1, -1), "expected to not find policies for service=test_s3, appId=null");
        assertThrows(Exception.class, () -> source.getServiceTagsIfUpdated(-1, -1), "expected to not find tags for service=test_s3, appId=null");
        assertThrows(Exception.class, () -> source.getRolesIfUpdated(-1, -1), "expected to not find roles for service=test_s3, appId=null");
        assertThrows(Exception.class, () -> source.getUserStoreIfUpdated(-1, -1), "expected to not find userStore for service=test_s3, appId=null");
    }

    @Test
    public void testLoadFailureWithNonExistingServiceNameAndAppId() throws Exception {
        RangerPolicySource source = getPolicySource("hive", "test_hive", "trino");

        assertThrows(Exception.class, () -> source.getServicePoliciesIfUpdated(-1, -1), "expected to not find policies for service=test_hive, appId=trino");
        assertThrows(Exception.class, () -> source.getServiceTagsIfUpdated(-1, -1), "expected to not find tags for service=test_hive, appId=trino");
        assertThrows(Exception.class, () -> source.getRolesIfUpdated(-1, -1), "expected to not find roles for service=test_hive, appId=trino");
        assertThrows(Exception.class, () -> source.getUserStoreIfUpdated(-1, -1), "expected to not find userStore for service=test_hive, appId=trino");
    }

    private RangerPolicySource getPolicySource(String serviceType, String serviceName, String appId) {
        String        propPrefix = "ranger.plugin." + serviceType;
        Configuration conf       = new Configuration();

        conf.set(propPrefix + ".policy.source.local_folder.path", "src/test/resources/admin.client");

        LocalFolderPolicySource source = new LocalFolderPolicySource();

        source.init(serviceName, appId, propPrefix, conf);

        return source;
    }
}
