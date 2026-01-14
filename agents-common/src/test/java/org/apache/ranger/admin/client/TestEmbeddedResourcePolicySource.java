package org.apache.ranger.admin.client;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestEmbeddedResourcePolicySource {
    @Test
    public void testLoadWithServiceNamePrefix() throws Exception {
        RangerPolicySource source = getPolicySource("s3", "dev_s3", null);

        assertNotNull(source.getServicePoliciesIfUpdated(-1, -1), "failed to load policies for service=dev_s3, appId=null");
        assertNotNull(source.getServiceTagsIfUpdated(-1, -1), "failed to load tags for service=dev_s3, appId=null");
        assertNotNull(source.getRolesIfUpdated(-1, -1), "failed to load roles for service=dev_s3, appId=null");
        assertNotNull(source.getUserStoreIfUpdated(-1, -1), "failed to load userStore for service=dev_s3, appId=null");
        assertNotNull(source.getGdsInfoIfUpdated(-1, -1), "failed to load gdsInfo for service=dev_s3, appId=null");
    }

    @Test
    public void testLoadWithServiceNamePrefixIgnoreAppId() throws Exception {
        RangerPolicySource source = getPolicySource("s3", "dev_s3", "s3proxy");

        assertNotNull(source.getServicePoliciesIfUpdated(-1, -1), "failed to load policies for service=dev_s3, appId=s3proxy");
        assertNotNull(source.getServiceTagsIfUpdated(-1, -1), "failed to load tags for service=dev_s3, appId=s3proxy");
        assertNotNull(source.getRolesIfUpdated(-1, -1), "failed to load roles for service=dev_s3, appId=s3proxy");
        assertNotNull(source.getUserStoreIfUpdated(-1, -1), "failed to load userStore for service=dev_s3, appId=s3proxy");
        assertNotNull(source.getGdsInfoIfUpdated(-1, -1), "failed to load gdsInfo for service=dev_s3, appId=s3proxy");
    }

    @Test
    public void testLoadWithAppIdServiceNamePrefix() throws Exception {
        RangerPolicySource source = getPolicySource("hive", "dev_hive", "hiveServer2");

        assertNotNull(source.getServicePoliciesIfUpdated(-1, -1), "failed to load policies for service=dev_hive, appId=hiveServer2");
        assertNotNull(source.getServiceTagsIfUpdated(-1, -1), "failed to load tags for service=dev_hive, appId=hiveServer2");
        assertNotNull(source.getRolesIfUpdated(-1, -1), "failed to load roles for service=dev_hive, appId=hiveServer2");
        assertNotNull(source.getUserStoreIfUpdated(-1, -1), "failed to load userStore for service=dev_hive, appId=hiveServer2");
        assertNotNull(source.getGdsInfoIfUpdated(-1, -1), "failed to load gdsInfo for service=dev_hive, appId=hiveServer2");
    }

    @Test
    public void testLoadFailureWithNonExistingServiceName() throws Exception {
        RangerPolicySource source = getPolicySource("s3", "test_s3", null);

        assertThrows(Exception.class, () -> source.getServicePoliciesIfUpdated(-1, -1), "expected to not find policies for service=test_s3, appId=null");
        assertThrows(Exception.class, () -> source.getServiceTagsIfUpdated(-1, -1), "expected to not find tags for service=test_s3, appId=null");
        assertThrows(Exception.class, () -> source.getRolesIfUpdated(-1, -1), "expected to not find roles for service=test_s3, appId=null");
        assertThrows(Exception.class, () -> source.getUserStoreIfUpdated(-1, -1), "expected to not find userStore for service=test_s3, appId=null");
        assertThrows(Exception.class, () -> source.getGdsInfoIfUpdated(-1, -1), "expected to not find gdsInfo for service=test_s3, appId=null");
    }

    @Test
    public void testLoadFailureWithNonExistingServiceNameAndAppId() throws Exception {
        RangerPolicySource source = getPolicySource("hive", "test_hive", "trino");

        assertThrows(Exception.class, () -> source.getServicePoliciesIfUpdated(-1, -1), "expected to not find policies for service=test_hive, appId=trino");
        assertThrows(Exception.class, () -> source.getServiceTagsIfUpdated(-1, -1), "expected to not find tags for service=test_hive, appId=trino");
        assertThrows(Exception.class, () -> source.getRolesIfUpdated(-1, -1), "expected to not find roles for service=test_hive, appId=trino");
        assertThrows(Exception.class, () -> source.getUserStoreIfUpdated(-1, -1), "expected to not find userStore for service=test_hive, appId=trino");
        assertThrows(Exception.class, () -> source.getGdsInfoIfUpdated(-1, -1), "expected to not find gdsInfo for service=test_hive, appId=trino");
    }

    private RangerPolicySource getPolicySource(String serviceType, String serviceName, String appId) {
        String        propPrefix = "ranger.plugin." + serviceType;
        Configuration conf       = new Configuration();

        conf.set(propPrefix + ".policy.source.embedded_resource.path", "/admin.client");

        EmbeddedResourcePolicySource source = new EmbeddedResourcePolicySource();

        source.init(serviceName, appId, propPrefix, conf);

        return source;
    }
}
