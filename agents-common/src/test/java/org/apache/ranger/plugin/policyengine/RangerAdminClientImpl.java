/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.plugin.policyengine;

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.admin.client.AbstractRangerAdminClient;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;

public class RangerAdminClientImpl
        extends AbstractRangerAdminClient {
    private static final String RANGER_TEST_BASEDIR = "RANGER_TEST_BASEDIR";
    private static final String RANGER_RESOURCE_BASE = "RANGER_RESOURCE_BASE";
    private static final String policiesFilepath = "/src/test/resources/policyengine/hbase-test-policies.json";
    private static final String updatedPoliciesFilepath = "/src/test/resources/policyengine/updated-hbase-test-policies.json";
    private static final String serviceDefFilename = "/src/test/resources/policyengine/ranger-servicedef-hbase.json";
    private static final String tagServiceDefFilename = "/src/test/resources/policyengine/ranger-servicedef-tag.json";

    @SuppressWarnings("unused")
    private String serviceName;
    @SuppressWarnings("unused")
    private String appId;
    private volatile boolean isLoadPolicyDelta;

    @Override
    public void init(String serviceName, String appId, String configPropertyPrefix, Configuration config) {
        super.init(serviceName, appId, configPropertyPrefix, config);
        this.serviceName = serviceName;
        this.appId = appId;
    }

    @Override
    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis)
            throws Exception {
        String basedir = System.getProperty(RANGER_TEST_BASEDIR);
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }
        String resourceBasePath = System.getProperty(RANGER_RESOURCE_BASE);
        if (resourceBasePath == null) {
            resourceBasePath = ".";
        }
        byte[] policiesBytes;
        if (!isLoadPolicyDelta) {
            policiesBytes = Files.readAllBytes(FileSystems.getDefault().getPath(basedir, policiesFilepath));
        } else {
            policiesBytes = Files.readAllBytes(FileSystems.getDefault().getPath(basedir, updatedPoliciesFilepath));
        }
        ServicePolicies ret = gson.fromJson(new String(policiesBytes, Charset.defaultCharset()), ServicePolicies.class);
        byte[] serviceDefBytes = Files.readAllBytes(FileSystems.getDefault().getPath(basedir, serviceDefFilename));
        byte[] tagServiceDefBytes = Files.readAllBytes(FileSystems.getDefault().getPath(basedir, tagServiceDefFilename));
        RangerServiceDef serviceDef = gson.fromJson(new String(serviceDefBytes, Charset.defaultCharset()), RangerServiceDef.class);
        RangerServiceDef tagServiceDef = gson.fromJson(new String(tagServiceDefBytes, Charset.defaultCharset()), RangerServiceDef.class);
        ret.setServiceDef(serviceDef);
        if (ret.getTagPolicies() == null) {
            ret.setTagPolicies(new ServicePolicies.TagPolicies());
        }
        ret.getTagPolicies().setServiceDef(tagServiceDef);
        if (!isLoadPolicyDelta) {
            isLoadPolicyDelta = true;
        }
        return ret;
    }
}
