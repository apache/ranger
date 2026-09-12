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
package org.apache.ranger.services.s3;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.s3.client.S3ResourceMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerServiceS3 extends RangerBaseService {
    private static final Logger LOG = LoggerFactory.getLogger(RangerServiceS3.class);

    public static final String ACCESS_TYPE_LIST_BUCKET = "s3:ListBucket";

    public RangerServiceS3() {
        super();
    }

    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
    }

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> ret = new HashMap<String, Object>();
        String serviceName = getServiceName();
        LOG.info("==> RangerServiceS3.validateConfig Service: (" + serviceName + " )");

        if (configs != null) {
            try {
                ret = S3ResourceMgr.connectionTest(serviceName, configs);
            } catch (S3Exception e) {
                LOG.error("<== RangerServiceS3.validateConfig Error: " + e.getMessage(), e);
                throw e;
            }
        }

        LOG.info("<== RangerServiceS3.validateConfig Response : (" + ret + " )");

        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        List<String> ret = new ArrayList<String>();
        String serviceName = getServiceName();
        Map<String, String> configs = getConfigs();

        LOG.info("==> RangerServiceS3.lookupResource Context: (" + context + ")");

        if (context != null) {
            try {
                long timeLookup = Long.parseLong(this.config.getProperties().getProperty("ranger.resource.lookup.timeout.value.in.ms"));
                ret = S3ResourceMgr.getS3Resources(serviceName, configs, context, timeLookup);
            } catch (S3Exception e) {
                LOG.error("<==RangerServiceS3.lookupResource Error : " + e);
                throw e;
            }
        }

        LOG.info("<== RangerServiceS3.lookupResource Response Received");

        return ret;
    }

    @Override
    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceS3.getDefaultRangerPolicies()");
        }

        List<RangerPolicy> ret = super.getDefaultRangerPolicies();

        for (RangerPolicy defaultPolicy : ret) {
            String lookUpUser = configs.get(RangerS3Constants.USER_NAME);
            String bucketName = configs.get(RangerS3Constants.BUCKET_NAME);

            if (defaultPolicy.getName().contains("all") && StringUtils.isNotBlank(lookUpUser)) {
                // Update the resource path value to use bucket name instead of wildcard
                RangerPolicy.RangerPolicyResource pathPolicyResource = defaultPolicy.getResources().get(RangerS3Constants.PATH);
                if (pathPolicyResource != null) {
                    // Clear existing values and set bucket name
                    pathPolicyResource.setValues(Collections.singletonList(bucketName));
                }

                // Clear existing policy items and create a new one with only ListBucket access
                defaultPolicy.getPolicyItems().clear();

                RangerPolicy.RangerPolicyItem policyItemForLookupUser = new RangerPolicy.RangerPolicyItem();

                // Set only ListBucket access
                List<RangerPolicy.RangerPolicyItemAccess> accessListForLookupUser = new ArrayList<>();
                accessListForLookupUser.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_LIST_BUCKET, true));
                policyItemForLookupUser.setAccesses(accessListForLookupUser);

                // Set the lookup user
                policyItemForLookupUser.setUsers(Collections.singletonList(lookUpUser));

                // Set delegate admin to true (or false based on your requirement)
                policyItemForLookupUser.setDelegateAdmin(true);

                // Add the modified policy item
                defaultPolicy.addPolicyItem(policyItemForLookupUser);

                LOG.info("Modified default policy for S3 service: policy=" + defaultPolicy.getName() +
                        ", bucketName=" + bucketName + ", user=" + lookUpUser + ", access=ListBucket");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceS3.getDefaultRangerPolicies()");
        }

        return ret;
    }
}
