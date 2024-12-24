/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.services.kms;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.kms.client.KMSResourceMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerServiceKMS extends RangerBaseService {
    private static final Logger LOG = LoggerFactory.getLogger(RangerServiceKMS.class);

    public static final String ACCESS_TYPE_DECRYPT_EEK  = "decrypteek";
    public static final String ACCESS_TYPE_GENERATE_EEK = "generateeek";
    public static final String ACCESS_TYPE_GET_METADATA = "getmetadata";
    public static final String ACCESS_TYPE_GET          = "get";

    public RangerServiceKMS() {
        super();
    }

    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
    }

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> ret         = new HashMap<>();
        String              serviceName = getServiceName();

        LOG.debug("==> RangerServiceKMS.validateConfig Service: ({})", serviceName);

        if (configs != null) {
            try {
                ret = KMSResourceMgr.validateConfig(serviceName, configs);
            } catch (Exception e) {
                LOG.error("<== RangerServiceKMS.validateConfig Error:{}", String.valueOf(e));

                throw e;
            }
        }

        LOG.debug("<== RangerServiceKMS.validateConfig Response : ({})", ret);

        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) {
        List<String>        ret         = new ArrayList<>();
        String              serviceName = getServiceName();
        Map<String, String> configs     = getConfigs();

        LOG.debug("==> RangerServiceKMS.lookupResource Context: ({})", context);

        if (context != null) {
            try {
                ret = KMSResourceMgr.getKMSResources(serviceName, configs, context);
            } catch (Exception e) {
                LOG.error("<==RangerServiceKMS.lookupResource Error : {}", String.valueOf(e));

                throw e;
            }
        }

        LOG.debug("<== RangerServiceKMS.lookupResource Response: ({})", ret);

        return ret;
    }

    @Override
    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
        LOG.debug("==> RangerServiceKMS.getDefaultRangerPolicies()");

        List<RangerPolicy> ret = super.getDefaultRangerPolicies();

        String adminPrincipal = getConfig().get(ADMIN_USER_PRINCIPAL);
        String adminKeytab    = getConfig().get(ADMIN_USER_KEYTAB);
        String authType       = getConfig().get(RANGER_AUTH_TYPE, "simple");
        String adminUser      = getLookupUser(authType, adminPrincipal, adminKeytab);

        // Add default policies for HDFS, HIVE, HABSE & OM users.
        List<RangerAccessTypeDef> hdfsAccessTypeDefs  = new ArrayList<>();
        List<RangerAccessTypeDef> omAccessTypeDefs    = new ArrayList<>();
        List<RangerAccessTypeDef> hiveAccessTypeDefs  = new ArrayList<>();
        List<RangerAccessTypeDef> hbaseAccessTypeDefs = new ArrayList<>();

        for (RangerAccessTypeDef accessTypeDef : serviceDef.getAccessTypes()) {
            if (accessTypeDef.getName().equalsIgnoreCase(ACCESS_TYPE_GET_METADATA)) {
                hdfsAccessTypeDefs.add(accessTypeDef);
                omAccessTypeDefs.add(accessTypeDef);
                hiveAccessTypeDefs.add(accessTypeDef);
            } else if (accessTypeDef.getName().equalsIgnoreCase(ACCESS_TYPE_GENERATE_EEK)) {
                hdfsAccessTypeDefs.add(accessTypeDef);
                omAccessTypeDefs.add(accessTypeDef);
            } else if (accessTypeDef.getName().equalsIgnoreCase(ACCESS_TYPE_DECRYPT_EEK)) {
                hiveAccessTypeDefs.add(accessTypeDef);
                hbaseAccessTypeDefs.add(accessTypeDef);
            }
        }

        for (RangerPolicy defaultPolicy : ret) {
            if (defaultPolicy.getName().contains("all") && StringUtils.isNotBlank(lookUpUser)) {
                RangerPolicyItem policyItemForLookupUser = new RangerPolicyItem();

                policyItemForLookupUser.setUsers(Collections.singletonList(lookUpUser));
                policyItemForLookupUser.setAccesses(Collections.singletonList(new RangerPolicyItemAccess(ACCESS_TYPE_GET)));
                policyItemForLookupUser.setDelegateAdmin(false);

                defaultPolicy.addPolicyItem(policyItemForLookupUser);
            }

            for (RangerPolicy.RangerPolicyItem item : defaultPolicy.getPolicyItems()) {
                if (StringUtils.isNotBlank(adminUser)) {
                    item.addUser(adminUser);
                }
            }

            String hdfsUser = getConfig().get("ranger.kms.service.user.hdfs", "hdfs");

            if (hdfsUser != null && !hdfsUser.isEmpty()) {
                LOG.info("Creating default KMS policy item for {}", hdfsUser);

                List<String> users = new ArrayList<>();

                users.add(hdfsUser);

                RangerPolicyItem policyItem = createDefaultPolicyItem(hdfsAccessTypeDefs, users);

                defaultPolicy.addPolicyItem(policyItem);
            }

            final String omUser = getConfig().get("ranger.kms.service.user.om", "om");

            if (StringUtils.isNotEmpty(omUser)) {
                LOG.info("Creating default KMS policy item for {}", omUser);

                List<String> users = new ArrayList<>();

                users.add(omUser);

                RangerPolicyItem policyItem = createDefaultPolicyItem(omAccessTypeDefs, users);

                defaultPolicy.addPolicyItem(policyItem);
            }

            String hiveUser = getConfig().get("ranger.kms.service.user.hive", "hive");

            if (hiveUser != null && !hiveUser.isEmpty()) {
                LOG.info("Creating default KMS policy item for {}", hiveUser);

                List<String> users = new ArrayList<>();

                users.add(hiveUser);

                RangerPolicyItem policyItem = createDefaultPolicyItem(hiveAccessTypeDefs, users);

                defaultPolicy.addPolicyItem(policyItem);
            }

            String hbaseUser = getConfig().get("ranger.kms.service.user.hbase", "hbase");

            if (hbaseUser != null && !hbaseUser.isEmpty()) {
                LOG.info("Creating default KMS policy item for {}", hbaseUser);

                List<String> users = new ArrayList<>();

                users.add(hbaseUser);

                RangerPolicyItem policyItem = createDefaultPolicyItem(hbaseAccessTypeDefs, users);

                defaultPolicy.addPolicyItem(policyItem);
            }
        }

        LOG.debug("<== RangerServiceKMS.getDefaultRangerPolicies() : {}", ret);

        return ret;
    }

    private RangerPolicy.RangerPolicyItem createDefaultPolicyItem(List<RangerAccessTypeDef> accessTypeDefs, List<String> users) {
        LOG.debug("==> RangerServiceTag.createDefaultPolicyItem()");

        RangerPolicyItem policyItem = new RangerPolicyItem();

        policyItem.setUsers(users);

        List<RangerPolicyItemAccess> accesses = new ArrayList<>();

        for (RangerAccessTypeDef accessTypeDef : accessTypeDefs) {
            RangerPolicyItemAccess access = new RangerPolicyItemAccess();

            access.setType(accessTypeDef.getName());
            access.setIsAllowed(true);

            accesses.add(access);
        }

        policyItem.setAccesses(accesses);
        policyItem.setDelegateAdmin(true);

        LOG.debug("<== RangerServiceTag.createDefaultPolicyItem(): {}", policyItem);

        return policyItem;
    }
}
