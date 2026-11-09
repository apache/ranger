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

package org.apache.ranger.patch;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
public class PatchForAtlasResourceLabelUpdate_J10071 extends BaseLoader {
    private static final Logger LOG = LoggerFactory.getLogger(PatchForAtlasResourceLabelUpdate_J10071.class);

    private static final String ATLAS_SERVICE_DEF_NAME = EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME;
    private static final String TAG_SERVICE_DEF_NAME   = EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME;

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    ServiceDBStore svcStore;

    @Autowired
    RangerValidatorFactory validatorFactory;

    public static void main(String[] args) {
        LOG.info("main()");

        try {
            PatchForAtlasResourceLabelUpdate_J10071 loader = (PatchForAtlasResourceLabelUpdate_J10071) CLIUtil.getBean(PatchForAtlasResourceLabelUpdate_J10071.class);

            loader.init();

            while (loader.isMoreToProcess()) {
                loader.load();
            }

            LOG.info("Load complete. Exiting!!!");

            System.exit(0);
        } catch (Exception e) {
            LOG.error("Error loading", e);

            System.exit(1);
        }
    }

    @Override
    public void init() throws Exception {
        // Do Nothing
    }

    @Override
    public void printStats() {
        LOG.info("PatchForAtlasResourceLabelUpdate data");
    }

    @Override
    public void execLoad() {
        LOG.info("==> PatchForAtlasResourceLabelUpdate_J10071.execLoad()");

        try {
            updateAtlasResourceLabels();
            removeAtlasAccessFromTagServiceDef();
        } catch (Exception e) {
            LOG.error("Error while running PatchForAtlasResourceLabelUpdate_J10071", e);

            System.exit(1);
        }

        LOG.info("<== PatchForAtlasResourceLabelUpdate_J10071.execLoad()");
    }

    private void updateAtlasResourceLabels() throws Exception {
        RangerServiceDef embeddedAtlasServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(ATLAS_SERVICE_DEF_NAME);

        if (embeddedAtlasServiceDef == null) {
            LOG.error("Embedded Atlas service-definition not found");

            throw new RuntimeException("Embedded Atlas service-definition not found");
        }

        XXServiceDef xxServiceDef = daoMgr.getXXServiceDef().findByName(ATLAS_SERVICE_DEF_NAME);

        if (xxServiceDef == null) {
            LOG.info("Atlas service-def not found in DB. No patching is needed");

            return;
        }

        RangerServiceDef dbAtlasServiceDef = svcStore.getServiceDefByName(ATLAS_SERVICE_DEF_NAME);

        if (dbAtlasServiceDef == null) {
            LOG.error("Atlas service-def not found in service store");

            throw new RuntimeException("Atlas service-def not found in service store");
        }

        Map<String, String> serviceDefOptionsUpdate = dbAtlasServiceDef.getOptions();

        if (serviceDefOptionsUpdate == null) {
            serviceDefOptionsUpdate = new HashMap<>();
        }

        boolean optionsUpdated = !"false".equalsIgnoreCase(serviceDefOptionsUpdate.get(RangerServiceDef.OPTION_ENABLE_TAG_BASED_POLICIES));

        if (optionsUpdated) {
            serviceDefOptionsUpdate.put(RangerServiceDef.OPTION_ENABLE_TAG_BASED_POLICIES, "false");
            dbAtlasServiceDef.setOptions(serviceDefOptionsUpdate);
        }

        Map<String, RangerServiceDef.RangerResourceDef> embeddedResourcesByName = buildResourceMap(embeddedAtlasServiceDef.getResources());
        boolean                                         labelsUpdated           = syncResourceLabels(dbAtlasServiceDef.getResources(), embeddedResourcesByName);

        if (labelsUpdated || optionsUpdated) {
            RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);

            validator.validate(dbAtlasServiceDef, Action.UPDATE);

            RangerServiceDef ret = svcStore.updateServiceDef(dbAtlasServiceDef);

            if (ret == null) {
                throw new RuntimeException("Error while updating " + ATLAS_SERVICE_DEF_NAME + " service-def");
            }

            if (labelsUpdated && optionsUpdated) {
                LOG.info("Updated Atlas resource labels and {}=false", RangerServiceDef.OPTION_ENABLE_TAG_BASED_POLICIES);
            } else if (labelsUpdated) {
                LOG.info("Updated Atlas resource labels and descriptions from embedded service-definition");
            } else {
                LOG.info("Updated Atlas service-def option {}=false", RangerServiceDef.OPTION_ENABLE_TAG_BASED_POLICIES);
            }
        } else {
            LOG.info("Atlas resource labels and descriptions are already up-to-date. No patching is needed");
        }
    }

    private Map<String, RangerServiceDef.RangerResourceDef> buildResourceMap(List<RangerServiceDef.RangerResourceDef> resources) {
        Map<String, RangerServiceDef.RangerResourceDef> result = new HashMap<>();

        if (CollectionUtils.isNotEmpty(resources)) {
            for (RangerServiceDef.RangerResourceDef resource : resources) {
                result.put(resource.getName(), resource);
            }
        }

        return result;
    }

    private boolean syncResourceLabels(List<RangerServiceDef.RangerResourceDef> dbResources, Map<String, RangerServiceDef.RangerResourceDef> embeddedResourcesByName) {
        boolean updated = false;

        if (CollectionUtils.isNotEmpty(dbResources)) {
            for (RangerServiceDef.RangerResourceDef dbResource : dbResources) {
                RangerServiceDef.RangerResourceDef embeddedResource = embeddedResourcesByName.get(dbResource.getName());

                if (embeddedResource != null && updateResourceLabelAndDescription(dbResource, embeddedResource)) {
                    updated = true;
                }
            }
        }

        return updated;
    }

    private boolean updateResourceLabelAndDescription(RangerServiceDef.RangerResourceDef dbResource, RangerServiceDef.RangerResourceDef embeddedResource) {
        boolean updated = false;

        if (!Objects.equals(dbResource.getLabel(), embeddedResource.getLabel())) {
            dbResource.setLabel(embeddedResource.getLabel());

            updated = true;
        }

        if (!Objects.equals(dbResource.getDescription(), embeddedResource.getDescription())) {
            dbResource.setDescription(embeddedResource.getDescription());

            updated = true;
        }

        if (!Objects.equals(dbResource.getRbKeyLabel(), embeddedResource.getRbKeyLabel())) {
            dbResource.setRbKeyLabel(embeddedResource.getRbKeyLabel());

            updated = true;
        }

        if (!Objects.equals(dbResource.getRbKeyDescription(), embeddedResource.getRbKeyDescription())) {
            dbResource.setRbKeyDescription(embeddedResource.getRbKeyDescription());

            updated = true;
        }

        return updated;
    }

    private void removeAtlasAccessFromTagServiceDef() throws Exception {
        LOG.info("==> PatchForAtlasResourceLabelUpdate_J10071.removeAtlasAccessFromTagServiceDef()");

        RangerServiceDef embeddedTagServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(TAG_SERVICE_DEF_NAME);

        if (embeddedTagServiceDef == null) {
            LOG.error("Embedded Tag service-definition not found");

            return;
        }

        removeAtlasAccessesFromTagPolicies(embeddedTagServiceDef.getId());
        removeAtlasAccessTypesFromTagServiceDef();

        LOG.info("<== PatchForAtlasResourceLabelUpdate_J10071.removeAtlasAccessFromTagServiceDef()");
    }

    private void removeAtlasAccessesFromTagPolicies(Long tagServiceDefId) throws Exception {
        List<XXPolicy> xxPolicies = daoMgr.getXXPolicy().findByServiceDefId(tagServiceDefId);

        if (CollectionUtils.isEmpty(xxPolicies)) {
            return;
        }

        for (XXPolicy xxPolicy : xxPolicies) {
            RangerPolicy rangerPolicy = svcStore.getPolicy(xxPolicy.getId());

            if (CollectionUtils.isEmpty(rangerPolicy.getPolicyItems())
                    && CollectionUtils.isEmpty(rangerPolicy.getAllowExceptions())
                    && CollectionUtils.isEmpty(rangerPolicy.getDenyPolicyItems())
                    && CollectionUtils.isEmpty(rangerPolicy.getDenyExceptions())) {
                continue;
            }

            boolean policyUpdated = removeAtlasAccessesFromPolicyItems(rangerPolicy.getPolicyItems())
                    | removeAtlasAccessesFromPolicyItems(rangerPolicy.getAllowExceptions())
                    | removeAtlasAccessesFromPolicyItems(rangerPolicy.getDenyPolicyItems())
                    | removeAtlasAccessesFromPolicyItems(rangerPolicy.getDenyExceptions());

            if (policyUpdated) {
                svcStore.updatePolicy(rangerPolicy);
            }
        }
    }

    private boolean removeAtlasAccessesFromPolicyItems(List<RangerPolicyItem> policyItems) {
        if (CollectionUtils.isEmpty(policyItems)) {
            return false;
        }

        boolean updated = false;
        List<RangerPolicyItem> policyItemsToRemove = new ArrayList<>();

        for (RangerPolicyItem policyItem : policyItems) {
            if (policyItem == null || policyItem.getAccesses() == null) {
                continue;
            }

            List<RangerPolicyItemAccess> accessesToRemove = new ArrayList<>();

            for (RangerPolicyItemAccess access : policyItem.getAccesses()) {
                if (access != null && isAtlasTagAccessType(access.getType())) {
                    accessesToRemove.add(access);
                }
            }

            if (!accessesToRemove.isEmpty()) {
                policyItem.getAccesses().removeAll(accessesToRemove);
                updated = true;
            }

            if (CollectionUtils.isEmpty(policyItem.getAccesses())) {
                policyItemsToRemove.add(policyItem);
            }
        }

        if (!policyItemsToRemove.isEmpty()) {
            policyItems.removeAll(policyItemsToRemove);
            updated = true;
        }

        return updated;
    }

    private void removeAtlasAccessTypesFromTagServiceDef() throws Exception {
        XXServiceDef xxTagServiceDef = daoMgr.getXXServiceDef().findByName(TAG_SERVICE_DEF_NAME);

        if (xxTagServiceDef == null) {
            LOG.info("Tag service-def not found in DB. No Atlas access type cleanup is needed");

            return;
        }

        RangerServiceDef dbTagServiceDef = svcStore.getServiceDefByName(TAG_SERVICE_DEF_NAME);

        if (dbTagServiceDef != null && CollectionUtils.isNotEmpty(dbTagServiceDef.getAccessTypes())) {
            List<RangerServiceDef.RangerAccessTypeDef> accessTypesToRemove = new ArrayList<>();

            for (RangerServiceDef.RangerAccessTypeDef accessTypeDef : dbTagServiceDef.getAccessTypes()) {
                if (accessTypeDef != null && isAtlasTagAccessType(accessTypeDef.getName())) {
                    LOG.info("Found Atlas access type to remove from tag service-def: {}", accessTypeDef.getName());
                    accessTypesToRemove.add(accessTypeDef);
                }
            }

            if (!accessTypesToRemove.isEmpty()) {
                dbTagServiceDef.getAccessTypes().removeAll(accessTypesToRemove);
                svcStore.updateServiceDef(dbTagServiceDef);
            }
        }

        List<XXAccessTypeDef> xxAccessTypes = daoMgr.getXXAccessTypeDef().findByServiceDefId(xxTagServiceDef.getId());

        for (XXAccessTypeDef xxAccessType : xxAccessTypes) {
            if (xxAccessType != null && isAtlasTagAccessType(xxAccessType.getName())) {
                svcStore.deleteXXAccessTypeDef(xxAccessType);
            }
        }
    }

    private boolean isAtlasTagAccessType(String accessTypeName) {
        if (accessTypeName == null) {
            return false;
        }

        String[] svcDefAccType = accessTypeName.split(":");
        String   serviceDefName = svcDefAccType.length > 0 ? svcDefAccType[0] : null;

        return ATLAS_SERVICE_DEF_NAME.equals(serviceDefName);
    }
}
