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
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class PatchForOzoneServiceDefAssumeRoleUpdate_J10065 extends BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(PatchForOzoneServiceDefAssumeRoleUpdate_J10065.class);

    private static final String       OZONE_RESOURCE_ROLE                             = "role";
    private static final String       ACCESS_TYPE_ASSUME_ROLE                         = "assume_role";
    private static final List<String> OZONE_RESOURCES_WITH_ACCESS_TYPE_RESTRICTIONS = Arrays.asList("volume", "bucket", "key");

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    ServiceDBStore svcDBStore;

    @Autowired
    JSONUtil jsonUtil;

    @Autowired
    RangerValidatorFactory validatorFactory;

    public static void main(String[] args) {
        logger.info("main()");

        try {
            PatchForOzoneServiceDefAssumeRoleUpdate_J10065 loader = (PatchForOzoneServiceDefAssumeRoleUpdate_J10065) CLIUtil.getBean(PatchForOzoneServiceDefAssumeRoleUpdate_J10065.class);

            loader.init();

            while (loader.isMoreToProcess()) {
                loader.load();
            }

            logger.info("Load complete. Exiting.");

            System.exit(0);
        } catch (Exception e) {
            logger.error("Error loading", e);

            System.exit(1);
        }
    }

    @Override
    public void init() throws Exception {
        // Do Nothing
    }

    @Override
    public void printStats() {
        logger.info("PatchForOzoneServiceDefAssumeRoleUpdate_J10065 data ");
    }

    @Override
    public void execLoad() {
        try {
            if (!updateOzoneServiceDef()) {
                logger.error("Failed to apply the patch.");

                System.exit(1);
            }
        } catch (Exception e) {
            logger.error("Error while updateOzoneServiceDef()data.", e);

            System.exit(1);
        }
    }

    protected Map<String, String> jsonStringToMap(String jsonStr) {
        Map<String, String> ret = null;

        if (!StringUtils.isEmpty(jsonStr)) {
            try {
                ret = jsonUtil.jsonToMap(jsonStr);
            } catch (Exception ex) {
                // fallback to earlier format: "name1=value1;name2=value2"
                for (String optionString : jsonStr.split(";")) {
                    if (StringUtils.isEmpty(optionString)) {
                        continue;
                    }

                    String[] nvArr = optionString.split("=");
                    String   name  = (nvArr != null && nvArr.length > 0) ? nvArr[0].trim() : null;
                    String   value = (nvArr != null && nvArr.length > 1) ? nvArr[1].trim() : null;

                    if (StringUtils.isEmpty(name)) {
                        continue;
                    }

                    if (ret == null) {
                        ret = new HashMap<>();
                    }

                    ret.put(name, value);
                }
            }
        }

        return ret;
    }

    private boolean updateOzoneServiceDef() throws Exception {
        logger.info("==> PatchForOzoneServiceDefAssumeRoleUpdate_J10065.updateOzoneServiceDef()");

        RangerServiceDef embeddedOzoneServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);

        if (embeddedOzoneServiceDef == null) {
            logger.error("The embedded Ozone service-definition does not exist.");

            return false;
        }

        XXServiceDef xXServiceDefObj = daoMgr.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);

        if (xXServiceDefObj == null) {
            logger.error("Ozone service-definition does not exist in the Ranger DAO.");

            return false;
        }

        Map<String, String> serviceDefOptionsPreUpdate = jsonStringToMap(xXServiceDefObj.getDefOptions());

        RangerServiceDef dbOzoneServiceDef = svcDBStore.getServiceDefByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);

        if (dbOzoneServiceDef == null) {
            logger.error("Ozone service-definition does not exist in the db store.");

            return false;
        }

        List<RangerServiceDef.RangerPolicyConditionDef> policyConditionsSnapshot = copyPolicyConditions(dbOzoneServiceDef.getPolicyConditions());

        detachServiceDefCollections(dbOzoneServiceDef);

        if (!requiresServiceDefUpdate(dbOzoneServiceDef)) {
            logger.info("Ozone service-definition already has role resource, assume_role access type and resource access-type restrictions. No patching is needed.");

            return true;
        }

        applyServiceDefUpdates(dbOzoneServiceDef, embeddedOzoneServiceDef);

        dbOzoneServiceDef.setPolicyConditions(copyPolicyConditions(policyConditionsSnapshot));

        RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcDBStore);

        validator.validate(dbOzoneServiceDef, RangerValidator.Action.UPDATE);

        RangerServiceDef ret = svcDBStore.updateServiceDef(dbOzoneServiceDef);

        if (ret == null) {
            throw new RuntimeException("Error while updating " + EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME + " service-def");
        }

        xXServiceDefObj = daoMgr.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);

        if (xXServiceDefObj == null) {
            logger.error("Ozone service-definition does not exist in the Ranger DAO.");

            return false;
        }

        String              jsonStrPostUpdate           = xXServiceDefObj.getDefOptions();
        Map<String, String> serviceDefOptionsPostUpdate = jsonStringToMap(jsonStrPostUpdate);

        if (serviceDefOptionsPostUpdate != null && serviceDefOptionsPostUpdate.containsKey(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES)) {
            if (serviceDefOptionsPreUpdate == null || !serviceDefOptionsPreUpdate.containsKey(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES)) {
                String preUpdateValue = serviceDefOptionsPreUpdate == null ? null : serviceDefOptionsPreUpdate.get(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES);

                if (preUpdateValue == null) {
                    serviceDefOptionsPostUpdate.remove(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES);
                } else {
                    serviceDefOptionsPostUpdate.put(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES, preUpdateValue);
                }

                xXServiceDefObj.setDefOptions(mapToJsonString(serviceDefOptionsPostUpdate));

                daoMgr.getXXServiceDef().update(xXServiceDefObj);
            }
        }

        logger.info("<== PatchForOzoneServiceDefAssumeRoleUpdate_J10065.updateOzoneServiceDef()");

        return true;
    }

    private void applyServiceDefUpdates(RangerServiceDef dbOzoneServiceDef, RangerServiceDef embeddedOzoneServiceDef) {
        List<RangerServiceDef.RangerResourceDef> dbResources = dbOzoneServiceDef.getResources();

        if (dbResources == null) {
            dbResources = new ArrayList<>();
            dbOzoneServiceDef.setResources(dbResources);
        }

        if (!isRoleResourcePresent(dbResources)) {
            RangerServiceDef.RangerResourceDef embeddedRoleResource = findResourceDef(embeddedOzoneServiceDef.getResources(), OZONE_RESOURCE_ROLE);

            if (embeddedRoleResource != null) {
                dbResources.add(new RangerServiceDef.RangerResourceDef(embeddedRoleResource));

                logger.info("Added role resource to Ozone service-definition.");
            }
        }

        for (String resourceName : OZONE_RESOURCES_WITH_ACCESS_TYPE_RESTRICTIONS) {
            RangerServiceDef.RangerResourceDef dbResource       = findResourceDef(dbResources, resourceName);
            RangerServiceDef.RangerResourceDef embeddedResource = findResourceDef(embeddedOzoneServiceDef.getResources(), resourceName);

            if (dbResource != null && embeddedResource != null && CollectionUtils.isEmpty(dbResource.getAccessTypeRestrictions())) {
                dbResource.setAccessTypeRestrictions(embeddedResource.getAccessTypeRestrictions());

                logger.info("Updated access-type restrictions on {} resource in Ozone service-definition.", resourceName);
            }
        }

        if (!isAssumeRoleAccessTypePresent(dbOzoneServiceDef.getAccessTypes())) {
            RangerServiceDef.RangerAccessTypeDef embeddedAssumeRoleAccessType = findAccessTypeDef(embeddedOzoneServiceDef.getAccessTypes(), ACCESS_TYPE_ASSUME_ROLE);

            if (embeddedAssumeRoleAccessType != null) {
                List<RangerServiceDef.RangerAccessTypeDef> updatedAccessTypes = new ArrayList<>();

                if (CollectionUtils.isNotEmpty(dbOzoneServiceDef.getAccessTypes())) {
                    for (RangerServiceDef.RangerAccessTypeDef accessType : dbOzoneServiceDef.getAccessTypes()) {
                        updatedAccessTypes.add(new RangerServiceDef.RangerAccessTypeDef(accessType));
                    }
                }

                updatedAccessTypes.add(new RangerServiceDef.RangerAccessTypeDef(embeddedAssumeRoleAccessType));

                dbOzoneServiceDef.setAccessTypes(updatedAccessTypes);

                logger.info("Added assume_role access type to Ozone service-definition.");
            }
        }
    }

    private void detachServiceDefCollections(RangerServiceDef serviceDef) {
        List<RangerServiceDef.RangerResourceDef> resources = serviceDef.getResources();

        if (CollectionUtils.isNotEmpty(resources)) {
            List<RangerServiceDef.RangerResourceDef> copiedResources = new ArrayList<>(resources.size());

            for (RangerServiceDef.RangerResourceDef resource : resources) {
                copiedResources.add(new RangerServiceDef.RangerResourceDef(resource));
            }

            serviceDef.setResources(copiedResources);
        }

        List<RangerServiceDef.RangerAccessTypeDef> accessTypes = serviceDef.getAccessTypes();

        if (CollectionUtils.isNotEmpty(accessTypes)) {
            List<RangerServiceDef.RangerAccessTypeDef> copiedAccessTypes = new ArrayList<>(accessTypes.size());

            for (RangerServiceDef.RangerAccessTypeDef accessType : accessTypes) {
                copiedAccessTypes.add(new RangerServiceDef.RangerAccessTypeDef(accessType));
            }

            serviceDef.setAccessTypes(copiedAccessTypes);
        }
    }

    private List<RangerServiceDef.RangerPolicyConditionDef> copyPolicyConditions(List<RangerServiceDef.RangerPolicyConditionDef> policyConditions) {
        List<RangerServiceDef.RangerPolicyConditionDef> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(policyConditions)) {
            for (RangerServiceDef.RangerPolicyConditionDef policyCondition : policyConditions) {
                Map<String, String> evaluatorOptions = policyCondition.getEvaluatorOptions();

                ret.add(new RangerServiceDef.RangerPolicyConditionDef(
                        policyCondition.getItemId(),
                        policyCondition.getName(),
                        policyCondition.getEvaluator(),
                        evaluatorOptions == null ? null : new HashMap<>(evaluatorOptions),
                        policyCondition.getValidationRegEx(),
                        policyCondition.getValidationMessage(),
                        policyCondition.getUiHint(),
                        policyCondition.getLabel(),
                        policyCondition.getDescription(),
                        policyCondition.getRbKeyLabel(),
                        policyCondition.getRbKeyDescription(),
                        policyCondition.getRbKeyValidationMessage()));
            }
        }

        return ret;
    }

    private RangerServiceDef.RangerResourceDef findResourceDef(List<RangerServiceDef.RangerResourceDef> resourceDefs, String resourceName) {
        if (CollectionUtils.isEmpty(resourceDefs)) {
            return null;
        }

        for (RangerServiceDef.RangerResourceDef resourceDef : resourceDefs) {
            if (resourceName.equals(resourceDef.getName())) {
                return resourceDef;
            }
        }

        return null;
    }

    private RangerServiceDef.RangerAccessTypeDef findAccessTypeDef(List<RangerServiceDef.RangerAccessTypeDef> accessTypeDefs, String accessTypeName) {
        if (CollectionUtils.isEmpty(accessTypeDefs)) {
            return null;
        }

        for (RangerServiceDef.RangerAccessTypeDef accessTypeDef : accessTypeDefs) {
            if (accessTypeName.equals(accessTypeDef.getName())) {
                return accessTypeDef;
            }
        }

        return null;
    }

    private boolean requiresServiceDefUpdate(RangerServiceDef dbOzoneServiceDef) {
        return !isRoleResourcePresent(dbOzoneServiceDef.getResources())
                || !isAssumeRoleAccessTypePresent(dbOzoneServiceDef.getAccessTypes())
                || !areAccessTypeRestrictionsPresent(dbOzoneServiceDef.getResources());
    }

    private boolean isRoleResourcePresent(List<RangerServiceDef.RangerResourceDef> resourceDefs) {
        if (CollectionUtils.isEmpty(resourceDefs)) {
            return false;
        }

        for (RangerServiceDef.RangerResourceDef resourceDef : resourceDefs) {
            if (OZONE_RESOURCE_ROLE.equals(resourceDef.getName())) {
                return true;
            }
        }

        return false;
    }

    private boolean isAssumeRoleAccessTypePresent(List<RangerServiceDef.RangerAccessTypeDef> accessTypeDefs) {
        if (CollectionUtils.isEmpty(accessTypeDefs)) {
            return false;
        }

        for (RangerServiceDef.RangerAccessTypeDef accessTypeDef : accessTypeDefs) {
            if (ACCESS_TYPE_ASSUME_ROLE.equals(accessTypeDef.getName())) {
                return true;
            }
        }

        return false;
    }

    private boolean areAccessTypeRestrictionsPresent(List<RangerServiceDef.RangerResourceDef> resourceDefs) {
        if (CollectionUtils.isEmpty(resourceDefs)) {
            return false;
        }

        for (RangerServiceDef.RangerResourceDef resourceDef : resourceDefs) {
            if (OZONE_RESOURCES_WITH_ACCESS_TYPE_RESTRICTIONS.contains(resourceDef.getName())
                    && CollectionUtils.isEmpty(resourceDef.getAccessTypeRestrictions())) {
                return false;
            }
        }

        return true;
    }

    private String mapToJsonString(Map<String, String> map) {
        String ret = null;

        if (map != null) {
            try {
                ret = jsonUtil.readMapToString(map);
            } catch (Exception ex) {
                logger.warn("mapToJsonString() failed to convert map: {}", map, ex);
            }
        }

        return ret;
    }
}
