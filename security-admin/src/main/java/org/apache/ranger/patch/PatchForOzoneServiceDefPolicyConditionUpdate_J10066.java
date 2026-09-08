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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXServiceDef;
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
import java.util.List;
import java.util.Map;

@Component
public class PatchForOzoneServiceDefPolicyConditionUpdate_J10066 extends BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(PatchForOzoneServiceDefPolicyConditionUpdate_J10066.class);

    private static final String POLICY_CONDITION_ACTION_MATCHES = "action-matches";

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
            PatchForOzoneServiceDefPolicyConditionUpdate_J10066 loader = (PatchForOzoneServiceDefPolicyConditionUpdate_J10066) CLIUtil.getBean(PatchForOzoneServiceDefPolicyConditionUpdate_J10066.class);
            loader.init();
            while (loader.isMoreToProcess()) {
                loader.load();
            }
            logger.info("Load complete. Exiting!!!");
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
        logger.info("PatchForOzoneServiceDefPolicyConditionUpdate_J10066");
    }

    @Override
    public void execLoad() {
        logger.info("==> PatchForOzoneServiceDefPolicyConditionUpdate_J10066.execLoad()");
        try {
            updateOzoneServiceDef();
        } catch (Exception e) {
            logger.error("Error while applying PatchForOzoneServiceDefPolicyConditionUpdate_J10066", e);
            throw new RuntimeException("PatchForOzoneServiceDefPolicyConditionUpdate_J10066 failed", e);
        }
        logger.info("<== PatchForOzoneServiceDefPolicyConditionUpdate_J10066.execLoad()");
    }

    private void updateOzoneServiceDef() {
        try {
            final String ozoneServiceDefName = EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME;
            final RangerServiceDef embeddedOzoneServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(ozoneServiceDefName);

            if (embeddedOzoneServiceDef == null) {
                logger.error("Embedded service-def for {} not found", ozoneServiceDefName);
                return;
            }

            final List<RangerServiceDef.RangerPolicyConditionDef> embeddedPolicyConditions = embeddedOzoneServiceDef.getPolicyConditions();

            if (embeddedPolicyConditions == null) {
                logger.error("Policy conditions are empty in embedded {} service-def", ozoneServiceDefName);
                return;
            }

            XXServiceDef xXServiceDefObj = daoMgr.getXXServiceDef().findByName(ozoneServiceDefName);

            if (xXServiceDefObj == null) {
                logger.error("Service def for {} is not found in DB", ozoneServiceDefName);
                return;
            }

            XXPolicyConditionDef existingActionMatches = daoMgr.getXXPolicyConditionDef().findByServiceDefIdAndName(
                    xXServiceDefObj.getId(), POLICY_CONDITION_ACTION_MATCHES);

            if (existingActionMatches != null) {
                logger.info("Ozone service-definition already has {} in DB. No patching needed.", POLICY_CONDITION_ACTION_MATCHES);
                return;
            }

            RangerServiceDef.RangerPolicyConditionDef embeddedActionMatchesCondition = null;

            for (RangerServiceDef.RangerPolicyConditionDef policyCondition : embeddedPolicyConditions) {
                if (POLICY_CONDITION_ACTION_MATCHES.equals(policyCondition.getName())) {
                    embeddedActionMatchesCondition = policyCondition;
                    break;
                }
            }

            if (embeddedActionMatchesCondition == null) {
                logger.error("Policy condition {} is missing from embedded Ozone service-definition.", POLICY_CONDITION_ACTION_MATCHES);
                return;
            }

            Map<String, String> serviceDefOptionsPreUpdate = null;
            final String        jsonStrPreUpdate           = xXServiceDefObj.getDefOptions();

            if (StringUtils.isNotEmpty(jsonStrPreUpdate)) {
                serviceDefOptionsPreUpdate = jsonUtil.jsonToMap(jsonStrPreUpdate);
            }

            final RangerServiceDef dbOzoneServiceDef = svcDBStore.getServiceDefByName(ozoneServiceDefName);

            if (dbOzoneServiceDef == null) {
                logger.error("Service def for {} is not found in ServiceDBStore", ozoneServiceDefName);
                return;
            }

            List<RangerServiceDef.RangerPolicyConditionDef> updatedPolicyConditions = dbOzoneServiceDef.getPolicyConditions() == null
                    ? new ArrayList<>()
                    : new ArrayList<>(dbOzoneServiceDef.getPolicyConditions());

            updatedPolicyConditions.removeIf(c -> POLICY_CONDITION_ACTION_MATCHES.equals(c.getName()));

            RangerServiceDef.RangerPolicyConditionDef actionMatchesCondition = new RangerServiceDef.RangerPolicyConditionDef(
                    embeddedActionMatchesCondition.getItemId(),
                    embeddedActionMatchesCondition.getName(),
                    embeddedActionMatchesCondition.getEvaluator(),
                    embeddedActionMatchesCondition.getEvaluatorOptions(),
                    embeddedActionMatchesCondition.getValidationRegEx(),
                    embeddedActionMatchesCondition.getValidationMessage(),
                    embeddedActionMatchesCondition.getUiHint(),
                    embeddedActionMatchesCondition.getLabel(),
                    embeddedActionMatchesCondition.getDescription(),
                    embeddedActionMatchesCondition.getRbKeyLabel(),
                    embeddedActionMatchesCondition.getRbKeyDescription(),
                    embeddedActionMatchesCondition.getRbKeyValidationMessage());

            actionMatchesCondition.setItemId(nextPolicyConditionItemId(updatedPolicyConditions));

            updatedPolicyConditions.add(actionMatchesCondition);

            dbOzoneServiceDef.setPolicyConditions(updatedPolicyConditions);

            final RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcDBStore);

            validator.validate(dbOzoneServiceDef, Action.UPDATE);

            svcDBStore.updateServiceDef(dbOzoneServiceDef);

            xXServiceDefObj = daoMgr.getXXServiceDef().findByName(ozoneServiceDefName);

            if (xXServiceDefObj != null) {
                final String              jsonStrPostUpdate           = xXServiceDefObj.getDefOptions();
                Map<String, String>       serviceDefOptionsPostUpdate = null;

                if (StringUtils.isNotEmpty(jsonStrPostUpdate)) {
                    serviceDefOptionsPostUpdate = jsonUtil.jsonToMap(jsonStrPostUpdate);
                }

                if (serviceDefOptionsPostUpdate != null && serviceDefOptionsPostUpdate.containsKey(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES)) {
                    if (serviceDefOptionsPreUpdate == null || !serviceDefOptionsPreUpdate.containsKey(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES)) {
                        serviceDefOptionsPostUpdate.remove(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES);

                        xXServiceDefObj.setDefOptions(jsonUtil.readMapToString(serviceDefOptionsPostUpdate));

                        daoMgr.getXXServiceDef().update(xXServiceDefObj);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error while updating ozone service-def policy conditions", e);
            throw new RuntimeException("Failed to update ozone service-def policy conditions", e);
        }
    }

    private long nextPolicyConditionItemId(List<RangerServiceDef.RangerPolicyConditionDef> policyConditions) {
        long maxItemId = 0L;

        if (policyConditions != null) {
            for (RangerServiceDef.RangerPolicyConditionDef policyCondition : policyConditions) {
                Long itemId = policyCondition.getItemId();

                if (itemId != null && itemId > maxItemId) {
                    maxItemId = itemId;
                }
            }
        }

        return maxItemId + 1;
    }
}
