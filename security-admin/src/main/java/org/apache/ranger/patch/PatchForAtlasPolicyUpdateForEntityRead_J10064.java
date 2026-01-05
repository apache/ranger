/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.rest.ServiceRESTUtil;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class PatchForAtlasPolicyUpdateForEntityRead_J10064 extends org.apache.ranger.patch.BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(PatchForAtlasPolicyUpdateForEntityRead_J10064.class);

    private static final String ENTITY_READ                       = "entity-read";

    private static final String RESOURCE_ENTITY_TYPE              = "entity-type";
    private static final String RESOURCE_ENTITY_LABEL             = "entity-label";
    private static final String RESOURCE_ENTITY_BUSINESS_METADATA = "entity-business-metadata";

    private static final String POLICY_NAME_ENTITY_BASE           = "all - entity-type, entity-classification, entity";
    private static final String POLICY_NAME_ENTITY_CLASSIFICATION = "all - entity-type, entity-classification, entity, classification";

    private static final List<String> ATLAS_RESOURCE_ENTITY = new ArrayList<>(Arrays.asList("entity-type", "entity-classification", "entity"));
    private static final List<String> CLASSIFICATION_ACCESS_TYPES    = new ArrayList<>(Arrays.asList("entity-remove-classification", "entity-add-classification", "entity-update-classification"));

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    ServiceDBStore svcDBStore;

    public static void main(String[] args) {
        logger.info("main()");

        try {
            PatchForAtlasPolicyUpdateForEntityRead_J10064 loader = (PatchForAtlasPolicyUpdateForEntityRead_J10064) CLIUtil.getBean(PatchForAtlasPolicyUpdateForEntityRead_J10064.class);

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
        logger.info("PatchForAtlasPolicyUpdateForEntityRead_J10064 Logs");
    }

    @Override
    public void execLoad() {
        logger.info("==> PatchForAtlasPolicyUpdateForEntityRead_J10064.execLoad()");

        try {
            updateAtlasPolicyForAccessType();
        } catch (Exception e) {
            throw new RuntimeException("Error while updating " + EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME + " service-def", e);
        }

        logger.info("<== PatchForAtlasPolicyUpdateForEntityRead_J10064.execLoad()");
    }

    private void updateAtlasPolicyForAccessType() throws Exception {
        logger.info("==> updateAtlasPolicyForAccessType()");

        XXServiceDef xXServiceDefObj = daoMgr.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);

        if (xXServiceDefObj == null) {
            logger.debug("ServiceDef not found with name: {}", EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);
            return;
        }

        Long            xServiceDefId = xXServiceDefObj.getId();
        List<XXService> xxServices    = daoMgr.getXXService().findByServiceDefId(xServiceDefId);

        for (XXService xxService : xxServices) {
            List<XXPolicy> xxPolicies = daoMgr.getXXPolicy().findByServiceId(xxService.getId());

            List<RangerPolicyItem> defaultEntityReadItems = new ArrayList<>();
            List<RangerPolicyItem> defaultClassificationItems = new ArrayList<>();

            for (XXPolicy xxPolicy : xxPolicies) {
                RangerPolicy rPolicy = svcDBStore.getPolicy(xxPolicy.getId());

                boolean isUpdated = processPolicyForAccessUpdate(rPolicy, defaultEntityReadItems, defaultClassificationItems);

                if (isUpdated) {
                    svcDBStore.updatePolicy(rPolicy);
                    logger.info("PatchForAtlasPolicyUpdateForEntityRead_J10064: updated policy (id={}, name={}) for service {} to remove/filter permissions",
                            rPolicy.getId(), rPolicy.getName(), xxService.getName());
                }
            }

            applyAggregatedDefaultPolicyUpdate(POLICY_NAME_ENTITY_BASE, defaultEntityReadItems, xxService);
            applyAggregatedDefaultPolicyUpdate(POLICY_NAME_ENTITY_CLASSIFICATION, defaultClassificationItems, xxService);
        }

        logger.info("<== updateAtlasPolicyForAccessType()");
    }

    private boolean processPolicyForAccessUpdate(RangerPolicy rPolicy, List<RangerPolicyItem> entityReadDefaults, List<RangerPolicyItem> classificationDefaults) {
        final Map<String, RangerPolicyResource> policyResources = rPolicy.getResources();

        if (policyResources == null) {
            return false;
        }

        final boolean isNonEntityResourceType = policyResources.containsKey(RESOURCE_ENTITY_TYPE) &&
                (policyResources.containsKey(RESOURCE_ENTITY_LABEL) || policyResources.containsKey(RESOURCE_ENTITY_BUSINESS_METADATA));

        if (isNonEntityResourceType) {
            return removeEntityReadAccess(rPolicy, entityReadDefaults);
        }

        if (isEntityResource(policyResources)) {
            return filterClassificationAccess(rPolicy, classificationDefaults);
        }

        return false;
    }

    private boolean isEntityResource(Map<String, RangerPolicyResource> policyResources) {
        if (policyResources == null || policyResources.size() != ATLAS_RESOURCE_ENTITY.size()) {
            return false;
        }

        return policyResources.keySet().containsAll(ATLAS_RESOURCE_ENTITY);
    }

    private boolean removeEntityReadAccess(RangerPolicy rPolicy, List<RangerPolicyItem> defaultEntityReadItems) {
        List<RangerPolicyItem> policyItems = rPolicy.getPolicyItems();
        if (CollectionUtils.isEmpty(policyItems)) {
            return false;
        }

        boolean isUpdated = false;
        Iterator<RangerPolicyItem> itemIterator = policyItems.iterator();
        while (itemIterator.hasNext()) {
            RangerPolicyItem item = itemIterator.next();

            boolean wasAccessRemoved = item.getAccesses().removeIf(itemAccess -> ENTITY_READ.equals(itemAccess.getType()));

            if (wasAccessRemoved) {
                isUpdated = true;

                RangerPolicyItem policyItemForentityRead = new RangerPolicyItem();
                policyItemForentityRead.setUsers(item.getUsers());
                policyItemForentityRead.setGroups(item.getGroups());
                policyItemForentityRead.setRoles(item.getRoles());
                policyItemForentityRead.setAccesses(Collections.singletonList(new RangerPolicyItemAccess(ENTITY_READ)));
                policyItemForentityRead.setDelegateAdmin(item.getDelegateAdmin());

                if (!defaultEntityReadItems.contains(policyItemForentityRead)) {
                    defaultEntityReadItems.add(policyItemForentityRead);
                }

                if (item.getAccesses().isEmpty()) {
                    itemIterator.remove();
                    logger.debug("Removing empty policy item from policy ID: {}", rPolicy.getId());
                }
            }
        }
        return isUpdated;
    }

    private boolean filterClassificationAccess(RangerPolicy policy, List<RangerPolicyItem> defaultClassificationItems) {
        List<RangerPolicyItem> policyItems = policy.getPolicyItems();
        if (CollectionUtils.isEmpty(policyItems)) {
            return false;
        }

        boolean isUpdated = false;
        ListIterator<RangerPolicyItem> policyItemIterator = policyItems.listIterator();

        while (policyItemIterator.hasNext()) {
            RangerPolicyItem policyItem = policyItemIterator.next();
            List<RangerPolicyItemAccess> accesses = policyItem.getAccesses();

            if (CollectionUtils.isNotEmpty(accesses)) {
                List<RangerPolicyItemAccess> accessesToRemove = accesses.stream()
                        .filter(access -> CLASSIFICATION_ACCESS_TYPES.contains(access.getType())).collect(Collectors.toList());

                boolean removed = accesses.removeAll(accessesToRemove);

                if (removed) {
                    isUpdated = true;

                    RangerPolicyItem policyItemForClassification = new RangerPolicyItem();
                    policyItemForClassification.setUsers(policyItem.getUsers());
                    policyItemForClassification.setGroups(policyItem.getGroups());
                    policyItemForClassification.setRoles(policyItem.getRoles());
                    policyItemForClassification.setAccesses(accessesToRemove);
                    policyItemForClassification.setDelegateAdmin(policyItem.getDelegateAdmin());

                    if (!defaultClassificationItems.contains(policyItemForClassification)) {
                        defaultClassificationItems.add(policyItemForClassification);
                    }
                }

                // Remove the policy item if all accesses were filtered out
                if (accesses.isEmpty()) {
                    policyItemIterator.remove();
                    logger.debug("Removing empty policy item from policy ID: {}", policy.getId());
                }
            }
        }

        return isUpdated;
    }

    private void applyAggregatedDefaultPolicyUpdate(String policyName, List<RangerPolicyItem> itemsToAdd, XXService service) {
        if (CollectionUtils.isEmpty(itemsToAdd)) {
            return;
        }

        try {
            XXPolicy xxPolicy = daoMgr.getXXPolicy().findByNameAndServiceId(policyName, service.getId());

            if (xxPolicy == null) {
                return;
            }

            RangerPolicy existingPolicy = svcDBStore.getPolicy(xxPolicy.getId());

            RangerPolicy appliedPolicy = new RangerPolicy();
            appliedPolicy.setPolicyItems(itemsToAdd);

            // Merge itemsToAdd into the existingPolicy
            ServiceRESTUtil.mergeExactMatchPolicyForResource(existingPolicy, appliedPolicy);

            svcDBStore.updatePolicy(existingPolicy);

            logger.info("Successfully Added {} policy-items to default policy (id={}, name={}) for service {}",
                    itemsToAdd.size(), existingPolicy.getId(), existingPolicy.getName(), service.getName());
        } catch (Exception e) {
            logger.error("Error updating default policy '{}' for service {}", policyName, service.getName(), e);
        }
    }
}
