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

import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Component
public class PatchForAtlasPolicyUpdateForEntityRead_J10064 extends org.apache.ranger.patch.BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(PatchForAtlasPolicyUpdateForEntityRead_J10064.class);

    private static final String RESOURCE_ENTITY_TYPE              = "entity-type";
    private static final String ENTITY_READ                       = "entity-read";
    private static final String RESOURCE_ENTITY_LABEL             = "entity-label";
    private static final String RESOURCE_ENTITY_BUSINESS_METADATA = "entity-business-metadata";

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
            updateAtlasPolicyForEntityReadAccessType();
        } catch (Exception e) {
            throw new RuntimeException("Error while updating " + EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME + " service-def", e);
        }

        logger.info("<== PatchForAtlasPolicyUpdateForEntityRead_J10064.execLoad()");
    }

    private boolean removeIfEntityReadPermissionExist(RangerPolicyItem item) {
        return item.getAccesses().removeIf(itemAccess -> itemAccess.getType().equals(ENTITY_READ));
    }

    private void updateAtlasPolicyForEntityReadAccessType() throws Exception {
        logger.info("==> updateAtlasPolicyForEntityReadAccessType() ");

        XXServiceDef xXServiceDefObj = daoMgr.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);

        if (xXServiceDefObj == null) {
            logger.debug("ServiceDef not found with name :{}", EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);

            return;
        }

        Long            xServiceDefId = xXServiceDefObj.getId();
        List<XXService> xxServices    = daoMgr.getXXService().findByServiceDefId(xServiceDefId);

        for (XXService xxService : xxServices) {
            List<XXPolicy> xxPolicies = daoMgr.getXXPolicy().findByServiceId(xxService.getId());

            for (XXPolicy xxPolicy : xxPolicies) {
                RangerPolicy rPolicy = svcDBStore.getPolicy(xxPolicy.getId());

                final Map<String, RangerPolicyResource> policyResources = rPolicy.getResources();
                final boolean isNonEntityResourceType = policyResources.containsKey(RESOURCE_ENTITY_LABEL)
                        || policyResources.containsKey(RESOURCE_ENTITY_BUSINESS_METADATA);
                boolean isUpdated = false;

                if (policyResources.containsKey(RESOURCE_ENTITY_TYPE) && isNonEntityResourceType) {
                    List<RangerPolicyItem> policyItems = rPolicy.getPolicyItems();
                    if (policyItems != null) {
                        Iterator<RangerPolicyItem> itemIterator = policyItems.iterator();
                        while (itemIterator.hasNext()) {
                            RangerPolicyItem item = itemIterator.next();
                            if (removeIfEntityReadPermissionExist(item)) {
                                if (item.getAccesses().isEmpty()) {
                                    itemIterator.remove();
                                    logger.debug("Removing empty policy item from policy ID: {}", rPolicy.getId());
                                }
                                isUpdated = true;
                            }
                        }
                    }
                }

                if (isUpdated) {
                    svcDBStore.updatePolicy(rPolicy);
                    logger.info("PatchForAtlasPolicyUpdateForEntityRead_J10064: updated policy (id={}, name={}) to remove {} permission",
                            rPolicy.getId(), rPolicy.getName(), ENTITY_READ);
                }
            }
        }

        logger.info("<== updateAtlasPolicyForEntityReadAccessType() ");
    }
}
