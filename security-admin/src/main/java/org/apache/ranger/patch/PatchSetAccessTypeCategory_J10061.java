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

package org.apache.ranger.patch;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef.AccessTypeCategory;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class PatchSetAccessTypeCategory_J10061 extends BaseLoader{
    private static final Logger logger = LoggerFactory.getLogger(PatchSetAccessTypeCategory_J10061.class);

    @Autowired
    ServiceDBStore svcStore;

    public static void main(String[] args) {
        logger.info("main()");

        try {
            PatchSetAccessTypeCategory_J10061 loader = (PatchSetAccessTypeCategory_J10061) CLIUtil.getBean(PatchSetAccessTypeCategory_J10061.class);

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
    public void execLoad() {
        logger.info("==> PatchSetAccessTypeCategory_J10061.execLoad()");

        try {
            updateAllServiceDef();
        } catch (Exception e) {
            logger.error("PatchSetAccessTypeCategory_J10061.execLoad(): failed", e);
        }

        logger.info("<== PatchSetAccessTypeCategory_J10061.execLoad()");
    }

    @Override
    public void printStats() {
        logger.info("PatchSetAccessTypeCategory_J10061");
    }

    private void updateAllServiceDef() throws Exception {
        logger.info("==> PatchSetAccessTypeCategory_J10061.updateAllServiceDef()");

        List<RangerServiceDef>                       serviceDefs        = svcStore.getServiceDefs(new SearchFilter());
        Map<String, Map<String, AccessTypeCategory>> embeddedCategories = new HashMap<>();

        for (RangerServiceDef serviceDef : serviceDefs) {
            if (StringUtils.equals(serviceDef.getName(), EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME)) {
                continue;
            }

            logger.info("Updating access-type categories for service-def:[" + serviceDef.getName() + "]");

            if (CollectionUtils.isEmpty(serviceDef.getAccessTypes())) {
                logger.info("No access-types found in service-def:[" + serviceDef.getName() + "]");

                continue;
            }

            RangerServiceDef embeddedServiceDef = null;

            try {
                embeddedServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(serviceDef.getName());

                if (embeddedServiceDef == null) {
                    logger.info("No embedded service-def found for:[" + serviceDef.getName() + "]. Access type category will not be updated");

                    continue;
                }
            } catch (Exception e) {
                logger.info("Failed to load embedded service-def for:[" + serviceDef.getName() + "]. Access type category will not be updated", e);

                continue;
            }

            Map<String, AccessTypeCategory> accessTypeCategories = new HashMap<>();

            for (RangerAccessTypeDef accessTypeDef : embeddedServiceDef.getAccessTypes()) {
                accessTypeCategories.put(accessTypeDef.getName(), accessTypeDef.getCategory());
            }

            embeddedCategories.put(serviceDef.getName(), accessTypeCategories);

            for (RangerAccessTypeDef accessTypeDef : serviceDef.getAccessTypes()) {
                AccessTypeCategory category = accessTypeCategories.get(accessTypeDef.getName());

                if (category == null) {
                    logger.info("Category not found for access-type:[" + accessTypeDef.getName() + "] in embedded service-def:[" + serviceDef.getName() + "]. Will not be updated");

                    continue;
                }

                accessTypeDef.setCategory(category);
            }

            svcStore.updateServiceDef(serviceDef);

            logger.info("Updated access-type categories for service-def:[" + serviceDef.getName() + "]");
        }

        RangerServiceDef tagServiceDef = svcStore.getServiceDefByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME);

        if (tagServiceDef != null && CollectionUtils.isNotEmpty(tagServiceDef.getAccessTypes())) {
            logger.info("Updating access-type categories for service-def:[" + tagServiceDef.getName() + "]");

            for (RangerAccessTypeDef accessTypeDef : tagServiceDef.getAccessTypes()) {
                String[] svcDefAccType  = accessTypeDef.getName().split(":");
                String   serviceDefName = svcDefAccType.length > 0 ? svcDefAccType[0] : null;
                String   accessTypeName = svcDefAccType.length > 1 ? svcDefAccType[1] : null;

                if (StringUtils.isBlank(serviceDefName) || StringUtils.isBlank(accessTypeName)) {
                    logger.warn("Invalid access-type:[" + accessTypeDef.getName() + "] found in tag service-def. Access type category will not be updated");

                    continue;
                }

                Map<String, AccessTypeCategory> accessTypeCategories = embeddedCategories.get(serviceDefName);

                if (accessTypeCategories == null) {
                    logger.warn("No embedded service-def found for:[" + serviceDefName + "]. Access type category will not be updated in tag service-def");

                    continue;
                }

                AccessTypeCategory category = accessTypeCategories.get(accessTypeName);

                if (category == null) {
                    logger.warn("Category not found for access-type:[" + accessTypeName + "] in embedded service-def:[" + serviceDefName + "]. Access type category will not be updated in tag service-def");

                    continue;
                }

                accessTypeDef.setCategory(category);
            }

            svcStore.updateServiceDef(tagServiceDef);

            logger.info("Updated access-type categories for service-def:[" + tagServiceDef.getName() + "]");
        }

        logger.info("<== PatchSetAccessTypeCategory_J10061.updateAllServiceDef()");
    }
}
