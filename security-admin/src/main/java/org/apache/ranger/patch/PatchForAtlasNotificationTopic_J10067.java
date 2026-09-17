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

import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class PatchForAtlasNotificationTopic_J10067 extends BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(PatchForAtlasNotificationTopic_J10067.class);

    private static final List<String> ATLAS_RESOURCES    = new ArrayList<>(Collections.singletonList("notification-topic"));
    private static final List<String> ATLAS_ACCESS_TYPES = new ArrayList<>(Collections.singletonList("post-notification"));

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    ServiceDBStore svcDBStore;

    @Autowired
    RangerValidatorFactory validatorFactory;

    @Autowired
    ServiceDBStore svcStore;

    public static void main(String[] args) {
        logger.info("main()");

        try {
            PatchForAtlasNotificationTopic_J10067 loader = (PatchForAtlasNotificationTopic_J10067) CLIUtil.getBean(PatchForAtlasNotificationTopic_J10067.class);

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
        logger.info("PatchForAtlasNotificationTopic_J10067 Logs");
    }

    @Override
    public void execLoad() {
        logger.info("==> PatchForAtlasNotificationTopic_J10067.execLoad()");

        try {
            addNotificationTopicPermissionInServiceDef();
        } catch (Exception e) {
            throw new RuntimeException("Error while updating " + EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME + " service-def");
        }

        logger.info("<== PatchForAtlasNotificationTopic_J10067.execLoad()");
    }

    private void addNotificationTopicPermissionInServiceDef() throws Exception {
        RangerServiceDef embeddedAtlasServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);

        if (embeddedAtlasServiceDef != null) {
            XXServiceDef xXServiceDefObj = daoMgr.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);

            if (xXServiceDefObj == null) {
                logger.info("{}: service-def not found. No patching is needed", EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);

                return;
            }

            RangerServiceDef dbAtlasServiceDef = svcDBStore.getServiceDefByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);

            List<RangerServiceDef.RangerResourceDef>   embeddedAtlasResourceDefs = embeddedAtlasServiceDef.getResources();
            List<RangerServiceDef.RangerAccessTypeDef> embeddedAtlasAccessTypes  = embeddedAtlasServiceDef.getAccessTypes();

            if (checkResourcePresent(embeddedAtlasResourceDefs)) {
                dbAtlasServiceDef.setResources(embeddedAtlasResourceDefs);

                if (checkAccessPresent(embeddedAtlasAccessTypes)) {
                    dbAtlasServiceDef.setAccessTypes(embeddedAtlasAccessTypes);
                }
            }

            RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);

            validator.validate(dbAtlasServiceDef, Action.UPDATE);

            RangerServiceDef ret = svcStore.updateServiceDef(dbAtlasServiceDef);

            if (ret == null) {
                logger.error("Error while updating {} service-def", EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);

                throw new RuntimeException("Error while updating " + EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME + " service-def");
            }
        }
    }

    private boolean checkResourcePresent(List<RangerServiceDef.RangerResourceDef> resourceDefs) {
        boolean ret = false;

        for (RangerServiceDef.RangerResourceDef resourceDef : resourceDefs) {
            if (ATLAS_RESOURCES.contains(resourceDef.getName())) {
                ret = true;
                break;
            }
        }

        return ret;
    }

    private boolean checkAccessPresent(List<RangerAccessTypeDef> embeddedAtlasAccessTypes) {
        boolean ret = false;

        for (RangerServiceDef.RangerAccessTypeDef accessDef : embeddedAtlasAccessTypes) {
            if (ATLAS_ACCESS_TYPES.contains(accessDef.getName())) {
                ret = true;
                break;
            }
        }

        return ret;
    }
}
