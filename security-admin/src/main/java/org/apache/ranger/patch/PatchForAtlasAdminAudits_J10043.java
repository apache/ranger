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
public class PatchForAtlasAdminAudits_J10043 extends BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(PatchForAtlasAdminAudits_J10043.class);

    private static final List<String> ATLAS_RESOURCES    = new ArrayList<>(Collections.singletonList("atlas-service"));
    private static final List<String> ATLAS_ACCESS_TYPES = new ArrayList<>(Collections.singletonList("admin-audits"));

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
            PatchForAtlasAdminAudits_J10043 loader = (PatchForAtlasAdminAudits_J10043) CLIUtil.getBean(PatchForAtlasAdminAudits_J10043.class);

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
        logger.info("PatchForAtlasAdminAudits_J10043 Logs");
    }

    @Override
    public void execLoad() {
        logger.info("==> PatchForAtlasAdminAudits_J10043.execLoad()");

        try {
            addAdminAuditsPermissionInServiceDef();
        } catch (Exception e) {
            throw new RuntimeException("Error while updating " + EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME + " service-def");
        }

        logger.info("<== PatchForAtlasAdminAudits_J10043.execLoad()");
    }

    private void addAdminAuditsPermissionInServiceDef() throws Exception {
        RangerServiceDef embeddedAtlasServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);

        if (embeddedAtlasServiceDef != null) {
            XXServiceDef xXServiceDefObj = daoMgr.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);

            if (xXServiceDefObj == null) {
                logger.info("{}: service-def not found. No patching is needed", xXServiceDefObj);

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
