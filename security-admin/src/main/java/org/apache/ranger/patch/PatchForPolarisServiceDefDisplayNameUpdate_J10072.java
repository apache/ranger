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
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PatchForPolarisServiceDefDisplayNameUpdate_J10072 extends BaseLoader {
    private static final Logger LOG = LoggerFactory.getLogger(PatchForPolarisServiceDefDisplayNameUpdate_J10072.class);

    private static final String POLARIS_SERVICE_DEF_NAME   = EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_POLARIS_NAME;
    private static final String POLARIS_DRAFT_DISPLAY_NAME = "Polaris (draft)";

    @Autowired
    ServiceDBStore svcStore;

    @Autowired
    RangerValidatorFactory validatorFactory;

    public static void main(String[] args) {
        LOG.info("main()");

        try {
            PatchForPolarisServiceDefDisplayNameUpdate_J10072 loader = (PatchForPolarisServiceDefDisplayNameUpdate_J10072) CLIUtil.getBean(PatchForPolarisServiceDefDisplayNameUpdate_J10072.class);

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
        LOG.info("PatchForPolarisServiceDefDisplayNameUpdate data");
    }

    @Override
    public void execLoad() {
        LOG.info("==> PatchForPolarisServiceDefDisplayNameUpdate_J10072.execLoad()");

        try {
            updatePolarisServiceDefDisplayName();
        } catch (Exception e) {
            LOG.error("Error while running PatchForPolarisServiceDefDisplayNameUpdate_J10072", e);

            System.exit(1);
        }

        LOG.info("<== PatchForPolarisServiceDefDisplayNameUpdate_J10072.execLoad()");
    }

    private void updatePolarisServiceDefDisplayName() throws Exception {
        RangerServiceDef embeddedPolarisServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(POLARIS_SERVICE_DEF_NAME);

        if (embeddedPolarisServiceDef == null) {
            LOG.error("Embedded Polaris service-definition not found");

            throw new RuntimeException("Embedded Polaris service-definition not found");
        }

        RangerServiceDef dbPolarisServiceDef = svcStore.getServiceDefByName(POLARIS_SERVICE_DEF_NAME);

        if (dbPolarisServiceDef == null) {
            LOG.info("Polaris service-def not found in DB. No patching is needed");
        } else {
            String  currentDisplayName     = dbPolarisServiceDef.getDisplayName();
            String  embeddedDisplayName    = embeddedPolarisServiceDef.getDisplayName();
            boolean displayNameNeedsUpdate = StringUtils.equals(currentDisplayName, POLARIS_DRAFT_DISPLAY_NAME)
                    && StringUtils.isNotBlank(embeddedDisplayName)
                    && !StringUtils.equals(currentDisplayName, embeddedDisplayName);

            if (displayNameNeedsUpdate) {
                LOG.info("Updating Polaris service-def displayName from {} to {}", currentDisplayName, embeddedDisplayName);

                dbPolarisServiceDef.setDisplayName(embeddedDisplayName);

                RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);

                validator.validate(dbPolarisServiceDef, Action.UPDATE);

                RangerServiceDef updated = svcStore.updateServiceDef(dbPolarisServiceDef);

                if (updated == null) {
                    throw new RuntimeException("Error while updating " + POLARIS_SERVICE_DEF_NAME + " service-def");
                }

                LOG.info("Updated Polaris service-def displayName to {}", embeddedDisplayName);
            } else {
                LOG.info("Polaris service-def displayName is already {}. No patching is needed", currentDisplayName);
            }
        }
    }
}
