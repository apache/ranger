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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
public class PatchForAtlasResourceLabelUpdate_J10071 extends BaseLoader {
    private static final Logger LOG = LoggerFactory.getLogger(PatchForAtlasResourceLabelUpdate_J10071.class);

    private static final String ATLAS_SERVICE_DEF_NAME = EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME;

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
        } catch (Exception e) {
            LOG.error("Error while updateAtlasResourceLabels()", e);

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

        Map<String, RangerServiceDef.RangerResourceDef> embeddedResourcesByName = buildResourceMap(embeddedAtlasServiceDef.getResources());
        boolean                                           updated                 = syncResourceLabels(dbAtlasServiceDef.getResources(), embeddedResourcesByName);

        if (updated) {
            RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);

            validator.validate(dbAtlasServiceDef, Action.UPDATE);

            RangerServiceDef ret = svcStore.updateServiceDef(dbAtlasServiceDef);

            if (ret == null) {
                throw new RuntimeException("Error while updating " + ATLAS_SERVICE_DEF_NAME + " service-def");
            }

            LOG.info("Updated Atlas resource labels and descriptions from embedded service-definition");
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
}
