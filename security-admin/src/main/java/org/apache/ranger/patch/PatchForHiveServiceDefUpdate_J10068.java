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
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerRowFilterDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class PatchForHiveServiceDefUpdate_J10068 extends BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(PatchForHiveServiceDefUpdate_J10068.class);

    private static final String HIVE_SERVICE_DEF_NAME            = "hive";
    private static final String ROW_FILTER_TABLE_RESOURCE_NAME   = "table";

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    ServiceDBStore svcStore;

    @Autowired
    JSONUtil jsonUtil;

    @Autowired
    RangerValidatorFactory validatorFactory;

    public static void main(String[] args) {
        logger.info("main()");

        try {
            PatchForHiveServiceDefUpdate_J10068 loader = (PatchForHiveServiceDefUpdate_J10068) CLIUtil.getBean(PatchForHiveServiceDefUpdate_J10068.class);

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
        logger.info("PatchForHiveServiceDefUpdate_J10068");
    }

    @Override
    public void execLoad() {
        logger.info("==> PatchForHiveServiceDefUpdate_J10068.execLoad()");

        try {
            updateHiveServiceDef();
        } catch (Exception e) {
            logger.error("Failed to apply PatchForHiveServiceDefUpdate_J10068.", e);

            System.exit(1);
        }

        logger.info("<== PatchForHiveServiceDefUpdate_J10068.execLoad()");
    }

    private void updateHiveServiceDef() throws Exception {
        RangerServiceDef embeddedHiveServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(HIVE_SERVICE_DEF_NAME);

        if (embeddedHiveServiceDef == null) {
            throw new IllegalStateException("The embedded Hive service-definition does not exist.");
        }

        RangerRowFilterDef embeddedRowFilterDef = embeddedHiveServiceDef.getRowFilterDef();

        if (embeddedRowFilterDef == null || CollectionUtils.isEmpty(embeddedRowFilterDef.getResources())) {
            throw new IllegalStateException("Embedded " + HIVE_SERVICE_DEF_NAME + " service-def has no rowFilterDef.");
        }

        XXServiceDef xXServiceDefObj = daoMgr.getXXServiceDef().findByName(HIVE_SERVICE_DEF_NAME);

        if (xXServiceDefObj == null) {
            throw new IllegalStateException(HIVE_SERVICE_DEF_NAME + " service-def not found in DB.");
        }

        Map<String, String> serviceDefOptionsPreUpdate = jsonStringToMap(xXServiceDefObj.getDefOptions());
        String              valueBeforeUpdate          = serviceDefOptionsPreUpdate == null ? null
                : serviceDefOptionsPreUpdate.get(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES);

        RangerServiceDef dbHiveServiceDef = svcStore.getServiceDefByName(HIVE_SERVICE_DEF_NAME);

        if (dbHiveServiceDef == null) {
            throw new IllegalStateException(HIVE_SERVICE_DEF_NAME + " service-def not found in DB.");
        }

        updateRowFilterTableResource(embeddedRowFilterDef, dbHiveServiceDef.getRowFilterDef());

        RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);

        validator.validate(dbHiveServiceDef, Action.UPDATE);

        RangerServiceDef ret = svcStore.updateServiceDef(dbHiveServiceDef);

        if (ret == null) {
            throw new IllegalStateException("Error while updating " + HIVE_SERVICE_DEF_NAME + " service-def");
        }

        restoreDefOptionsIfNeeded(valueBeforeUpdate);

        logger.info("Successfully updated rowFilterDef table resource for {} service-def", HIVE_SERVICE_DEF_NAME);
    }

    private void updateRowFilterTableResource(RangerRowFilterDef embeddedRowFilterDef, RangerRowFilterDef dbRowFilterDef) {
        if (dbRowFilterDef == null || CollectionUtils.isEmpty(dbRowFilterDef.getResources())) {
            throw new IllegalStateException("DB " + HIVE_SERVICE_DEF_NAME + " service-def has no rowFilterDef.");
        }

        RangerResourceDef embeddedTableResource = findRowFilterResource(embeddedRowFilterDef, ROW_FILTER_TABLE_RESOURCE_NAME);
        RangerResourceDef dbTableResource       = findRowFilterResource(dbRowFilterDef, ROW_FILTER_TABLE_RESOURCE_NAME);

        if (embeddedTableResource == null) {
            throw new IllegalStateException("Embedded " + HIVE_SERVICE_DEF_NAME + " rowFilterDef has no table resource.");
        }

        if (dbTableResource == null) {
            throw new IllegalStateException("DB " + HIVE_SERVICE_DEF_NAME + " rowFilterDef has no table resource.");
        }

        Map<String, String> matcherOptions = embeddedTableResource.getMatcherOptions();

        dbTableResource.setMatcherOptions(matcherOptions != null ? new HashMap<>(matcherOptions) : null);
        dbTableResource.setUiHint(embeddedTableResource.getUiHint());
    }

    private RangerResourceDef findRowFilterResource(RangerRowFilterDef rowFilterDef, String resourceName) {
        RangerResourceDef result = null;

        if (rowFilterDef != null && CollectionUtils.isNotEmpty(rowFilterDef.getResources())) {
            for (RangerResourceDef resourceDef : rowFilterDef.getResources()) {
                if (resourceName.equals(resourceDef.getName())) {
                    result = resourceDef;
                    break;
                }
            }
        }

        return result;
    }

    private void restoreDefOptionsIfNeeded(String valueBeforeUpdate) throws Exception {
        XXServiceDef xXServiceDefObj = daoMgr.getXXServiceDef().findByName(HIVE_SERVICE_DEF_NAME);

        if (xXServiceDefObj == null) {
            return;
        }

        Map<String, String> serviceDefOptionsPostUpdate = jsonStringToMap(xXServiceDefObj.getDefOptions());

        if (serviceDefOptionsPostUpdate == null) {
            return;
        }

        String valueAfterUpdate = serviceDefOptionsPostUpdate.get(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES);

        if (!StringUtils.equals(valueBeforeUpdate, valueAfterUpdate)) {
            if (StringUtils.isEmpty(valueBeforeUpdate)) {
                serviceDefOptionsPostUpdate.remove(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES);
            } else {
                serviceDefOptionsPostUpdate.put(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES, valueBeforeUpdate);
            }

            xXServiceDefObj.setDefOptions(mapToJsonString(serviceDefOptionsPostUpdate));

            daoMgr.getXXServiceDef().update(xXServiceDefObj);
        }
    }

    protected Map<String, String> jsonStringToMap(String jsonStr) {
        Map<String, String> ret = null;

        if (!StringUtils.isEmpty(jsonStr)) {
            try {
                ret = jsonUtil.jsonToMap(jsonStr);
            } catch (Exception excp) {
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

    private String mapToJsonString(Map<String, String> map) throws Exception {
        String ret = null;

        if (map != null) {
            ret = jsonUtil.readMapToString(map);
        }

        return ret;
    }
}
