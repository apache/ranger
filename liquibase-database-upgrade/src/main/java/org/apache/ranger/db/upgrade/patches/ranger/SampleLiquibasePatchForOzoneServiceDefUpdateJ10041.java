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

package org.apache.ranger.db.upgrade.patches.ranger;

import liquibase.change.custom.CustomTaskChange;
import liquibase.change.custom.CustomTaskRollback;
import liquibase.database.Database;
import liquibase.exception.CustomChangeException;
import liquibase.exception.RollbackImpossibleException;
import liquibase.exception.SetupException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.upgrade.SpringContext;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.SearchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Sample for demonstration of how java patch PatchForOzoneServiceDefUpdate_J10041 can be ported into liquibase.
// Instead of @Autowired, SpringContext.getBean(...) is used to initialize the spring components required in this class
// Since @Transactional annotation cannot be used in this class, TransactionTemplate is used to start a transaction which is required to write into the database
// rollback() method is only partially implemented here for demonstration purposes only and does not fully rollback changes in the execute() method
@SuppressWarnings("PMD.JUnit4TestShouldUseBeforeAnnotation")
public class SampleLiquibasePatchForOzoneServiceDefUpdateJ10041
        implements CustomTaskChange, CustomTaskRollback {
    private static final Logger       logger                = LoggerFactory.getLogger(
            SampleLiquibasePatchForOzoneServiceDefUpdateJ10041.class);
    private static final List<String> OZONE_CONFIGS         = new ArrayList<>(
            Arrays.asList("dfs.datanode.kerberos.principal", "dfs.namenode.kerberos.principal", "dfs.secondary.namenode.kerberos.principal", "commonNameForCertificate"));
    private static final String       OZONE_RESOURCE_VOLUME = "volume";
    private static final String       OZONE_RESOURCE_KEY    = "key";
    private static final String       ACCESS_TYPE_READ_ACL  = "read_acl";
    private static final String       ACCESS_TYPE_WRITE_ACL = "write_acl";

    //@Autowired
    RangerDaoManager           daoMgr           = SpringContext.getBean(RangerDaoManager.class);
    ServiceDBStore             svcDBStore       = SpringContext.getBean(ServiceDBStore.class);
    JSONUtil                   jsonUtil         = SpringContext.getBean(JSONUtil.class);
    PlatformTransactionManager txManager        = SpringContext.getBean("transactionManager");
    RangerValidatorFactory     validatorFactory = SpringContext.getBean(RangerValidatorFactory.class);

    @Override
    public void execute(Database database) throws CustomChangeException {
        boolean isServiceDefUpdateSuccessful;
        try {
            TransactionTemplate txTemplate = new TransactionTemplate(txManager);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            isServiceDefUpdateSuccessful = Boolean.TRUE.equals(txTemplate.execute(status -> {
                try {
                    return updateOzoneServiceDef();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        } catch (Exception e) {
            throw new CustomChangeException(e);
        }
        if (!isServiceDefUpdateSuccessful) {
            throw new CustomChangeException("Updating ozone service def was unsuccessful");
        }
    }

    @Override
    public String getConfirmationMessage() {
        return null;
    }

    @Override
    public void setUp() throws SetupException {
    }

    @Override
    public void setFileOpener(ResourceAccessor resourceAccessor) {
    }

    @Override
    public ValidationErrors validate(Database database) {
        return null;
    }

    @Override
    public void rollback(Database database)
            throws CustomChangeException, RollbackImpossibleException {
        boolean isServiceDefUpdateSuccessful;
        try {
            TransactionTemplate txTemplate = new TransactionTemplate(txManager);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
            isServiceDefUpdateSuccessful = Boolean.TRUE.equals(txTemplate.execute(status -> {
                try {
                    return rollbackOzoneServiceDef();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        } catch (Exception e) {
            throw new CustomChangeException(e);
        }
        if (!isServiceDefUpdateSuccessful) {
            throw new CustomChangeException("Rollback service def was unsuccessful");
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
                        ret = new HashMap<String, String>();
                    }
                    ret.put(name, value);
                }
            }
        }
        return ret;
    }

    private boolean rollbackOzoneServiceDef() throws Exception {
        RangerServiceDef                           dbOzoneServiceDef        = svcDBStore.getServiceDefByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);
        RangerServiceDef                           embeddedOzoneServiceDef  = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);
        List<RangerServiceDef.RangerAccessTypeDef> embeddedOzoneAccessTypes = embeddedOzoneServiceDef.getAccessTypes();
        logger.info("Before deletion embeddedOzoneAccessTypes=" + embeddedOzoneAccessTypes);
        logger.info("Before deletion dbOzoneServiceDef.accessTypes=" + dbOzoneServiceDef.getAccessTypes().toString());
        embeddedOzoneAccessTypes.removeIf(item -> item.getItemId() == 101L);
        logger.info("After deletion embeddedOzoneAccessTypes=" + embeddedOzoneAccessTypes);
        boolean ret = false;
        if (!embeddedOzoneAccessTypes.toString().equalsIgnoreCase(dbOzoneServiceDef.getAccessTypes().toString())) {
            logger.info("Trying to write to DB new access types after deleting access type added suring update");
            dbOzoneServiceDef.setAccessTypes(embeddedOzoneAccessTypes);
            ret = true;
        } else {
            logger.info("Deletion of access type with id 101 not possible as not present. Rollback failed");
        }
        return ret;
    }

    private boolean updateOzoneServiceDef() throws Exception {
        RangerServiceDef                              ret;
        RangerServiceDef                              embeddedOzoneServiceDef;
        RangerServiceDef                              dbOzoneServiceDef;
        List<RangerServiceDef.RangerServiceConfigDef> embeddedOzoneConfigDefs;
        List<RangerServiceDef.RangerResourceDef>      embeddedOzoneResourceDefs;
        List<RangerServiceDef.RangerAccessTypeDef>    embeddedOzoneAccessTypes;
        XXServiceDef                                  xXServiceDefObj;

        embeddedOzoneServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);
        logger.info("EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME" + EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);
        if (embeddedOzoneServiceDef != null) {
            if (daoMgr == null) {
                logger.error("daoMgr is null");
            } else if (daoMgr.getXXServiceDef() == null) {
                logger.error("daoMgr.getXXServiceDef() is null");
            } else if (daoMgr.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME) == null) {
                logger.error("daoMgr.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME) is null");
            } else {
                logger.info("Nothing was null, all good");
            }
            xXServiceDefObj = daoMgr.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);
            Map<String, String> serviceDefOptionsPreUpdate;
            String              jsonPreUpdate;

            if (xXServiceDefObj != null) {
                jsonPreUpdate              = xXServiceDefObj.getDefOptions();
                serviceDefOptionsPreUpdate = jsonStringToMap(jsonPreUpdate);
            } else {
                logger.error("Ozone service-definition does not exist in the Ranger DAO. No patching is needed!!");
                return true;
            }
            dbOzoneServiceDef = svcDBStore.getServiceDefByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);

            if (dbOzoneServiceDef != null) {
                // Remove old Ozone configs
                embeddedOzoneConfigDefs = embeddedOzoneServiceDef.getConfigs();
                if (checkNotConfigPresent(embeddedOzoneConfigDefs)) {
                    dbOzoneServiceDef.setConfigs(embeddedOzoneConfigDefs);
                }

                // Update volume resource with recursive flag false and key resource with recursive flag true
                embeddedOzoneResourceDefs = embeddedOzoneServiceDef.getResources();
                if (checkVolKeyResUpdate(embeddedOzoneResourceDefs)) {
                    dbOzoneServiceDef.setResources(embeddedOzoneResourceDefs);
                }

                // Add new access types
                embeddedOzoneAccessTypes = embeddedOzoneServiceDef.getAccessTypes();
                embeddedOzoneAccessTypes.add(new RangerServiceDef.RangerAccessTypeDef(101L, "liquibase_java_patch", "liquibase_java_patch", "liquibase_java_patch", new ArrayList<>()));

                if (embeddedOzoneAccessTypes != null) {
                    if (checkAccessTypesPresent(embeddedOzoneAccessTypes)) {
                        if (!embeddedOzoneAccessTypes.toString().equalsIgnoreCase(dbOzoneServiceDef.getAccessTypes().toString())) {
                            dbOzoneServiceDef.setAccessTypes(embeddedOzoneAccessTypes);
                        }
                    }
                }
            } else {
                logger.error("Ozone service-definition does not exist in the db store.");
                return false;
            }
            RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcDBStore);
            validator.validate(dbOzoneServiceDef, RangerValidator.Action.UPDATE);

            ret = svcDBStore.updateServiceDef(dbOzoneServiceDef);
            if (ret == null) {
                throw new RuntimeException("Error while updating " + EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME + " service-def");
            }
            xXServiceDefObj = daoMgr.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);
            if (xXServiceDefObj != null) {
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
            } else {
                logger.error("Ozone service-definition does not exist in the Ranger DAO.");
                return false;
            }
            List<XXService> dbServices = daoMgr.getXXService().findByServiceDefId(embeddedOzoneServiceDef.getId());
            if (CollectionUtils.isNotEmpty(dbServices)) {
                for (XXService dbService : dbServices) {
                    SearchFilter filter = new SearchFilter();
                    filter.setParam(SearchFilter.SERVICE_NAME, dbService.getName());
                    updateExisitngOzonePolicies(svcDBStore.getServicePolicies(dbService.getId(), filter));
                }
            }
        } else {
            logger.error("The embedded Ozone service-definition does not exist.");
            return false;
        }
        return true;
    }

    private boolean checkNotConfigPresent(List<RangerServiceDef.RangerServiceConfigDef> configDefs) {
        boolean      ret         = false;
        List<String> configNames = new ArrayList<>();
        for (RangerServiceDef.RangerServiceConfigDef configDef : configDefs) {
            configNames.add(configDef.getName());
        }
        for (String delConfig : OZONE_CONFIGS) {
            if (!configNames.contains(delConfig)) {
                ret = true;
                break;
            }
        }
        return ret;
    }

    private boolean checkVolKeyResUpdate(List<RangerServiceDef.RangerResourceDef> embeddedOzoneResDefs) {
        boolean ret = false;
        for (RangerServiceDef.RangerResourceDef resDef : embeddedOzoneResDefs) {
            if ((resDef.getName().equals(OZONE_RESOURCE_VOLUME) && (!resDef.getRecursiveSupported() || resDef.getExcludesSupported())) ||
                    (resDef.getName().equals(OZONE_RESOURCE_KEY) && resDef.getRecursiveSupported())) {
                ret = true;
                break;
            }
        }
        return ret;
    }

    private boolean checkAccessTypesPresent(List<RangerServiceDef.RangerAccessTypeDef> embeddedOzoneAccessTypes) {
        boolean ret = false;
        for (RangerServiceDef.RangerAccessTypeDef accessDef : embeddedOzoneAccessTypes) {
            if (ACCESS_TYPE_READ_ACL.equals(accessDef.getName()) || ACCESS_TYPE_WRITE_ACL.equals(accessDef.getName())) {
                ret = true;
                break;
            }
        }
        return ret;
    }

    private void updateExisitngOzonePolicies(List<RangerPolicy> policies) throws Exception {
        if (CollectionUtils.isNotEmpty(policies)) {
            for (RangerPolicy policy : policies) {
                List<RangerPolicy.RangerPolicyItem> policyItems = policy.getPolicyItems();
                if (CollectionUtils.isNotEmpty(policyItems)) {
                    for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
                        List<RangerPolicy.RangerPolicyItemAccess> policyItemAccesses = policyItem.getAccesses();
                        // Add new access types
                        policyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("read_acl"));
                        policyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("write_acl"));
                        policyItem.setAccesses(policyItemAccesses);
                    }
                }
                Map<String, RangerPolicy.RangerPolicyResource> policyResources = policy.getResources();
                if (MapUtils.isNotEmpty(policyResources)) {
                    if (policyResources.containsKey(OZONE_RESOURCE_VOLUME)) {
                        // Set recursive flag as false for volume resource
                        policyResources.get(OZONE_RESOURCE_VOLUME).setIsRecursive(false);
                        // Set exclude support flag as true for volume resource
                        policyResources.get(OZONE_RESOURCE_VOLUME).setIsExcludes(false);
                    }
                    if (policyResources.containsKey(OZONE_RESOURCE_KEY)) {
                        // Set is recursive flag as true for volume resource
                        policyResources.get(OZONE_RESOURCE_KEY).setIsRecursive(true);
                    }
                }
                svcDBStore.updatePolicy(policy);
            }
        }
    }

    private String mapToJsonString(Map<String, String> map) {
        String ret = null;
        if (map != null) {
            try {
                ret = jsonUtil.readMapToString(map);
            } catch (Exception ex) {
                logger.warn("mapToJsonString() failed to convert map: " + map, ex);
            }
        }
        return ret;
    }
}
