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
import org.apache.log4j.Logger;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.util.CLIUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class PatchForTrinoSvcDefUpdate_J10062 extends BaseLoader {
    public static final String LOGIN_ID_ADMIN = "admin";
    public static final String WILDCARD_ASTERISK = "*";
    public static final String POlICY_NAME_FOR_ALL_SYSINFO = "all - sysinfo";
    public static final String POlICY_NAME_FOR_ALL_CATALOG_SCHEMA_FUNCTION = "all - catalog, schema, schemafunction";
    public static final String POlICY_NAME_FOR_ALL_QUERY = "all - queryid";
    public static final String POlICY_NAME_FOR_ALL_ROLE = "all - role";
    public static final String RESOURCE_SYSINFO = "sysinfo";
    public static final String RESOURCE_CATALOG = "catalog";
    public static final String RESOURCE_SCHEMA = "schema";
    public static final String RESOURCE_QUERYID = "queryid";
    public static final String RESOURCE_ROLE = "role";
    public static final String RESOURCE_SCHEMAFUNCTION = "schemafunction";
    public static final String ACCESS_TYPE_READ_SYSINFO = "read_sysinfo";
    public static final String ACCESS_TYPE_WRITE_SYSINFO = "write_sysinfo";
    public static final String ACCESS_TYPE_CREATE = "create";
    public static final String ACCESS_TYPE_DROP = "drop";
    public static final String ACCESS_TYPE_SHOW = "show";
    public static final String ACCESS_TYPE_GRANT = "grant";
    public static final String ACCESS_TYPE_REVOKE = "revoke";
    public static final String ACCESS_TYPE_EXECUTE = "execute";
    public static final String ACCESS_TYPE_SELECT = "select";
    private static final Logger logger = Logger.getLogger(PatchForTrinoSvcDefUpdate_J10062.class);
    private static final String TRINO_SVC_DEF_NAME = EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TRINO_NAME;
    @Autowired
    GUIDUtil guidUtil;
    @Autowired
    ServiceDBStore svcDBStore;
    @Autowired
    @Qualifier(value = "transactionManager")
    PlatformTransactionManager txManager;
    @Autowired
    private RangerDaoManager daoMgr;
    @Autowired
    private RangerValidatorFactory validatorFactory;

    public static void main(String[] args) {
        logger.info("main()");
        try {
            PatchForTrinoSvcDefUpdate_J10062 loader = (PatchForTrinoSvcDefUpdate_J10062) CLIUtil.getBean(PatchForTrinoSvcDefUpdate_J10062.class);
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
        // DO NOTHING
    }

    @Override
    public void printStats() {
        logger.info("PatchForTrinoSvcDefUpdate_J10062 logs ");
    }

    @Override
    public void execLoad() {
        logger.info("==> PatchForTrinoSvcDefUpdate_J10062.execLoad()");
        try {
            TransactionTemplate txTemplate = new TransactionTemplate(txManager);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
            try {
                txTemplate.execute(new TransactionCallback<Object>() {
                    @Override
                    public Object doInTransaction(TransactionStatus status) {
                        RangerServiceDef dbRangerServiceDef = null;
                        RangerServiceDef embeddedTrinoServiceDef = null;
                        try {
                            embeddedTrinoServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(TRINO_SVC_DEF_NAME);
                        } catch (Exception ex) {
                            logger.error("Error while loading service-def: " + TRINO_SVC_DEF_NAME, ex);
                        }
                        if (embeddedTrinoServiceDef == null) {
                            logger.error("The embedded Trino service-definition does not exist.");
                            throw new RuntimeException("Error while updating " + TRINO_SVC_DEF_NAME + " service-def");
                        }
                        if (embeddedTrinoServiceDef != null) {
                            try {
                                dbRangerServiceDef = svcDBStore.getServiceDefByName(TRINO_SVC_DEF_NAME);
                            } catch (Exception e) {
                                logger.error("The Trino service-definition does not exist in ranger db.");
                            } finally {
                                if (dbRangerServiceDef == null) {
                                    logger.error("The Trino service-definition does not exist.");
                                    throw new RuntimeException("Error while updating " + TRINO_SVC_DEF_NAME + " service-def");
                                }
                            }
                        }
                        dbRangerServiceDef = updateTrinoSvcDef(embeddedTrinoServiceDef, dbRangerServiceDef);
                        if (dbRangerServiceDef != null) {
                            try {
                                createDefaultPolicies(dbRangerServiceDef);
                            } catch (Exception e) {
                                logger.error("Error while creating default ranger policies for " + TRINO_SVC_DEF_NAME + " service-def");
                                throw new RuntimeException("Error while creating default ranger policies for " + TRINO_SVC_DEF_NAME + " service-def");
                            }
                        } else {
                            logger.error("Error while updating " + TRINO_SVC_DEF_NAME + " service-def");
                            throw new RuntimeException("Error while updating " + TRINO_SVC_DEF_NAME + " service-def");
                        }
                        return null;
                    }
                });
            } catch (Throwable ex) {
                logger.error("Error while updating " + TRINO_SVC_DEF_NAME + " service-def");
                throw new RuntimeException(ex.getMessage());
            }
        } catch (Exception e) {
            logger.error("Error while executing PatchForTrinoSvcDefUpdate_J10062, Error - ", e);
            throw new RuntimeException(e.getMessage());
        }
        logger.info("<== PatchForTrinoSvcDefUpdate_J10062.execLoad()");
    }

    private RangerServiceDef updateTrinoSvcDef(RangerServiceDef embeddedTrinoServiceDef, RangerServiceDef dbRangerServiceDef) {
        logger.info("==> PatchForTrinoSvcDefUpdate_J10062.updateTrinoSvcDef()");
        RangerServiceDef ret = null;
        try {
            dbRangerServiceDef.setResources(embeddedTrinoServiceDef.getResources());
            dbRangerServiceDef.setAccessTypes(embeddedTrinoServiceDef.getAccessTypes());
            dbRangerServiceDef.setConfigs(embeddedTrinoServiceDef.getConfigs());
            RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(this.svcDBStore);
            validator.validate(dbRangerServiceDef, Action.UPDATE);
            ret = this.svcDBStore.updateServiceDef(dbRangerServiceDef);
            logger.info(TRINO_SVC_DEF_NAME + " service-def has been updated");
        } catch (Exception e) {
            logger.error("Error while updating" + TRINO_SVC_DEF_NAME + " service-def", e);
            throw new RuntimeException(e);
        }
        logger.info("<== PatchForTrinoSvcDefUpdate_J10062.updateTrinoSvcDef()");
        return ret;
    }

    private void createDefaultPolicies(RangerServiceDef dbRangerServiceDef) throws Exception {
        List<XXService> dbServices = daoMgr.getXXService().findByServiceDefId(dbRangerServiceDef.getId());
        if (CollectionUtils.isNotEmpty(dbServices)) {
            for (XXService dbService : dbServices) {
                addDefaultPolicies(dbService.getName(), null);
            }
        }
    }

    private void addDefaultPolicies(String serviceName, String zoneName) throws Exception {
        logger.info("===> addDefaultPolicies ServiceName : " + serviceName + " ZoneName : " + zoneName);
        List<String> resources = new ArrayList<>();
        resources.add(RESOURCE_SYSINFO);
        RangerPolicy allSysInfoPolicy = getPolicy(serviceName, zoneName, POlICY_NAME_FOR_ALL_SYSINFO, resources);
        List<RangerPolicy> policies = svcDBStore.getPoliciesByResourceSignature(serviceName, allSysInfoPolicy.getResourceSignature(), true);
        if (CollectionUtils.isEmpty(policies)) {
            logger.info("No policy found with resource sysinfo = * creating new policy");
            svcDBStore.createPolicy(allSysInfoPolicy);
        }

        resources.clear();
        policies.clear();
        resources.add(RESOURCE_CATALOG);
        resources.add(RESOURCE_SCHEMA);
        resources.add(RESOURCE_SCHEMAFUNCTION);
        RangerPolicy allCatalogSchemaFunctionPolicy = getPolicy(serviceName, zoneName, POlICY_NAME_FOR_ALL_CATALOG_SCHEMA_FUNCTION, resources);
        policies = svcDBStore.getPoliciesByResourceSignature(serviceName, allCatalogSchemaFunctionPolicy.getResourceSignature(), true);
        if (CollectionUtils.isEmpty(policies)) {
            logger.info("No policy found with resource catalog, schema, schemafunction = *; creating new policy");
            svcDBStore.createPolicy(allCatalogSchemaFunctionPolicy);
        }

        resources.clear();
        policies.clear();
        resources.add(RESOURCE_QUERYID);
        RangerPolicy allQueryIdPolicy = getPolicy(serviceName, zoneName, POlICY_NAME_FOR_ALL_QUERY, resources);
        policies = svcDBStore.getPoliciesByResourceSignature(serviceName, allQueryIdPolicy.getResourceSignature(), true);
        if (CollectionUtils.isEmpty(policies)) {
            logger.info("No policy found with resource queryId = *; creating new policy");
            svcDBStore.createPolicy(allQueryIdPolicy);
        }

        resources.clear();
        policies.clear();
        resources.add(RESOURCE_ROLE);
        RangerPolicy allRolePolicy = getPolicy(serviceName, zoneName, POlICY_NAME_FOR_ALL_ROLE, resources);
        policies = svcDBStore.getPoliciesByResourceSignature(serviceName, allRolePolicy.getResourceSignature(), true);
        if (CollectionUtils.isEmpty(policies)) {
            logger.info("No policy found with resource role = *; creating new policy");
            svcDBStore.createPolicy(allRolePolicy);
        }
        logger.info("<=== addDefaultPolicies");
    }

    private RangerPolicy getPolicy(String serviceName, String zoneName, String policyName, List<String> resources) {
        logger.info("===> getPolicy ");
        RangerPolicy policy;
        Map<String, RangerPolicy.RangerPolicyResource> policyResources = new HashMap<>();
        for (String resource : resources) {
            policyResources.put(resource, new RangerPolicy.RangerPolicyResource(WILDCARD_ASTERISK));
        }
        policy = new RangerPolicy();
        policy.setService(serviceName);
        policy.setName(policyName);
        policy.setDescription("Policy for " + policyName);
        policy.setIsAuditEnabled(true);
        policy.setCreatedBy(LOGIN_ID_ADMIN);
        policy.setResources(policyResources);
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        policy.setGuid(guidUtil.genGUID());
        policy.setZoneName(zoneName);
        List<RangerPolicy.RangerPolicyItem> policyItems = new ArrayList<>();
        policyItems.add(getPolicyItem(policyName, "trino", true));
        policyItems.add(getPolicyItem("select", "rangerlookup", false));
        policy.setPolicyItems(policyItems);
        policy.setResourceSignature(new RangerPolicyResourceSignature(policy).getSignature());
        if (logger.isDebugEnabled()) {
            logger.debug("===> getPolicy policy ResourceSignature  " + policy.getResourceSignature());
            logger.debug("===> getPolicy policy : " + policy);
        }
        logger.info("<=== getPolicy ");
        return policy;
    }

    private RangerPolicy.RangerPolicyItem getPolicyItem(String policyName, String user, boolean delegateAdmin) {
        RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
        policyItem.setUsers(new ArrayList<String>() {{
            add(user);
        }});
        policyItem.setAccesses(getAccessList(policyName));
        policyItem.setDelegateAdmin(delegateAdmin);
        return policyItem;
    }

    private List<RangerPolicy.RangerPolicyItemAccess> getAccessList(String policyName) {
        List<RangerPolicy.RangerPolicyItemAccess> accessList = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();
        switch (policyName) {
            case POlICY_NAME_FOR_ALL_SYSINFO:
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_READ_SYSINFO));
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_WRITE_SYSINFO));
                break;
            case POlICY_NAME_FOR_ALL_CATALOG_SCHEMA_FUNCTION:
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_CREATE));
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_DROP));
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_SHOW));
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_GRANT));
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_EXECUTE));
                break;
            case POlICY_NAME_FOR_ALL_ROLE:
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_CREATE));
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_DROP));
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_SHOW));
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_GRANT));
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_REVOKE));
                break;
            case POlICY_NAME_FOR_ALL_QUERY:
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_EXECUTE));
                break;
            default:
                accessList.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_SELECT));
                break;
        }
        return accessList;
    }

}