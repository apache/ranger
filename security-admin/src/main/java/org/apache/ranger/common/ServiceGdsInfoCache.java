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

package org.apache.ranger.common;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXServiceVersionInfo;
import org.apache.ranger.plugin.model.RangerGds;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.service.RangerGdsDataShareInDatasetService;
import org.apache.ranger.service.RangerGdsDataShareService;
import org.apache.ranger.service.RangerGdsDatasetInProjectService;
import org.apache.ranger.service.RangerGdsDatasetService;
import org.apache.ranger.service.RangerGdsProjectService;
import org.apache.ranger.service.RangerGdsSharedResourceService;
import org.apache.ranger.util.RangerAdminCache;
import org.apache.ranger.view.RangerGdsVList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.PostConstruct;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_GDS_NAME;

@Component
public class ServiceGdsInfoCache extends RangerAdminCache<String, ServiceGdsInfo> {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceGdsInfoCache.class);

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    RangerGdsDataShareService dataShareService;

    @Autowired
    RangerGdsSharedResourceService sharedResourceService;

    @Autowired
    RangerGdsDatasetService datasetService;

    @Autowired
    RangerGdsDataShareInDatasetService dataShareInDatasetService;

    @Autowired
    RangerGdsProjectService projectService;

    @Autowired
    RangerGdsDatasetInProjectService datasetInProjectService;

    @Autowired
    ServiceStore svcStore;

    @Autowired
    @Qualifier(value = "transactionManager")
    PlatformTransactionManager txManager;

    public ServiceGdsInfoCache() {
        super("gds-info", null);
    }

    @PostConstruct
    public void init() {
        setLoader(new ServiceGdsInfoLoader(txManager));
    }

    private class ServiceGdsInfoLoader extends RangerDBValueLoader<String, ServiceGdsInfo> {
        public ServiceGdsInfoLoader(PlatformTransactionManager txManager) {
            super(txManager);
        }

        @Override
        protected RefreshableValue<ServiceGdsInfo> dbLoad(String serviceName, RefreshableValue<ServiceGdsInfo> currentValue) throws Exception {
            XXServiceVersionInfo serviceVersionInfo = daoMgr.getXXServiceVersionInfo().findByServiceName(serviceName);

            if (serviceVersionInfo == null) {
                LOG.error("Requested Service not found. serviceName={}", serviceName);

                throw new KeyNotFoundException(serviceName + ": service not found");
            }

            ServiceGdsInfo lastKnownGdsInfo = currentValue != null ? currentValue.getValue() : null;
            Long           lastKnownVersion = lastKnownGdsInfo != null ? lastKnownGdsInfo.getGdsVersion() : null;
            Long           latestVersion    = serviceVersionInfo.getGdsVersion();

            RefreshableValue<ServiceGdsInfo> ret = null;

            if (lastKnownVersion == null || !lastKnownVersion.equals(latestVersion)) {
                ServiceGdsInfo latestGdsInfo    = retrieveServiceGdsInfo(serviceVersionInfo.getServiceId(), serviceName);
                Date           latestUpdateTime = serviceVersionInfo.getGdsUpdateTime();

                latestGdsInfo.setGdsLastUpdateTime(latestUpdateTime != null ? latestUpdateTime.getTime() : null);
                latestGdsInfo.setGdsVersion(latestVersion);

                LOG.info("Refreshed gdsVersionInfo: serviceName={}, lastKnownVersion={}, latestVersion={}", serviceName, lastKnownVersion, latestVersion);

                ret = new RefreshableValue<>(latestGdsInfo);
            } else {
                LOG.debug("No change in gdsVersionInfo: serviceName={}, lastKnownVersion={}, latestVersion={}", serviceName, lastKnownVersion, latestVersion);
            }

            return ret;
        }

        private ServiceGdsInfo retrieveServiceGdsInfo(Long serviceId, String serviceName) throws Exception {
            ServiceGdsInfo ret = new ServiceGdsInfo();

            ret.setServiceName(serviceName);
            ret.setGdsServiceDef(svcStore.getServiceDefByName(EMBEDDED_SERVICEDEF_GDS_NAME));

            SearchFilter filter = new SearchFilter(SearchFilter.SERVICE_ID, serviceId.toString());

            populateDatasets(ret, filter);
            populateProjects(ret, filter);
            populateDataShares(ret, filter);
            populateSharedResources(ret, filter);
            populateDataSharesInDataset(ret, filter);
            populateDatasetsInProject(ret, filter);

            return ret;
        }

        private void populateDatasets(ServiceGdsInfo gdsInfo, SearchFilter filter) {
            for (RangerGds.RangerDataset dataset : datasetService.searchDatasets(filter).getList()) {
                if (Boolean.FALSE.equals(dataset.getIsEnabled())) {
                    continue;
                }

                ServiceGdsInfo.DatasetInfo dsInfo = new ServiceGdsInfo.DatasetInfo();

                dsInfo.setId(dataset.getId());
                dsInfo.setName(dataset.getName());
                dsInfo.setValiditySchedule(dataset.getValiditySchedule());
                dsInfo.setPolicies(getPolicies(daoMgr.getXXGdsDatasetPolicyMap().getDatasetPolicyIds(dataset.getId())));

                gdsInfo.addDataset(dsInfo);
            }
        }

        private void populateProjects(ServiceGdsInfo gdsInfo, SearchFilter filter) {
            for (RangerGds.RangerProject project : projectService.searchProjects(filter).getList()) {
                if (Boolean.FALSE.equals(project.getIsEnabled())) {
                    continue;
                }
                ServiceGdsInfo.ProjectInfo projInfo = new ServiceGdsInfo.ProjectInfo();

                projInfo.setId(project.getId());
                projInfo.setName(project.getName());
                projInfo.setValiditySchedule(project.getValiditySchedule());
                projInfo.setPolicies(getPolicies(daoMgr.getXXGdsProjectPolicyMap().getProjectPolicyIds(project.getId())));

                gdsInfo.addProject(projInfo);
            }
        }

        private void populateDataShares(ServiceGdsInfo gdsInfo, SearchFilter filter) {
            RangerGdsVList.RangerDataShareList dataShares = dataShareService.searchDataShares(filter);

            for (RangerGds.RangerDataShare dataShare : dataShares.getList()) {
                if (Boolean.FALSE.equals(dataShare.getIsEnabled())) {
                    continue;
                }

                ServiceGdsInfo.DataShareInfo dshInfo = new ServiceGdsInfo.DataShareInfo();

                dshInfo.setId(dataShare.getId());
                dshInfo.setName(dataShare.getName());
                dshInfo.setZoneName(dataShare.getZone());
                dshInfo.setConditionExpr(dataShare.getConditionExpr());
                dshInfo.setDefaultAccessTypes(dataShare.getDefaultAccessTypes());
                dshInfo.setDefaultTagMasks(dataShare.getDefaultTagMasks());

                gdsInfo.addDataShare(dshInfo);
            }
        }

        private void populateSharedResources(ServiceGdsInfo gdsInfo, SearchFilter filter) {
            for (RangerGds.RangerSharedResource resource : sharedResourceService.searchSharedResources(filter).getList()) {
                if (Boolean.FALSE.equals(resource.getIsEnabled())) {
                    continue;
                }
                ServiceGdsInfo.SharedResourceInfo resourceInfo = new ServiceGdsInfo.SharedResourceInfo();

                resourceInfo.setId(resource.getId());
                resourceInfo.setName(resource.getName());
                resourceInfo.setDataShareId(resource.getDataShareId());
                resourceInfo.setResource(resource.getResource());
                resourceInfo.setSubResource(resource.getSubResource());
                resourceInfo.setSubResourceType(resource.getSubResourceType());
                resourceInfo.setConditionExpr(resource.getConditionExpr());
                resourceInfo.setAccessTypes(resource.getAccessTypes());
                resourceInfo.setRowFilter(resource.getRowFilter());
                resourceInfo.setSubResourceMasks(resource.getSubResourceMasks());
                resourceInfo.setProfiles(resource.getProfiles());

                gdsInfo.addResource(resourceInfo);
            }
        }

        private void populateDataSharesInDataset(ServiceGdsInfo gdsInfo, SearchFilter filter) {
            for (RangerGds.RangerDataShareInDataset dshInDs : dataShareInDatasetService.searchDataShareInDatasets(filter).getList()) {
                if (dshInDs.getStatus() != RangerGds.GdsShareStatus.ACTIVE || Boolean.FALSE.equals(dshInDs.getIsEnabled())) {
                    continue;
                }

                ServiceGdsInfo.DataShareInDatasetInfo dshInDsInfo = new ServiceGdsInfo.DataShareInDatasetInfo();

                dshInDsInfo.setDatasetId(dshInDs.getDatasetId());
                dshInDsInfo.setDataShareId(dshInDs.getDataShareId());
                dshInDsInfo.setStatus(dshInDs.getStatus());
                dshInDsInfo.setValiditySchedule(dshInDs.getValiditySchedule());
                dshInDsInfo.setProfiles(dshInDs.getProfiles());

                gdsInfo.addDataShareInDataset(dshInDsInfo);
            }
        }

        private void populateDatasetsInProject(ServiceGdsInfo gdsInfo, SearchFilter filter) {
            for (RangerGds.RangerDatasetInProject dip : datasetInProjectService.searchDatasetInProjects(filter).getList()) {
                if (dip.getStatus() != RangerGds.GdsShareStatus.ACTIVE || Boolean.FALSE.equals(dip.getIsEnabled())) {
                    continue;
                }

                ServiceGdsInfo.DatasetInProjectInfo dipInfo = new ServiceGdsInfo.DatasetInProjectInfo();

                dipInfo.setDatasetId(dip.getDatasetId());
                dipInfo.setProjectId(dip.getProjectId());
                dipInfo.setStatus(dip.getStatus());
                dipInfo.setValiditySchedule(dip.getValiditySchedule());
                dipInfo.setProfiles(dip.getProfiles());

                gdsInfo.addDatasetInProjectInfo(dipInfo);
            }
        }

        private List<RangerPolicy> getPolicies(List<Long> policyIds) {
            List<RangerPolicy> ret = new ArrayList<>();

            if (CollectionUtils.isNotEmpty(policyIds)) {
                for (Long policyId : policyIds) {
                    try {
                        RangerPolicy policy = svcStore.getPolicy(policyId);

                        if (policy != null) {
                            ret.add(policy);
                        }
                    } catch (Exception excp) {
                        LOG.error("getPolicies(): failed to get policy with id={}", policyId, excp);
                    }
                }
            }

            return ret;
        }
    }
}
