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

package org.apache.ranger.biz;

import org.apache.http.HttpStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.db.RangerTransactionSynchronizationAdapter;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXGdsDataShareInDatasetDao;
import org.apache.ranger.db.XXGdsDatasetDao;
import org.apache.ranger.db.XXGdsDatasetInProjectDao;
import org.apache.ranger.db.XXGdsProjectDao;
import org.apache.ranger.entity.XXGdsDataShareInDataset;
import org.apache.ranger.entity.XXGdsDataset;
import org.apache.ranger.entity.XXGdsDatasetInProject;
import org.apache.ranger.entity.XXGdsDatasetPolicyMap;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXSecurityZone;
import org.apache.ranger.entity.XXGdsProject;
import org.apache.ranger.entity.XXGdsProjectPolicyMap;
import org.apache.ranger.plugin.model.RangerGds.DatasetSummary;
import org.apache.ranger.plugin.model.RangerGds.DataShareInDatasetSummary;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerGds.GdsPermission;
import org.apache.ranger.plugin.model.RangerGds.GdsShareStatus;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShareInDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDatasetInProject;
import org.apache.ranger.plugin.model.RangerGds.RangerGdsObjectACL;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.model.RangerGds.RangerSharedResource;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPrincipal.PrincipalType;
import org.apache.ranger.plugin.store.AbstractGdsStore;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.service.RangerGdsDataShareService;
import org.apache.ranger.service.RangerGdsDataShareInDatasetService;
import org.apache.ranger.service.RangerGdsDatasetService;
import org.apache.ranger.service.RangerGdsDatasetInProjectService;
import org.apache.ranger.service.RangerGdsProjectService;
import org.apache.ranger.service.RangerGdsSharedResourceService;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.validation.RangerGdsValidator;
import org.apache.ranger.view.RangerGdsVList.RangerDataShareList;
import org.apache.ranger.view.RangerGdsVList.RangerDataShareInDatasetList;
import org.apache.ranger.view.RangerGdsVList.RangerDatasetList;
import org.apache.ranger.view.RangerGdsVList.RangerDatasetInProjectList;
import org.apache.ranger.view.RangerGdsVList.RangerProjectList;
import org.apache.ranger.view.RangerGdsVList.RangerSharedResourceList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import java.util.*;

import static org.apache.ranger.db.XXGlobalStateDao.RANGER_GLOBAL_STATE_NAME_DATASET;
import static org.apache.ranger.db.XXGlobalStateDao.RANGER_GLOBAL_STATE_NAME_DATA_SHARE;
import static org.apache.ranger.db.XXGlobalStateDao.RANGER_GLOBAL_STATE_NAME_PROJECT;
import static org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_GDS_NAME;


@Component
public class GdsDBStore extends AbstractGdsStore {
    private static final Logger LOG = LoggerFactory.getLogger(GdsDBStore.class);

    public static final String RESOURCE_NAME_DATASET_ID = "dataset-id";
    public static final String RESOURCE_NAME_PROJECT_ID = "project-id";

    public static final String NOT_AUTHORIZED_FOR_DATASET_POLICIES     = "User is not authorized to manage policies for this dataset";
    public static final String NOT_AUTHORIZED_TO_VIEW_DATASET_POLICIES = "User is not authorized to view policies for this dataset";
    public static final String NOT_AUTHORIZED_FOR_PROJECT_POLICIES     = "User is not authorized to manage policies for this dataset";
    public static final String NOT_AUTHORIZED_TO_VIEW_PROJECT_POLICIES = "User is not authorized to view policies for this dataset";

    @Autowired
    RangerGdsValidator validator;

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
    RangerTransactionSynchronizationAdapter transactionSynchronizationAdapter;

    @Autowired
    GUIDUtil guidUtil;

    @Autowired
    RangerPolicyService policyService;

    @Autowired
    RangerBizUtil bizUtil;

    @Autowired
    ServiceStore svcStore;

    @Autowired
    RESTErrorUtil restErrorUtil;

    @PostConstruct
    public void initStore() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> GdsInMemoryStore.initStore()");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== GdsInMemoryStore.initStore()");
        }
    }

    public PList<DatasetSummary> getDatasetSummary(SearchFilter filter) throws Exception {
        LOG.debug("==> getDatasetSummary({})", filter);

        PList<RangerDataset>  datasets       = getUnscrubbedDatasets(filter);
        List<DatasetSummary>  datasetSummary = toDatasetSummary(datasets.getList(), getGdsPermissionFromFilter(filter));
        PList<DatasetSummary> ret            = new PList<>(datasetSummary, datasets.getStartIndex(), datasets.getPageSize(), datasets.getTotalCount(), datasets.getResultSize(), datasets.getSortType(), datasets.getSortBy());

        ret.setQueryTimeMS(datasets.getQueryTimeMS());

        LOG.debug("<== getDatasetSummary({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public RangerDataset createDataset(RangerDataset dataset) {
        LOG.debug("==> createDataset({})", dataset);

        validator.validateCreate(dataset);

        if (StringUtils.isBlank(dataset.getGuid())) {
            dataset.setGuid(guidUtil.genGUID());
        }

        if (dataset.getAcl() == null) {
            dataset.setAcl(new RangerGdsObjectACL());
        }

        addCreatorAsAclAdmin(dataset.getAcl());

        RangerDataset ret = datasetService.create(dataset);

        datasetService.createObjectHistory(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_DATASET);

        LOG.debug("<== createDataset({}): ret={}", dataset, ret);

        return ret;
    }

    @Override
    public RangerDataset updateDataset(RangerDataset dataset) {
        LOG.debug("==> updateDataset({})", dataset);

        RangerDataset existing = null;

        try {
            existing = datasetService.read(dataset.getId());
        } catch (Exception excp) {
            // ignore
        }

        validator.validateUpdate(dataset, existing);

        RangerDataset ret = datasetService.update(dataset);

        datasetService.createObjectHistory(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_DATASET);

        LOG.debug("<== updateDataset({}): ret={}", dataset, ret);

        return ret;
    }

    @Override
    public void deleteDataset(Long datasetId, boolean forceDelete) throws Exception {
        LOG.debug("==> deleteDataset({}, {})", datasetId, forceDelete);

        RangerDataset existing = null;

        try {
            existing = datasetService.read(datasetId);
        } catch (Exception excp) {
            // ignore
        }

        validator.validateDelete(datasetId, existing);

        if (existing != null) {
            if (forceDelete) {
                removeDSHIDForDataset(datasetId);
                removeDIPForDataset(datasetId);
            }

            deleteDatasetPolicies(existing);
            datasetService.delete(existing);

            datasetService.createObjectHistory(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

            updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_DATASET);
        }

        LOG.debug("<== deleteDataset({}, {})", datasetId, forceDelete);
    }

    @Override
    public RangerDataset getDataset(Long datasetId) throws Exception {
        LOG.debug("==> getDataset({})", datasetId);

        RangerDataset ret = datasetService.read(datasetId);

        if (ret != null && !validator.hasPermission(ret.getAcl(), GdsPermission.VIEW)) {
            throw new Exception("no permission on dataset id=" + datasetId);
        }

        LOG.debug("<== getDataset({}): ret={}", datasetId, ret);

        return ret;
    }

    @Override
    public RangerDataset getDatasetByName(String name) throws Exception {
        LOG.debug("==> getDatasetByName({})", name);

        XXGdsDatasetDao datasetDao = daoMgr.getXXGdsDataset();
        XXGdsDataset    existing   = datasetDao.findByName(name);

        if (existing == null) {
            throw new Exception("no dataset with name=" + name);
        }

        RangerDataset ret = datasetService.getPopulatedViewObject(existing);

        if (ret != null && !validator.hasPermission(ret.getAcl(), GdsPermission.VIEW)) {
            throw new Exception("no permission on dataset name=" + name);
        }

        LOG.debug("<== getDatasetByName({}): ret={}", name, ret);

        return ret;
    }

    @Override
    public PList<String> getDatasetNames(SearchFilter filter) {
        LOG.debug("==> getDatasetNames({})", filter);

        PList<RangerDataset> datasets = searchDatasets(filter);
        PList<String>        ret      = new PList<>(new ArrayList<>(), datasets.getStartIndex(), datasets.getPageSize(), datasets.getTotalCount(), datasets.getResultSize(), datasets.getSortType(), datasets.getSortBy());

        ret.setQueryTimeMS(datasets.getQueryTimeMS());

        if (CollectionUtils.isNotEmpty(datasets.getList())) {
            for (RangerDataset dataset : datasets.getList()) {
                ret.getList().add(dataset.getName());
            }
        }

        LOG.debug("<== getDatasetNames({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public PList<RangerDataset> searchDatasets(SearchFilter filter) {
        LOG.debug("==> searchDatasets({})", filter);

        PList<RangerDataset> ret           = getUnscrubbedDatasets(filter);
        List<RangerDataset>  datasets      = ret.getList();
        GdsPermission        gdsPermission = getGdsPermissionFromFilter(filter);

        for (RangerDataset dataset : datasets) {
            if (gdsPermission.equals(GdsPermission.LIST)) {
                scrubDatasetForListing(dataset);
            }
        }

        LOG.debug("<== searchDatasets({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public RangerPolicy addDatasetPolicy(Long datasetId, RangerPolicy policy) throws Exception {
        LOG.debug("==> addDatasetPolicy({}, {})", datasetId, policy);

        RangerDataset dataset = datasetService.read(datasetId);

        if (!validator.hasPermission(dataset.getAcl(), GdsPermission.POLICY_ADMIN)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_FOR_DATASET_POLICIES);
        }

        prepareDatasetPolicy(dataset, policy);

        RangerPolicy ret = svcStore.createPolicy(policy);

        daoMgr.getXXGdsDatasetPolicyMap().create(new XXGdsDatasetPolicyMap(datasetId, ret.getId()));

        LOG.debug("<== addDatasetPolicy({}, {}): ret={}", datasetId, policy, ret);

        return ret;
    }

    @Override
    public RangerPolicy updateDatasetPolicy(Long datasetId, RangerPolicy policy) throws Exception {
        LOG.debug("==> updateDatasetPolicy({}, {})", datasetId, policy);

        RangerDataset dataset = datasetService.read(datasetId);

        if (!validator.hasPermission(dataset.getAcl(), GdsPermission.POLICY_ADMIN)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_FOR_DATASET_POLICIES);
        }

        XXGdsDatasetPolicyMap existing = daoMgr.getXXGdsDatasetPolicyMap().getDatasetPolicyMap(datasetId, policy.getId());

        if (existing == null) {
            throw new Exception("no policy exists: datasetId=" + datasetId + ", policyId=" + policy.getId());
        }

        prepareDatasetPolicy(dataset, policy);

        RangerPolicy ret = svcStore.updatePolicy(policy);

        LOG.debug("<== updateDatasetPolicy({}, {}): ret={}", datasetId, policy, ret);

        return ret;
    }

    @Override
    public void deleteDatasetPolicy(Long datasetId, Long policyId) throws Exception {
        LOG.debug("==> deleteDatasetPolicy({}, {})", datasetId, policyId);

        RangerDataset dataset = datasetService.read(datasetId);

        if (!validator.hasPermission(dataset.getAcl(), GdsPermission.POLICY_ADMIN)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_FOR_DATASET_POLICIES);
        }

        XXGdsDatasetPolicyMap existing = daoMgr.getXXGdsDatasetPolicyMap().getDatasetPolicyMap(datasetId, policyId);

        if (existing == null) {
            throw new Exception("no policy exists: datasetId=" + datasetId + ", policyId=" + policyId);
        }

        RangerPolicy policy = svcStore.getPolicy(policyId);

        daoMgr.getXXGdsDatasetPolicyMap().remove(existing);
        svcStore.deletePolicy(policy);

        LOG.debug("<== deleteDatasetPolicy({}, {})", datasetId, policyId);
    }

    @Override
    public void deleteDatasetPolicies(Long datasetId) throws Exception {
        LOG.debug("==> deleteDatasetPolicies({})", datasetId);

        RangerDataset dataset = datasetService.read(datasetId);

        deleteDatasetPolicies(dataset);

        LOG.debug("<== deleteDatasetPolicy({})", datasetId);
    }

    @Override
    public RangerPolicy getDatasetPolicy(Long datasetId, Long policyId) throws Exception {
        LOG.debug("==> getDatasetPolicy({}, {})", datasetId, policyId);

        RangerDataset dataset = datasetService.read(datasetId);

        if (!validator.hasPermission(dataset.getAcl(), GdsPermission.AUDIT)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_TO_VIEW_DATASET_POLICIES);
        }

        XXGdsDatasetPolicyMap existing = daoMgr.getXXGdsDatasetPolicyMap().getDatasetPolicyMap(datasetId, policyId);

        if (existing == null) {
            throw new Exception("no policy exists: datasetId=" + datasetId + ", policyId=" + policyId);
        }

        RangerPolicy ret = svcStore.getPolicy(policyId);

        LOG.debug("<== getDatasetPolicy({}, {}): ret={}", datasetId, policyId, ret);

        return ret;
    }

    @Override
    public List<RangerPolicy> getDatasetPolicies(Long datasetId) throws Exception {
        LOG.debug("==> getDatasetPolicies({})", datasetId);

        List<RangerPolicy> ret = null;

        RangerDataset dataset = datasetService.read(datasetId);

        if (!validator.hasPermission(dataset.getAcl(), GdsPermission.AUDIT)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_TO_VIEW_DATASET_POLICIES);
        }

        List<Long> policyIds = daoMgr.getXXGdsDatasetPolicyMap().getDatasetPolicyIds(datasetId);

        if (policyIds != null) {
            ret = new ArrayList<>(policyIds.size());

            for (Long policyId : policyIds) {
                ret.add(svcStore.getPolicy(policyId));
            }
        }

        LOG.debug("<== getDatasetPolicies({}): ret={}", datasetId, ret);

        return ret;
    }

    @Override
    public RangerProject createProject(RangerProject project) {
        LOG.debug("==> createProject({})", project);

        validator.validateCreate(project);

        if (StringUtils.isBlank(project.getGuid())) {
            project.setGuid(guidUtil.genGUID());
        }

        if (project.getAcl() == null) {
            project.setAcl(new RangerGdsObjectACL());
        }

        addCreatorAsAclAdmin(project.getAcl());

        RangerProject ret = projectService.create(project);

        projectService.createObjectHistory(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_PROJECT);

        LOG.debug("<== createProject({}): ret={}", project, ret);

        return ret;
    }

    @Override
    public RangerProject updateProject(RangerProject project) {
        LOG.debug("==> updateProject({})", project);

        RangerProject existing = null;

        try {
            existing = projectService.read(project.getId());
        } catch (Exception excp) {
            // ignore
        }

        validator.validateUpdate(project, existing);

        RangerProject ret = projectService.update(project);

        projectService.createObjectHistory(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_PROJECT);

        LOG.debug("<== updateProject({}): ret={}", project, ret);

        return ret;
    }

    @Override
    public void deleteProject(Long projectId) throws Exception {
        LOG.debug("==> deleteProject({})", projectId);

        RangerProject existing = null;

        try {
            existing = projectService.read(projectId);
        } catch(Exception excp) {
            // ignore
        }

        validator.validateDelete(projectId, existing);

        if (existing != null) {
            deleteProjectPolicies(existing);
            projectService.delete(existing);

            projectService.createObjectHistory(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

            updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_PROJECT);
        }

        LOG.debug("<== deleteProject({})", projectId);
    }

    @Override
    public RangerProject getProject(Long projectId) throws Exception {
        LOG.debug("==> getProject({})", projectId);

        RangerProject ret = projectService.read(projectId);

        if (ret != null && !validator.hasPermission(ret.getAcl(), GdsPermission.VIEW)) {
            throw new Exception("no permission on project id=" + projectId);
        }

        LOG.debug("<== getProject({}): ret={}", projectId, ret);

        return ret;
    }

    @Override
    public RangerProject getProjectByName(String name) throws Exception {
        LOG.debug("==> getProjectByName({})", name);

        XXGdsProjectDao projectDao = daoMgr.getXXGdsProject();
        XXGdsProject    existing   = projectDao.findByName(name);

        if (existing == null) {
            throw new Exception("no project with name=" + name);
        }

        RangerProject ret = projectService.getPopulatedViewObject(existing);

        if (ret != null && !validator.hasPermission(ret.getAcl(), GdsPermission.VIEW)) {
            throw new Exception("no permission on project name=" + name);
        }

        LOG.debug("<== getProjectByName({}): ret={}", name, ret);

        return ret;
    }

    @Override
    public PList<String> getProjectNames(SearchFilter filter) {
        LOG.debug("==> getProjectNames({})", filter);

        PList<RangerProject> projects = searchProjects(filter);
        PList<String>        ret      = new PList<>(new ArrayList<>(), projects.getStartIndex(), projects.getPageSize(), projects.getTotalCount(), projects.getResultSize(), projects.getSortType(), projects.getSortBy());

        ret.setQueryTimeMS(projects.getQueryTimeMS());

        if (CollectionUtils.isNotEmpty(projects.getList())) {
            for (RangerProject project : projects.getList()) {
                ret.getList().add(project.getName());
            }
        }

        LOG.debug("<== getProjectNames({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public PList<RangerProject> searchProjects(SearchFilter filter) {
        LOG.debug("==> searchProjects({})", filter);

        int maxRows    = filter.getMaxRows();
        int startIndex = filter.getStartIndex();

        filter.setStartIndex(0);
        filter.setMaxRows(0);

        GdsPermission       gdsPermission = getGdsPermissionFromFilter(filter);
        RangerProjectList   result        = projectService.searchProjects(filter);
        List<RangerProject> projects      = new ArrayList<>();

        for (RangerProject project : result.getList()) {
            if (gdsPermission.equals(GdsPermission.LIST)) {
                scrubProjectForListing(project);
            }

            projects.add(project);
        }

        int endIndex = Math.min((startIndex + maxRows), projects.size());
        List<RangerProject> paginatedProjects = projects.subList(startIndex, endIndex);
        PList<RangerProject> ret = new PList<>(paginatedProjects, startIndex, maxRows, projects.size(), paginatedProjects.size(), result.getSortBy(), result.getSortType());

        LOG.debug("<== searchProjects({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public RangerPolicy addProjectPolicy(Long projectId, RangerPolicy policy) throws Exception {
        LOG.debug("==> addProjectPolicy({}, {})", projectId, policy);

        RangerProject project = projectService.read(projectId);

        if (!validator.hasPermission(project.getAcl(), GdsPermission.POLICY_ADMIN)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_FOR_PROJECT_POLICIES);
        }

        prepareProjectPolicy(project, policy);

        RangerPolicy ret = svcStore.createPolicy(policy);

        daoMgr.getXXGdsProjectPolicyMap().create(new XXGdsProjectPolicyMap(projectId, ret.getId()));

        LOG.debug("<== addProjectPolicy({}, {}): ret={}", projectId, policy, ret);

        return ret;
    }

    @Override
    public RangerPolicy updateProjectPolicy(Long projectId, RangerPolicy policy) throws Exception {
        LOG.debug("==> updateProjectPolicy({}, {})", projectId, policy);

        RangerProject project = projectService.read(projectId);

        if (!validator.hasPermission(project.getAcl(), GdsPermission.POLICY_ADMIN)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_FOR_PROJECT_POLICIES);
        }

        XXGdsProjectPolicyMap existing = daoMgr.getXXGdsProjectPolicyMap().getProjectPolicyMap(projectId, policy.getId());

        if (existing == null) {
            throw new Exception("no policy exists: projectId=" + projectId + ", policyId=" + policy.getId());
        }

        prepareProjectPolicy(project, policy);

        RangerPolicy ret = svcStore.updatePolicy(policy);

        LOG.debug("<== updateProjectPolicy({}, {}): ret={}", projectId, policy, ret);

        return ret;
    }

    @Override
    public void deleteProjectPolicy(Long projectId, Long policyId) throws Exception {
        LOG.debug("==> deleteProjectPolicy({}, {})", projectId, policyId);

        RangerProject project = projectService.read(projectId);

        if (!validator.hasPermission(project.getAcl(), GdsPermission.POLICY_ADMIN)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_FOR_DATASET_POLICIES);
        }

        XXGdsProjectPolicyMap existing = daoMgr.getXXGdsProjectPolicyMap().getProjectPolicyMap(projectId, policyId);

        if (existing == null) {
            throw new Exception("no policy exists: projectId=" + projectId + ", policyId=" + policyId);
        }

        RangerPolicy policy = svcStore.getPolicy(policyId);

        daoMgr.getXXGdsProjectPolicyMap().remove(existing);
        svcStore.deletePolicy(policy);

        LOG.debug("<== deleteProjectPolicy({}, {})", projectId, policyId);
    }

    @Override
    public void deleteProjectPolicies(Long projectId) throws Exception {
        LOG.debug("==> deleteProjectPolicies({})", projectId);

        RangerProject project = projectService.read(projectId);

        deleteProjectPolicies(project);

        LOG.debug("<== deleteProjectPolicy({})", projectId);
    }

    @Override
    public RangerPolicy getProjectPolicy(Long projectId, Long policyId) throws Exception {
        LOG.debug("==> getProjectPolicy({}, {})", projectId, policyId);

        RangerProject project = projectService.read(projectId);

        if (!validator.hasPermission(project.getAcl(), GdsPermission.AUDIT)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_TO_VIEW_PROJECT_POLICIES);
        }

        XXGdsProjectPolicyMap existing = daoMgr.getXXGdsProjectPolicyMap().getProjectPolicyMap(projectId, policyId);

        if (existing == null) {
            throw new Exception("no policy exists: projectId=" + projectId + ", policyId=" + policyId);
        }

        RangerPolicy ret = svcStore.getPolicy(policyId);

        LOG.debug("<== getProjectPolicy({}, {}): ret={}", projectId, policyId, ret);

        return ret;
    }

    @Override
    public List<RangerPolicy> getProjectPolicies(Long projectId) throws Exception {
        LOG.debug("==> getProjectPolicies({})", projectId);

        List<RangerPolicy> ret = null;

        RangerProject project = projectService.read(projectId);

        if (!validator.hasPermission(project.getAcl(), GdsPermission.AUDIT)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_TO_VIEW_PROJECT_POLICIES);
        }

        List<Long> policyIds = daoMgr.getXXGdsProjectPolicyMap().getProjectPolicyIds(projectId);

        if (policyIds != null) {
            ret = new ArrayList<>(policyIds.size());

            for (Long policyId : policyIds) {
                ret.add(svcStore.getPolicy(policyId));
            }
        }

        LOG.debug("<== getProjectPolicies({}): ret={}", projectId, ret);

        return ret;
    }


    @Override
    public RangerDataShare createDataShare(RangerDataShare dataShare) {
        LOG.debug("==> createDataShare({})", dataShare);

        validator.validateCreate(dataShare);

        if (StringUtils.isBlank(dataShare.getGuid())) {
            dataShare.setGuid(guidUtil.genGUID());
        }

        if (dataShare.getAcl() == null) {
            dataShare.setAcl(new RangerGdsObjectACL());
        }

        addCreatorAsAclAdmin(dataShare.getAcl());

        RangerDataShare ret = dataShareService.create(dataShare);

        dataShareService.createObjectHistory(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_DATA_SHARE);

        LOG.debug("<== createDataShare({}): ret={}", dataShare, ret);

        return ret;
    }

    @Override
    public RangerDataShare updateDataShare(RangerDataShare dataShare) {
        LOG.debug("==> updateDataShare({})", dataShare);

        RangerDataShare existing = null;

        try {
            existing = dataShareService.read(dataShare.getId());
        } catch (Exception excp) {
            // ignore
        }

        validator.validateUpdate(dataShare, existing);

        RangerDataShare ret = dataShareService.update(dataShare);

        dataShareService.createObjectHistory(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_DATA_SHARE);

        LOG.debug("<== updateDataShare({}): ret={}", dataShare, ret);

        return ret;
    }

    @Override
    public void deleteDataShare(Long dataShareId, boolean forceDelete) {
        LOG.debug("==> deleteDataShare(dataShareId: {}, forceDelete: {})", dataShareId, forceDelete);

        RangerDataShare existing = null;

        try {
            existing = dataShareService.read(dataShareId);
        } catch (Exception excp) {
            // ignore
        }

        validator.validateDelete(dataShareId, existing);

        if(forceDelete) {
            removeDshInDsForDataShare(dataShareId);
            removeSharedResourcesForDataShare(dataShareId);
        }

        if (existing != null) {
            dataShareService.delete(existing);

            dataShareService.createObjectHistory(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

            updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_DATA_SHARE);
        }

        LOG.debug("<== deleteDataShare(dataShareId: {}, forceDelete: {})", dataShareId, forceDelete);
    }

    @Override
    public RangerDataShare getDataShare(Long dataShareId) {
        LOG.debug("==> getDataShare({})", dataShareId);

        RangerDataShare ret = dataShareService.read(dataShareId);

        // TODO: enforce RangerDataShare.acl

        LOG.debug("<== getDataShare({}): ret={}", dataShareId, ret);

        return ret;
    }

    @Override
    public PList<RangerDataShare> searchDataShares(SearchFilter filter) {
        LOG.debug("==> searchDataShares({})", filter);

        int maxRows = filter.getMaxRows();
        int startIndex = filter.getStartIndex();
        filter.setStartIndex(0);
        filter.setMaxRows(0);

        RangerDataShareList   result     = dataShareService.searchDataShares(filter);
        List<RangerDataShare> dataShares = new ArrayList<>();

        for (RangerDataShare dataShare : result.getList()) {
            // TODO: enforce RangerDataShare.acl

            dataShares.add(dataShare);
        }

        int endIndex = Math.min((startIndex + maxRows), dataShares.size());
        List<RangerDataShare> paginatedDataShares = dataShares.subList(startIndex, endIndex);
        PList<RangerDataShare> ret = new PList<>(paginatedDataShares, startIndex, maxRows, dataShares.size(), paginatedDataShares.size(), result.getSortBy(), result.getSortType());

        LOG.debug("<== searchDataShares({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public RangerSharedResource addSharedResource(RangerSharedResource resource) {
        LOG.debug("==> addSharedResource({})", resource);

        validator.validateCreate(resource);

        if (StringUtils.isBlank(resource.getGuid())) {
            resource.setGuid(guidUtil.genGUID());
        }

        RangerSharedResource ret = sharedResourceService.create(resource);

        sharedResourceService.createObjectHistory(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        LOG.debug("<== addSharedResource({}): ret={}", resource, ret);

        return ret;
    }

    @Override
    public RangerSharedResource updateSharedResource(RangerSharedResource resource) {
        LOG.debug("==> updateSharedResource({})", resource);

        RangerSharedResource existing = null;

        try {
            existing = sharedResourceService.read(resource.getId());
        } catch (Exception excp) {
            // ignore
        }

        validator.validateUpdate(resource, existing);

        RangerSharedResource ret = sharedResourceService.update(resource);

        sharedResourceService.createObjectHistory(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        LOG.debug("<== updateSharedResource({}): ret={}", resource, ret);

        return ret;
    }

    @Override
    public void removeSharedResource(Long sharedResourceId) {
        LOG.debug("==> removeSharedResource({})", sharedResourceId);


        RangerSharedResource existing = null;

        try {
            existing = sharedResourceService.read(sharedResourceId);
        } catch (Exception excp) {
            // ignore
        }

        validator.validateDelete(sharedResourceId, existing);

        if (existing != null) {
            sharedResourceService.delete(existing);

            sharedResourceService.createObjectHistory(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);
        }

        LOG.debug("<== removeSharedResource({})", sharedResourceId);
    }

    @Override
    public RangerSharedResource getSharedResource(Long sharedResourceId) {
        LOG.debug("==> getSharedResource({})", sharedResourceId);

        RangerSharedResource ret = sharedResourceService.read(sharedResourceId);

        // TODO: enforce RangerSharedResource.acl

        LOG.debug("<== getSharedResource({}): ret={}", sharedResourceId, ret);

        return ret;
    }

    @Override
    public PList<RangerSharedResource> searchSharedResources(SearchFilter filter) {
        LOG.debug("==> searchSharedResources({})", filter);

        int maxRows = filter.getMaxRows();
        int startIndex = filter.getStartIndex();
        filter.setStartIndex(0);
        filter.setMaxRows(0);

        RangerSharedResourceList   result          = sharedResourceService.searchSharedResources(filter);
        List<RangerSharedResource> sharedResources = new ArrayList<>();

        for (RangerSharedResource dataShare : result.getList()) {
            // TODO: enforce RangerSharedResource.acl

            sharedResources.add(dataShare);
        }

        int endIndex = Math.min((startIndex + maxRows), sharedResources.size());
        List<RangerSharedResource> paginatedSharedResources = sharedResources.subList(startIndex, endIndex);
        PList<RangerSharedResource> ret = new PList<>(paginatedSharedResources, startIndex, maxRows, sharedResources.size(), paginatedSharedResources.size(), result.getSortBy(), result.getSortType());

        LOG.debug("<== searchSharedResources({}): ret={}", filter, ret);

        return ret;
    }


    @Override
    public RangerDataShareInDataset addDataShareInDataset(RangerDataShareInDataset dataShareInDataset) throws Exception {
        LOG.debug("==> addDataShareInDataset({})", dataShareInDataset);

        XXGdsDataShareInDatasetDao datasetDao = daoMgr.getXXGdsDataShareInDataset();
        XXGdsDataShareInDataset    existing   = datasetDao.findByDataShareIdAndDatasetId(dataShareInDataset.getDataShareId(), dataShareInDataset.getDatasetId());

        if (existing != null) {
            throw new Exception("data share '" + dataShareInDataset.getDataShareId() + "' already shared with dataset " + dataShareInDataset.getDatasetId() + " - id=" + existing.getId());
        }

        validator.validateCreate(dataShareInDataset);

        if (StringUtils.isBlank(dataShareInDataset.getGuid())) {
            dataShareInDataset.setGuid(guidUtil.genGUID());
        }

        RangerDataShareInDataset ret = dataShareInDatasetService.create(dataShareInDataset);

        dataShareInDatasetService.createObjectHistory(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        LOG.debug("<== addDataShareInDataset({}): ret={}", dataShareInDataset, ret);

        return ret;
    }

    @Override
    public RangerDataShareInDataset updateDataShareInDataset(RangerDataShareInDataset dataShareInDataset) {
        LOG.debug("==> updateDataShareInDataset({})", dataShareInDataset);

        RangerDataShareInDataset existing = dataShareInDatasetService.read(dataShareInDataset.getId());

        validator.validateUpdate(dataShareInDataset, existing);

        RangerDataShareInDataset ret = dataShareInDatasetService.update(dataShareInDataset);

        dataShareInDatasetService.createObjectHistory(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        LOG.debug("<== updateDataShareInDataset({}): ret={}", dataShareInDataset, ret);

        return ret;
    }

    @Override
    public void removeDataShareInDataset(Long dataShareInDatasetId) {
        LOG.debug("==> removeDataShareInDataset({})", dataShareInDatasetId);

        RangerDataShareInDataset existing = dataShareInDatasetService.read(dataShareInDatasetId);

        validator.validateDelete(dataShareInDatasetId, existing);

        dataShareInDatasetService.delete(existing);

        dataShareInDatasetService.createObjectHistory(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

        LOG.debug("<== removeDataShareInDataset({})", dataShareInDatasetId);
    }

    @Override
    public RangerDataShareInDataset getDataShareInDataset(Long dataShareInDatasetId) {
        LOG.debug("==> getDataShareInDataset({})", dataShareInDatasetId);

        RangerDataShareInDataset ret = dataShareInDatasetService.read(dataShareInDatasetId);

        LOG.debug("<== getDataShareInDataset({}): ret={}", dataShareInDatasetId, ret);

        return ret;
    }

    @Override
    public PList<RangerDataShareInDataset> searchDataShareInDatasets(SearchFilter filter) {
        LOG.debug("==> searchDataShareInDatasets({})", filter);

        int maxRows = filter.getMaxRows();
        int startIndex = filter.getStartIndex();
        filter.setStartIndex(0);
        filter.setMaxRows(0);

        List<RangerDataShareInDataset> dataShareInDatasets = new ArrayList<>();
        RangerDataShareInDatasetList   result              = dataShareInDatasetService.searchDataShareInDatasets(filter);

        for (RangerDataShareInDataset dataShareInDataset : result.getList()) {
            // TODO: enforce RangerSharedResource.acl

            dataShareInDatasets.add(dataShareInDataset);
        }

        int endIndex = Math.min((startIndex + maxRows), dataShareInDatasets.size());
        List<RangerDataShareInDataset> paginatedDataShareInDatasets = dataShareInDatasets.subList(startIndex, endIndex);
        PList<RangerDataShareInDataset> ret = new PList<>(paginatedDataShareInDatasets, startIndex, maxRows, dataShareInDatasets.size(), paginatedDataShareInDatasets.size(), result.getSortBy(), result.getSortType());

        LOG.debug("<== searchDataShareInDatasets({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public RangerDatasetInProject addDatasetInProject(RangerDatasetInProject datasetInProject) throws Exception {
        LOG.debug("==> addDatasetInProject({})", datasetInProject);

        XXGdsDatasetInProjectDao datasetDao = daoMgr.getXXGdsDatasetInProject();
        XXGdsDatasetInProject    existing   = datasetDao.findByDatasetIdAndProjectId(datasetInProject.getDatasetId(), datasetInProject.getProjectId());

        if (existing != null) {
            throw new Exception("dataset '" + datasetInProject.getDatasetId() + "' already shared with project " + datasetInProject.getProjectId() + " - id=" + existing.getId());
        }

        validator.validateCreate(datasetInProject);

        if (StringUtils.isBlank(datasetInProject.getGuid())) {
            datasetInProject.setGuid(guidUtil.genGUID());
        }

        RangerDatasetInProject ret = datasetInProjectService.create(datasetInProject);

        datasetInProjectService.createObjectHistory(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        LOG.debug("<== addDatasetInProject({}): ret={}", datasetInProject, ret);

        return ret;
    }

    @Override
    public RangerDatasetInProject updateDatasetInProject(RangerDatasetInProject datasetInProject) {
        LOG.debug("==> updateDatasetInProject({})", datasetInProject);

        RangerDatasetInProject existing = datasetInProjectService.read(datasetInProject.getId());

        validator.validateUpdate(datasetInProject, existing);

        RangerDatasetInProject ret = datasetInProjectService.update(datasetInProject);

        datasetInProjectService.createObjectHistory(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        LOG.debug("<== updateDatasetInProject({}): ret={}", datasetInProject, ret);

        return ret;
    }

    @Override
    public void removeDatasetInProject(Long datasetInProjectId) {
        LOG.debug("==> removeDatasetInProject({})", datasetInProjectId);

        RangerDatasetInProject existing = datasetInProjectService.read(datasetInProjectId);

        validator.validateDelete(datasetInProjectId, existing);

        datasetInProjectService.delete(existing);

        datasetInProjectService.createObjectHistory(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

        LOG.debug("<== removeDatasetInProject({})", datasetInProjectId);
    }

    @Override
    public RangerDatasetInProject getDatasetInProject(Long datasetInProjectId) {
        LOG.debug("==> getDatasetInProject({})", datasetInProjectId);

        RangerDatasetInProject ret = datasetInProjectService.read(datasetInProjectId);

        // TODO: enforce RangerDatasetInProject.acl

        LOG.debug("<== getDatasetInProject({}): ret={}", datasetInProjectId, ret);

        return ret;
    }

    @Override
    public PList<RangerDatasetInProject> searchDatasetInProjects(SearchFilter filter) {
        LOG.debug("==> searchDatasetInProjects({})", filter);

        int maxRows = filter.getMaxRows();
        int startIndex = filter.getStartIndex();
        filter.setStartIndex(0);
        filter.setMaxRows(0);

        List<RangerDatasetInProject> datasetInProjects = new ArrayList<>();
        RangerDatasetInProjectList   result            = datasetInProjectService.searchDatasetInProjects(filter);

        for (RangerDatasetInProject datasetInProject : result.getList()) {
            // TODO: enforce RangerDatasetInProject.acl

            datasetInProjects.add(datasetInProject);
        }

        int endIndex = Math.min((startIndex + maxRows), datasetInProjects.size());
        List<RangerDatasetInProject> paginatedDatasetInProjects = datasetInProjects.subList(startIndex, endIndex);
        PList<RangerDatasetInProject> ret = new PList<>(paginatedDatasetInProjects, startIndex, maxRows, datasetInProjects.size(), paginatedDatasetInProjects.size(), result.getSortBy(), result.getSortType());

        LOG.debug("<== searchDatasetInProjects({}): ret={}", filter, ret);

        return ret;
    }

    private void updateGlobalVersion(String stateName) {
        transactionSynchronizationAdapter.executeOnTransactionCommit(new GlobalVersionUpdater(daoMgr, stateName));
    }

    private static class GlobalVersionUpdater implements Runnable {
        final RangerDaoManager daoManager;
        final String           stateName;

        public GlobalVersionUpdater(RangerDaoManager daoManager, String stateName) {
            this.daoManager = daoManager;
            this.stateName  = stateName;
        }

        @Override
        public void run() {
            try {
                this.daoManager.getXXGlobalState().onGlobalAppDataChange(stateName);
            } catch (Exception e) {
                LOG.error("Failed to update GlobalState version for state:[{}]", stateName , e);
            }
        }
    }

    private List<DatasetSummary> toDatasetSummary(List<RangerDataset> datasets, GdsPermission gdsPermission) {
        List<DatasetSummary> ret         = new ArrayList<>();
        String               currentUser = bizUtil.getCurrentUserLoginId();

        for (RangerDataset dataset : datasets) {
            GdsPermission permissionForCaller = validator.getGdsPermissionForUser(dataset.getAcl(), currentUser);

            if (permissionForCaller.equals(GdsPermission.NONE)) {
                continue;
            }

            DatasetSummary datasetSummary = new DatasetSummary();

            datasetSummary.setId(dataset.getId());
            datasetSummary.setName(dataset.getName());
            datasetSummary.setDescription(dataset.getDescription());
            datasetSummary.setCreateTime(dataset.getCreateTime());
            datasetSummary.setUpdateTime(dataset.getUpdateTime());
            datasetSummary.setCreatedBy(dataset.getCreatedBy());
            datasetSummary.setUpdatedBy(dataset.getUpdatedBy());
            datasetSummary.setIsEnabled(dataset.getIsEnabled());
            datasetSummary.setGuid(dataset.getGuid());
            datasetSummary.setVersion(dataset.getVersion());
            datasetSummary.setPermissionForCaller(permissionForCaller);

            if (!gdsPermission.equals(GdsPermission.LIST)) {
                datasetSummary.setProjectsCount(getDIPCountForDataset(dataset.getId()));
                datasetSummary.setPrincipalsCount(getPrincipalCountForDataset(dataset.getName()));

                List<DataShareInDatasetSummary> dshInDsSummaryList = getDshInDsSummaryList(dataset.getId());

                datasetSummary.setDataShares(dshInDsSummaryList);
                datasetSummary.setTotalResourceCount(dshInDsSummaryList.stream()
                        .map(DataShareInDatasetSummary::getResourceCount)
                        .mapToLong(Long::longValue)
                        .sum());
            }

            ret.add(datasetSummary);
        }

        return ret;
    }

    private Map<GdsShareStatus, Long> getDataSharesInDatasetCountByStatus(Long datasetId) {
        Map<GdsShareStatus, Long> ret            = new HashMap<>();
        Map<Short, Long>          countsByStatus = daoMgr.getXXGdsDataShareInDataset().getDataSharesInDatasetCountByStatus(datasetId);

        for (Map.Entry<Short, Long> entry : countsByStatus.entrySet()) {
            ret.put(RangerGdsDatasetInProjectService.toShareStatus(entry.getKey()), entry.getValue());
        }

        return ret;
    }

    private Long getDIPCountForDataset(Long datasetId) {
        return datasetInProjectService.getDatasetsInProjectCount(datasetId);
    }

    private Map<PrincipalType, Long> getPrincipalCountForDataset(String datasetName) {
        Map<PrincipalType, Long> ret    = new HashMap<>();
        Set<String>              users  = new HashSet<>();
        Set<String>              groups = new HashSet<>();
        Set<String>              roles  = new HashSet<>();

        if (StringUtils.isNotEmpty(datasetName)) {
            List<XXPolicy> policies = daoMgr.getXXPolicy().findByServiceType(EMBEDDED_SERVICEDEF_GDS_NAME);

            for (XXPolicy policyFromDb : policies) {
                RangerPolicy                     policy    = policyService.getPopulatedViewObject(policyFromDb);
                Collection<RangerPolicyResource> resources = policy.getResources().values();

                for (RangerPolicyResource resource : resources) {
                    if (resource.getValues().contains(datasetName)){
                        List<RangerPolicyItem> policyItems = policy.getPolicyItems();

                        for (RangerPolicyItem policyItem : policyItems) {
                            users.addAll(policyItem.getUsers());
                            groups.addAll(policyItem.getGroups());
                            roles.addAll(policyItem.getRoles());
                        }
                    }
                }
            }
        }

        ret.put(PrincipalType.USER,  (long) users.size());
        ret.put(PrincipalType.GROUP, (long) groups.size());
        ret.put(PrincipalType.ROLE,  (long) roles.size());

        return ret;
    }

    private PList<RangerDataset> getUnscrubbedDatasets(SearchFilter filter) {
        int maxRows    = filter.getMaxRows();
        int startIndex = filter.getStartIndex();

        filter.setStartIndex(0);
        filter.setMaxRows(0);

        GdsPermission       gdsPermission = getGdsPermissionFromFilter(filter);
        RangerDatasetList   result        = datasetService.searchDatasets(filter);
        List<RangerDataset> datasets      = new ArrayList<>();

        for (RangerDataset dataset : result.getList()) {
            if (dataset != null && validator.hasPermission(dataset.getAcl(), gdsPermission)) {
                datasets.add(dataset);
            }
        }

        int                  endIndex          = Math.min((startIndex + maxRows), datasets.size());
        List<RangerDataset>  paginatedDatasets = datasets.subList(startIndex, endIndex);
        PList<RangerDataset> ret               = new PList<>(paginatedDatasets, startIndex, maxRows, datasets.size(), paginatedDatasets.size(), result.getSortBy(), result.getSortType());

        return ret;
    }

    private GdsPermission getGdsPermissionFromFilter(SearchFilter filter) {
        String        gdsPermissionStr = filter.getParam(SearchFilter.GDS_PERMISSION);
        GdsPermission gdsPermission    = null;

        if (StringUtils.isNotEmpty(gdsPermissionStr)) {
            try {
                gdsPermission = GdsPermission.valueOf(gdsPermissionStr);
            } catch (IllegalArgumentException ex) {
                LOG.info("Ignoring invalid GdsPermission: {}", gdsPermissionStr);
            }
        }

        if (gdsPermission == null) {
            gdsPermission = GdsPermission.VIEW;
        }

        return gdsPermission;
    }

    private void scrubDatasetForListing(RangerDataset dataset) {
        dataset.setAcl(null);
        dataset.setOptions(null);
        dataset.setAdditionalInfo(null);
    }

    private void scrubProjectForListing(RangerProject project) {
        project.setAcl(null);
        project.setOptions(null);
        project.setAdditionalInfo(null);
    }

    private void removeDshInDsForDataShare(Long dataShareId) {
        SearchFilter                 filter      = new SearchFilter(SearchFilter.DATA_SHARE_ID, dataShareId.toString());
        RangerDataShareInDatasetList dshInDsList = dataShareInDatasetService.searchDataShareInDatasets(filter);

        for(RangerDataShareInDataset dshInDs : dshInDsList.getList()) {
            final boolean dshInDsDeleted = dataShareInDatasetService.delete(dshInDs);

            if(!dshInDsDeleted) {
                throw restErrorUtil.createRESTException("DataShareInDataset could not be deleted", MessageEnums.ERROR_DELETE_OBJECT, dshInDs.getId(), "DataSHareInDatasetId", null, 500);
            }
        }
    }

    private void removeSharedResourcesForDataShare(Long dataShareId) {
        SearchFilter             filter          = new SearchFilter(SearchFilter.DATA_SHARE_ID, dataShareId.toString());
        RangerSharedResourceList sharedResources = sharedResourceService.searchSharedResources(filter);

        for (RangerSharedResource sharedResource : sharedResources.getList()) {
            final boolean sharedResourceDeleted = sharedResourceService.delete(sharedResource);

            if (!sharedResourceDeleted) {
                throw restErrorUtil.createRESTException("SharedResource could not be deleted", MessageEnums.ERROR_DELETE_OBJECT, sharedResource.getId(), "SharedResourceId", null, HttpStatus.SC_INTERNAL_SERVER_ERROR);
            }
        }
    }

    private void prepareDatasetPolicy(RangerDataset dataset, RangerPolicy policy) {
        policy.setName("DATASET: " + dataset.getName() + "@" + System.currentTimeMillis());
        policy.setDescription("Policy for dataset: " + dataset.getName());
        policy.setServiceType(EMBEDDED_SERVICEDEF_GDS_NAME);
        policy.setService(ServiceDBStore.GDS_SERVICE_NAME);
        policy.setZoneName(null);
        policy.setResources(Collections.singletonMap(RESOURCE_NAME_DATASET_ID, new RangerPolicyResource(dataset.getId().toString())));
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        policy.setPolicyPriority(RangerPolicy.POLICY_PRIORITY_NORMAL);
        policy.setAllowExceptions(Collections.emptyList());
        policy.setDenyPolicyItems(Collections.emptyList());
        policy.setDenyExceptions(Collections.emptyList());
        policy.setDataMaskPolicyItems(Collections.emptyList());
        policy.setRowFilterPolicyItems(Collections.emptyList());
        policy.setIsDenyAllElse(Boolean.FALSE);
    }

    private void prepareProjectPolicy(RangerProject project, RangerPolicy policy) {
        policy.setName("PROJECT: " + project.getName() + "@" + System.currentTimeMillis());
        policy.setDescription("Policy for project: " + project.getName());
        policy.setServiceType(EMBEDDED_SERVICEDEF_GDS_NAME);
        policy.setService(ServiceDBStore.GDS_SERVICE_NAME);
        policy.setZoneName(null);
        policy.setResources(Collections.singletonMap(RESOURCE_NAME_PROJECT_ID, new RangerPolicyResource(project.getId().toString())));
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        policy.setPolicyPriority(RangerPolicy.POLICY_PRIORITY_NORMAL);
        policy.setAllowExceptions(Collections.emptyList());
        policy.setDenyPolicyItems(Collections.emptyList());
        policy.setDenyExceptions(Collections.emptyList());
        policy.setDataMaskPolicyItems(Collections.emptyList());
        policy.setRowFilterPolicyItems(Collections.emptyList());
        policy.setIsDenyAllElse(Boolean.FALSE);
    }

    private void deleteDatasetPolicies(RangerDataset dataset) throws Exception {
        if (!validator.hasPermission(dataset.getAcl(), GdsPermission.POLICY_ADMIN)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_FOR_DATASET_POLICIES);
        }

        List<XXGdsDatasetPolicyMap> existingMaps = daoMgr.getXXGdsDatasetPolicyMap().getDatasetPolicyMaps(dataset.getId());

        if (existingMaps != null) {
            for (XXGdsDatasetPolicyMap existing : existingMaps) {
                RangerPolicy policy = svcStore.getPolicy(existing.getPolicyId());

                daoMgr.getXXGdsDatasetPolicyMap().remove(existing);
                svcStore.deletePolicy(policy);
            }
        }
    }

    private void deleteProjectPolicies(RangerProject project) throws Exception {
        if (!validator.hasPermission(project.getAcl(), GdsPermission.POLICY_ADMIN)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_FOR_PROJECT_POLICIES);
        }

        List<XXGdsProjectPolicyMap> existingMaps = daoMgr.getXXGdsProjectPolicyMap().getProjectPolicyMaps(project.getId());

        if (existingMaps != null) {
            for (XXGdsProjectPolicyMap existing : existingMaps) {
                RangerPolicy policy = svcStore.getPolicy(existing.getPolicyId());

                daoMgr.getXXGdsProjectPolicyMap().remove(existing);
                svcStore.deletePolicy(policy);
            }
        }
    }

    private void removeDIPForDataset(Long datasetId) {
        XXGdsDatasetInProjectDao    dipDao    = daoMgr.getXXGdsDatasetInProject();
        List<XXGdsDatasetInProject> dshidList = dipDao.findByDatasetId(datasetId);

        for (XXGdsDatasetInProject dip : dshidList) {
            boolean dipDeleted = dipDao.remove(dip.getId());

            if (!dipDeleted) {
                throw restErrorUtil.createRESTException("DatasetInProject could not be deleted",
                                                        MessageEnums.ERROR_DELETE_OBJECT, dip.getId(), "DatasetInProjectId", null,
                                                        HttpStatus.SC_INTERNAL_SERVER_ERROR);
            }
        }
    }

    private void removeDSHIDForDataset(Long datasetId) {
        XXGdsDataShareInDatasetDao    dshidDao  = daoMgr.getXXGdsDataShareInDataset();
        List<XXGdsDataShareInDataset> dshidList = dshidDao.findByDatasetId(datasetId);

        for (XXGdsDataShareInDataset dshid : dshidList) {
            boolean dshidDeleted = dshidDao.remove(dshid.getId());

            if (!dshidDeleted) {
                throw restErrorUtil.createRESTException("DataShareInDataset could not be deleted",
                                                        MessageEnums.ERROR_DELETE_OBJECT, dshid.getId(), "DataShareInDataset", null,
                                                        HttpStatus.SC_INTERNAL_SERVER_ERROR);
            }
        }
    }

    private void addCreatorAsAclAdmin(RangerGdsObjectACL acl) {
        String currentUser = bizUtil.getCurrentUserLoginId();
        Map<String, GdsPermission> userAcl = acl.getUsers();

        if (userAcl == null) {
            userAcl = new HashMap<>();

            acl.setUsers(userAcl);
        }

        if (acl.getUsers().get(currentUser) != GdsPermission.ADMIN) {
            acl.getUsers().put(currentUser, GdsPermission.ADMIN);
        }
    }

    private List<DataShareInDatasetSummary> getDshInDsSummaryList(Long datasetId) {
        List<DataShareInDatasetSummary> ret        = new ArrayList<>();
        SearchFilter                    filter     = new SearchFilter(SearchFilter.DATASET_ID, datasetId.toString());
        RangerDataShareList             dataShares = dataShareService.searchDataShares(filter);

        if (CollectionUtils.isNotEmpty(dataShares.getList())) {
            RangerDataShareInDatasetList dshInDsList = dataShareInDatasetService.searchDataShareInDatasets(filter);

            if (CollectionUtils.isNotEmpty(dshInDsList.getList())) {
                for (RangerDataShare dataShare : dataShares.getList()) {
                    DataShareInDatasetSummary summary = toDshInDsSummary(dataShare, dshInDsList.getList());

                    ret.add(summary);
                }
            }
        }

        return ret;
    }

    private DataShareInDatasetSummary toDshInDsSummary(RangerDataShare dataShare, List<RangerDataShareInDataset> dshInDsList) {
        Optional<RangerDataShareInDataset> dshInDs = dshInDsList.stream().filter(d -> d.getDataShareId().equals(dataShare.getId())).findFirst();

        if (!dshInDs.isPresent()) {
            throw restErrorUtil.createRESTException("RequestStatus for DataShareInDataset not found", MessageEnums.DATA_NOT_FOUND, dataShare.getId(), "SharedResourceId", null, HttpStatus.SC_NOT_FOUND);
        }

        DataShareInDatasetSummary summary = new DataShareInDatasetSummary();

        summary.setId(dataShare.getId());
        summary.setCreatedBy(dataShare.getCreatedBy());
        summary.setCreateTime(dataShare.getCreateTime());
        summary.setUpdatedBy(dataShare.getUpdatedBy());
        summary.setUpdateTime(dataShare.getUpdateTime());
        summary.setGuid(dataShare.getGuid());
        summary.setIsEnabled(dataShare.getIsEnabled());
        summary.setVersion(dataShare.getVersion());

        summary.setName(dataShare.getName());
        summary.setServiceId(getServiceId(dataShare.getService()));
        summary.setServiceName(dataShare.getService());
        summary.setZoneId(getZoneId(dataShare.getZone()));
        summary.setZoneName(dataShare.getZone());
        summary.setShareStatus(dshInDs.get().getStatus());
        summary.setApprover(dshInDs.get().getApprover());
        summary.setResourceCount(sharedResourceService.getResourceCountForDataShare(dataShare.getId()));

        return summary;
    }

    private Long getServiceId(String serviceName) {
        XXService xService = daoMgr.getXXService().findByName(serviceName);

        if (xService == null) {
            throw restErrorUtil.createRESTException("Service not found", MessageEnums.DATA_NOT_FOUND, null, "ServiceName", null, HttpStatus.SC_NOT_FOUND);
        }

        return xService.getId();
    }

    private Long getZoneId(String zoneName) {
        Long ret = null;

        if (StringUtils.isNotBlank(zoneName)) {
            XXSecurityZone xxSecurityZone = daoMgr.getXXSecurityZoneDao().findByZoneName(zoneName);

            if (xxSecurityZone == null) {
                throw restErrorUtil.createRESTException("Security Zone not found", MessageEnums.DATA_NOT_FOUND, null, "ZoneName", null, HttpStatus.SC_NOT_FOUND);
            }

            ret = xxSecurityZone.getId();
        }

        return ret;
    }
}
