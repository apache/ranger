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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.db.RangerTransactionSynchronizationAdapter;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXGdsDataShareInDatasetDao;
import org.apache.ranger.db.XXGdsDatasetDao;
import org.apache.ranger.db.XXGdsDatasetInProjectDao;
import org.apache.ranger.db.XXGdsProjectDao;
import org.apache.ranger.entity.XXGdsDataShareInDataset;
import org.apache.ranger.entity.XXGdsDataset;
import org.apache.ranger.entity.XXGdsDatasetInProject;
import org.apache.ranger.entity.XXGdsProject;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShareInDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDatasetInProject;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.model.RangerGds.RangerSharedResource;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.store.AbstractGdsStore;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.service.RangerGdsDataShareService;
import org.apache.ranger.service.RangerGdsDataShareInDatasetService;
import org.apache.ranger.service.RangerGdsDatasetService;
import org.apache.ranger.service.RangerGdsDatasetInProjectService;
import org.apache.ranger.service.RangerGdsProjectService;
import org.apache.ranger.service.RangerGdsSharedResourceService;
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

@Component
public class GdsDBStore extends AbstractGdsStore {
    private static final Logger LOG = LoggerFactory.getLogger(GdsDBStore.class);

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


    @PostConstruct
    public void initStore() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> GdsInMemoryStore.initStore()");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== GdsInMemoryStore.initStore()");
        }
    }

    @Override
    public RangerDataset createDataset(RangerDataset dataset) throws Exception {
        LOG.debug("==> createDataset({})", dataset);

        validator.validateCreate(dataset);

        if (StringUtils.isBlank(dataset.getGuid())) {
            dataset.setGuid(guidUtil.genGUID());
        }

        RangerDataset ret = datasetService.create(dataset);

        datasetService.createObjectHistory(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_DATASET);

        LOG.debug("<== createDataset({}): ret={}", dataset, ret);

        return ret;
    }

    @Override
    public RangerDataset updateDataset(RangerDataset dataset) throws Exception {
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
    public void deleteDataset(Long datasetId) throws Exception {
        LOG.debug("==> deleteDataset({})", datasetId);

        RangerDataset existing = null;

        try {
            existing = datasetService.read(datasetId);
        } catch (Exception excp) {
            // ignore
        }

        validator.validateDelete(datasetId, existing);

        datasetService.delete(existing);

        datasetService.createObjectHistory(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

        updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_DATASET);

        LOG.debug("<== deleteDataset({})", datasetId);
    }

    @Override
    public RangerDataset getDataset(Long datasetId) throws Exception {
        LOG.debug("==> getDataset({})", datasetId);

        RangerDataset ret = datasetService.read(datasetId);

        // TODO: enforce RangerDataset.acl

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

        // TODO: enforce RangerDataset.acl

        LOG.debug("<== getDatasetByName({}): ret={}", name, ret);

        return ret;
    }

    @Override
    public PList<String> getDatasetNames(SearchFilter filter) throws Exception {
        LOG.debug("==> getDatasetNames({})", filter);

        RangerDatasetList result = datasetService.searchDatasets(filter);
        List<String>      names  = new ArrayList<>();

        for (RangerDataset dataset : result.getList()) {
            // TODO: enforce RangerDataset.acl

            names.add(dataset.getName());
        }

        PList<String> ret = new PList<>(names, 0, names.size(), names.size(), names.size(), result.getSortType(), result.getSortBy());

        LOG.debug("<== getDatasetNames({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public PList<RangerDataset> searchDatasets(SearchFilter filter) throws Exception {
        LOG.debug("==> searchDatasets({})", filter);

        RangerDatasetList   result   = datasetService.searchDatasets(filter);
        List<RangerDataset> datasets = new ArrayList<>();

        for (RangerDataset dataset : result.getList()) {
            // TODO: enforce RangerDataset.acl

            datasets.add(dataset);
        }

        PList<RangerDataset> ret = new PList<>(datasets, 0, datasets.size(), datasets.size(), datasets.size(), result.getSortBy(), result.getSortType());

        LOG.debug("<== searchDatasets({}): ret={}", filter, ret);

        return ret;
    }


    @Override
    public RangerProject createProject(RangerProject project) throws Exception {
        LOG.debug("==> createProject({})", project);

        validator.validateCreate(project);

        if (StringUtils.isBlank(project.getGuid())) {
            project.setGuid(guidUtil.genGUID());
        }

        RangerProject ret = projectService.create(project);

        projectService.createObjectHistory(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_PROJECT);

        LOG.debug("<== createProject({}): ret={}", project, ret);

        return ret;
    }

    @Override
    public RangerProject updateProject(RangerProject project) throws Exception {
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

        projectService.delete(existing);

        projectService.createObjectHistory(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

        updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_PROJECT);

        LOG.debug("<== deleteProject({})", projectId);
    }

    @Override
    public RangerProject getProject(Long projectId) throws Exception {
        LOG.debug("==> getProject({})", projectId);

        RangerProject ret = projectService.read(projectId);

        // TODO: enforce RangerProject.acl

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

        // TODO: enforce RangerProject.acl

        LOG.debug("<== getProjectByName({}): ret={}", name, ret);

        return ret;
    }

    @Override
    public PList<String> getProjectNames(SearchFilter filter) throws Exception {
        LOG.debug("==> getProjectNames({})", filter);

        RangerProjectList result = projectService.searchProjects(filter);
        List<String>      names  = new ArrayList<>();

        for (RangerProject project : result.getList()) {
            // TODO: enforce RangerProject.acl

            names.add(project.getName());
        }

        PList<String> ret = new PList<>(names, 0, names.size(), names.size(), names.size(), result.getSortType(), result.getSortBy());

        LOG.debug("<== getProjectNames({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public PList<RangerProject> searchProjects(SearchFilter filter) throws Exception {
        LOG.debug("==> searchProjects({})", filter);

        RangerProjectList   result   = projectService.searchProjects(filter);
        List<RangerProject> projects = new ArrayList<>();

        for (RangerProject project : result.getList()) {
            // TODO: enforce RangerProject.acl

            projects.add(project);
        }

        PList<RangerProject> ret = new PList<>(projects, 0, projects.size(), projects.size(), projects.size(), result.getSortBy(), result.getSortType());

        LOG.debug("<== searchProjects({}): ret={}", filter, ret);

        return ret;
    }


    @Override
    public RangerDataShare createDataShare(RangerDataShare dataShare) throws Exception {
        LOG.debug("==> createDataShare({})", dataShare);

        validator.validateCreate(dataShare);

        if (StringUtils.isBlank(dataShare.getGuid())) {
            dataShare.setGuid(guidUtil.genGUID());
        }

        RangerDataShare ret = dataShareService.create(dataShare);

        dataShareService.createObjectHistory(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_DATA_SHARE);

        LOG.debug("<== createDataShare({}): ret={}", dataShare, ret);

        return ret;
    }

    @Override
    public RangerDataShare updateDataShare(RangerDataShare dataShare) throws Exception {
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
    public void deleteDataShare(Long dataShareId) throws Exception {
        LOG.debug("==> deleteDataShare({})", dataShareId);

        RangerDataShare existing = null;

        try {
            existing = dataShareService.read(dataShareId);
        } catch (Exception excp) {
            // ignore
        }

        validator.validateDelete(dataShareId, existing);

        dataShareService.delete(existing);

        dataShareService.createObjectHistory(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

        updateGlobalVersion(RANGER_GLOBAL_STATE_NAME_DATA_SHARE);

        LOG.debug("<== deleteDataShare({})", dataShareId);
    }

    @Override
    public RangerDataShare getDataShare(Long dataShareId) throws Exception {
        LOG.debug("==> getDataShare({})", dataShareId);

        RangerDataShare ret = dataShareService.read(dataShareId);

        // TODO: enforce RangerDataShare.acl

        LOG.debug("<== getDataShare({}): ret={}", dataShareId, ret);

        return ret;
    }

    @Override
    public PList<RangerDataShare> searchDataShares(SearchFilter filter) throws Exception {
        LOG.debug("==> searchDataShares({})", filter);

        RangerDataShareList   result     = dataShareService.searchDataShares(filter);
        List<RangerDataShare> dataShares = new ArrayList<>();

        for (RangerDataShare dataShare : result.getList()) {
            // TODO: enforce RangerDataShare.acl

            dataShares.add(dataShare);
        }

        PList<RangerDataShare> ret = new PList<>(dataShares, 0, dataShares.size(), dataShares.size(), dataShares.size(), result.getSortBy(), result.getSortType());

        LOG.debug("<== searchDataShares({}): ret={}", filter, ret);

        return ret;
    }


    @Override
    public RangerSharedResource addSharedResource(RangerSharedResource resource) throws Exception {
        LOG.debug("==> addSharedResource({})", resource);

        validator.validateCreate(resource);

        // TODO: enforce RangerSharedResource.acl
        resource.setResourceSignature(RangerPolicyResourceSignature.toSignatureString(resource.getResource()));

        if (StringUtils.isBlank(resource.getGuid())) {
            resource.setGuid(guidUtil.genGUID());
        }

        RangerSharedResource ret = sharedResourceService.create(resource);

        sharedResourceService.createObjectHistory(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        LOG.debug("<== addSharedResource({}): ret={}", resource, ret);

        return ret;
    }

    @Override
    public RangerSharedResource updateSharedResource(RangerSharedResource resource) throws Exception {
        LOG.debug("==> updateSharedResource({})", resource);

        RangerSharedResource existing = null;

        try {
            existing = sharedResourceService.read(resource.getId());
        } catch (Exception excp) {
            // ignore
        }

        validator.validateUpdate(resource, existing);

        resource.setResourceSignature(RangerPolicyResourceSignature.toSignatureString(resource.getResource()));

        RangerSharedResource ret = sharedResourceService.update(resource);

        sharedResourceService.createObjectHistory(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        LOG.debug("<== updateSharedResource({}): ret={}", resource, ret);

        return ret;
    }

    @Override
    public void removeSharedResource(Long sharedResourceId) throws Exception {
        LOG.debug("==> removeSharedResource({})", sharedResourceId);


        RangerSharedResource existing = null;

        try {
            existing = sharedResourceService.read(sharedResourceId);
        } catch (Exception excp) {
            // ignore
        }

        validator.validateDelete(sharedResourceId, existing);

        sharedResourceService.delete(existing);

        sharedResourceService.createObjectHistory(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

        LOG.debug("<== removeSharedResource({})", sharedResourceId);
    }

    @Override
    public RangerSharedResource getSharedResource(Long sharedResourceId) throws Exception {
        LOG.debug("==> getSharedResource({})", sharedResourceId);

        RangerSharedResource ret = sharedResourceService.read(sharedResourceId);

        // TODO: enforce RangerSharedResource.acl

        LOG.debug("<== getSharedResource({}): ret={}", sharedResourceId, ret);

        return ret;
    }

    @Override
    public PList<RangerSharedResource> searchSharedResources(SearchFilter filter) throws Exception {
        LOG.debug("==> searchSharedResources({})", filter);

        RangerSharedResourceList   result          = sharedResourceService.searchSharedResources(filter);
        List<RangerSharedResource> sharedResources = new ArrayList<>();

        for (RangerSharedResource dataShare : result.getList()) {
            // TODO: enforce RangerSharedResource.acl

            sharedResources.add(dataShare);
        }

        PList<RangerSharedResource> ret = new PList<>(sharedResources, 0, sharedResources.size(), sharedResources.size(), sharedResources.size(), result.getSortBy(), result.getSortType());

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

        // TODO: enforce RangerDataShareInDataset.acl

        if (StringUtils.isBlank(dataShareInDataset.getGuid())) {
            dataShareInDataset.setGuid(guidUtil.genGUID());
        }

        RangerDataShareInDataset ret = dataShareInDatasetService.create(dataShareInDataset);

        dataShareInDatasetService.createObjectHistory(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        LOG.debug("<== addDataShareInDataset({}): ret={}", dataShareInDataset, ret);

        return ret;
    }

    @Override
    public RangerDataShareInDataset updateDataShareInDataset(RangerDataShareInDataset dataShareInDataset) throws Exception {
        LOG.debug("==> updateDataShareInDataset({})", dataShareInDataset);

        RangerDataShareInDataset existing = dataShareInDatasetService.read(dataShareInDataset.getId());

        // TODO: enforce RangerDataShareInDataset.acl

        RangerDataShareInDataset ret = dataShareInDatasetService.update(dataShareInDataset);

        dataShareInDatasetService.createObjectHistory(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        LOG.debug("<== updateDataShareInDataset({}): ret={}", dataShareInDataset, ret);

        return ret;
    }

    @Override
    public void removeDataShareInDataset(Long dataShareInDatasetId) throws Exception {
        LOG.debug("==> removeDataShareInDataset({})", dataShareInDatasetId);

        RangerDataShareInDataset existing = dataShareInDatasetService.read(dataShareInDatasetId);

        // TODO: enforce RangerDataShareInDataset.acl

        dataShareInDatasetService.delete(existing);

        dataShareInDatasetService.createObjectHistory(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

        LOG.debug("<== removeDataShareInDataset({})", dataShareInDatasetId);
    }

    @Override
    public RangerDataShareInDataset getDataShareInDataset(Long dataShareInDatasetId) throws Exception {
        LOG.debug("==> getDataShareInDataset({})", dataShareInDatasetId);

        RangerDataShareInDataset ret = dataShareInDatasetService.read(dataShareInDatasetId);

        LOG.debug("<== getDataShareInDataset({}): ret={}", dataShareInDatasetId, ret);

        return ret;
    }

    @Override
    public PList<RangerDataShareInDataset> searchDataShareInDatasets(SearchFilter filter) throws Exception {
        LOG.debug("==> searchDataShareInDatasets({})", filter);

        List<RangerDataShareInDataset> dataShareInDatasets = new ArrayList<>();
        RangerDataShareInDatasetList   result              = dataShareInDatasetService.searchDataShareInDatasets(filter);

        for (RangerDataShareInDataset dataShareInDataset : result.getList()) {
            // TODO: enforce RangerSharedResource.acl

            dataShareInDatasets.add(dataShareInDataset);
        }

        PList<RangerDataShareInDataset> ret = new PList<>(dataShareInDatasets, 0, dataShareInDatasets.size(), dataShareInDatasets.size(), dataShareInDatasets.size(), result.getSortBy(), result.getSortType());

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

        // TODO: enforce RangerDatasetInProject.acl

        if (StringUtils.isBlank(datasetInProject.getGuid())) {
            datasetInProject.setGuid(guidUtil.genGUID());
        }

        RangerDatasetInProject ret = datasetInProjectService.create(datasetInProject);

        datasetInProjectService.createObjectHistory(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        LOG.debug("<== addDatasetInProject({}): ret={}", datasetInProject, ret);

        return ret;
    }

    @Override
    public RangerDatasetInProject updateDatasetInProject(RangerDatasetInProject datasetInProject) throws Exception {
        LOG.debug("==> updateDatasetInProject({})", datasetInProject);

        RangerDatasetInProject existing = datasetInProjectService.read(datasetInProject.getId());

        // TODO: enforce RangerDatasetInProject.acl

        RangerDatasetInProject ret = datasetInProjectService.update(datasetInProject);

        datasetInProjectService.createObjectHistory(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        LOG.debug("<== updateDatasetInProject({}): ret={}", datasetInProject, ret);

        return ret;
    }

    @Override
    public void removeDatasetInProject(Long datasetInProjectId) throws Exception {
        LOG.debug("==> removeDatasetInProject({})", datasetInProjectId);

        RangerDatasetInProject existing = datasetInProjectService.read(datasetInProjectId);

        // TODO: enforce RangerDatasetInProject.acl

        datasetInProjectService.delete(existing);

        datasetInProjectService.createObjectHistory(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

        LOG.debug("<== removeDatasetInProject({})", datasetInProjectId);
    }

    @Override
    public RangerDatasetInProject getDatasetInProject(Long datasetInProjectId) throws Exception {
        LOG.debug("==> getDatasetInProject({})", datasetInProjectId);

        RangerDatasetInProject ret = datasetInProjectService.read(datasetInProjectId);

        // TODO: enforce RangerDatasetInProject.acl

        LOG.debug("<== getDatasetInProject({}): ret={}", datasetInProjectId, ret);

        return ret;
    }

    @Override
    public PList<RangerDatasetInProject> searchDatasetInProjects(SearchFilter filter) throws Exception {
        LOG.debug("==> searchDatasetInProjects({})", filter);

        List<RangerDatasetInProject> datasetInProjects = new ArrayList<>();
        RangerDatasetInProjectList   result            = datasetInProjectService.searchDatasetInProjects(filter);

        for (RangerDatasetInProject datasetInProject : result.getList()) {
            // TODO: enforce RangerDatasetInProject.acl

            datasetInProjects.add(datasetInProject);
        }

        PList<RangerDatasetInProject> ret = new PList<>(datasetInProjects, 0, datasetInProjects.size(), datasetInProjects.size(), datasetInProjects.size(), result.getSortBy(), result.getSortType());

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
}
