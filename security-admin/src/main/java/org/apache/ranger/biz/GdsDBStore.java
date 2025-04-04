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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.ranger.biz.ServiceDBStore.REMOVE_REF_TYPE;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.ServiceGdsInfoCache;
import org.apache.ranger.common.db.RangerTransactionSynchronizationAdapter;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXGdsDataShareDao;
import org.apache.ranger.db.XXGdsDataShareInDatasetDao;
import org.apache.ranger.db.XXGdsDatasetDao;
import org.apache.ranger.db.XXGdsDatasetInProjectDao;
import org.apache.ranger.db.XXGdsProjectDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.entity.XXGdsDataShare;
import org.apache.ranger.entity.XXGdsDataShareInDataset;
import org.apache.ranger.entity.XXGdsDataset;
import org.apache.ranger.entity.XXGdsDatasetInProject;
import org.apache.ranger.entity.XXGdsDatasetPolicyMap;
import org.apache.ranger.entity.XXGdsProject;
import org.apache.ranger.entity.XXGdsProjectPolicyMap;
import org.apache.ranger.entity.XXSecurityZone;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.plugin.model.RangerGds.DataShareInDatasetSummary;
import org.apache.ranger.plugin.model.RangerGds.DataShareSummary;
import org.apache.ranger.plugin.model.RangerGds.DatasetSummary;
import org.apache.ranger.plugin.model.RangerGds.DatasetsSummary;
import org.apache.ranger.plugin.model.RangerGds.GdsPermission;
import org.apache.ranger.plugin.model.RangerGds.GdsShareStatus;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShareInDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDatasetInProject;
import org.apache.ranger.plugin.model.RangerGds.RangerGdsBaseModelObject;
import org.apache.ranger.plugin.model.RangerGds.RangerGdsObjectACL;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.model.RangerGds.RangerSharedResource;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerPrincipal.PrincipalType;
import org.apache.ranger.plugin.model.RangerValiditySchedule;
import org.apache.ranger.plugin.model.validation.RangerValidityScheduleValidator;
import org.apache.ranger.plugin.model.validation.ValidationFailureDetails;
import org.apache.ranger.plugin.policyevaluator.RangerValidityScheduleEvaluator;
import org.apache.ranger.plugin.store.AbstractGdsStore;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.service.RangerGdsDataShareInDatasetService;
import org.apache.ranger.service.RangerGdsDataShareService;
import org.apache.ranger.service.RangerGdsDatasetInProjectService;
import org.apache.ranger.service.RangerGdsDatasetService;
import org.apache.ranger.service.RangerGdsProjectService;
import org.apache.ranger.service.RangerGdsSharedResourceService;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.validation.RangerGdsValidationDBProvider;
import org.apache.ranger.validation.RangerGdsValidator;
import org.apache.ranger.view.RangerGdsVList.RangerDataShareInDatasetList;
import org.apache.ranger.view.RangerGdsVList.RangerDataShareList;
import org.apache.ranger.view.RangerGdsVList.RangerDatasetInProjectList;
import org.apache.ranger.view.RangerGdsVList.RangerDatasetList;
import org.apache.ranger.view.RangerGdsVList.RangerProjectList;
import org.apache.ranger.view.RangerGdsVList.RangerSharedResourceList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.ranger.db.XXGlobalStateDao.RANGER_GLOBAL_STATE_NAME_GDS;
import static org.apache.ranger.plugin.policyevaluator.RangerValidityScheduleEvaluator.DATE_FORMATTER;
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
    public static final String GDS_POLICY_NAME_TIMESTAMP_SEP           = "@";

    public static final String LABELS                                  = "labelCounts";
    public static final String KEYWORDS                                = "keywordCounts";

    private static final Set<Integer> SHARE_STATUS_AGR = new HashSet<>(Arrays.asList(GdsShareStatus.ACTIVE.ordinal(), GdsShareStatus.GRANTED.ordinal(), GdsShareStatus.REQUESTED.ordinal()));

    @Autowired
    RangerGdsValidator validator;

    @Autowired
    RangerGdsValidationDBProvider validationDBProvider;

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
    RangerBizUtil bizUtil;

    @Autowired
    ServiceStore svcStore;

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    ServiceGdsInfoCache serviceGdsInfoCache;

    @Autowired
    GdsPolicyAdminCache gdsPolicyAdminCache;

    @PostConstruct
    public void initStore() {
        LOG.debug("==> GdsInMemoryStore.initStore()");

        LOG.debug("<== GdsInMemoryStore.initStore()");
    }

    @Override
    public RangerDataset createDataset(RangerDataset dataset) {
        LOG.debug("==> createDataset({})", dataset);

        dataset.setName(StringUtils.trim(dataset.getName()));

        validator.validateCreate(dataset);

        if (StringUtils.isBlank(dataset.getGuid())) {
            dataset.setGuid(guidUtil.genGUID());
        }

        if (dataset.getAcl() == null) {
            dataset.setAcl(new RangerGdsObjectACL());
        }

        addCreatorAsAclAdmin(dataset.getAcl());

        RangerDataset ret = datasetService.create(dataset);

        datasetService.onObjectChange(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        updateGdsVersion();

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

        dataset.setName(StringUtils.trim(dataset.getName()));

        validator.validateUpdate(dataset, existing);

        copyExistingBaseFields(dataset, existing);

        RangerDataset ret = datasetService.update(dataset);

        datasetService.onObjectChange(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        if (existing != null && !StringUtils.equals(dataset.getName(), existing.getName())) {
            List<RangerPolicy> policyList = getDatasetPolicies(dataset.getId());

            for (RangerPolicy policy : policyList) {
                updateDatasetNameInPolicy(dataset, policy);

                svcStore.updatePolicy(policy);
            }
        }

        updateGdsVersionForDataset(ret.getId());

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
            updateGdsVersionForDataset(existing.getId());

            if (forceDelete) {
                removeDSHIDForDataset(datasetId);
                removeDIPForDataset(datasetId);
            }

            deleteDatasetPolicies(existing);
            datasetService.delete(existing);

            datasetService.onObjectChange(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);
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

        if (filter.getParam(SearchFilter.CREATED_BY) != null) {
            setUserId(filter, SearchFilter.CREATED_BY);
        }

        PList<RangerDataset> ret           = getUnscrubbedDatasets(filter);
        GdsPermission        gdsPermission = getGdsPermissionFromFilter(filter);

        for (RangerDataset dataset : ret.getList()) {
            if (gdsPermission.equals(GdsPermission.LIST)) {
                scrubDatasetForListing(dataset);
            }
        }

        LOG.debug("<== searchDatasets({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public RangerProject createProject(RangerProject project) {
        LOG.debug("==> createProject({})", project);

        project.setName(StringUtils.trim(project.getName()));

        validator.validateCreate(project);

        if (StringUtils.isBlank(project.getGuid())) {
            project.setGuid(guidUtil.genGUID());
        }

        if (project.getAcl() == null) {
            project.setAcl(new RangerGdsObjectACL());
        }

        addCreatorAsAclAdmin(project.getAcl());

        RangerProject ret = projectService.create(project);

        projectService.onObjectChange(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        updateGdsVersion();

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

        project.setName(StringUtils.trim(project.getName()));

        validator.validateUpdate(project, existing);

        copyExistingBaseFields(project, existing);

        RangerProject ret = projectService.update(project);

        projectService.onObjectChange(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        if (existing != null && !StringUtils.equals(project.getName(), existing.getName())) {
            List<RangerPolicy> policyList = getProjectPolicies(project.getId());

            for (RangerPolicy policy : policyList) {
                updateProjectNameInPolicy(project, policy);

                svcStore.updatePolicy(policy);
            }
        }

        updateGdsVersionForProject(ret.getId());

        LOG.debug("<== updateProject({}): ret={}", project, ret);

        return ret;
    }

    @Override
    public void deleteProject(Long projectId, boolean forceDelete) throws Exception {
        LOG.debug("==> deleteProject({}, {})", projectId, forceDelete);

        RangerProject existing = null;

        try {
            existing = projectService.read(projectId);
        } catch (Exception excp) {
            // ignore
        }

        validator.validateDelete(projectId, existing);

        if (existing != null) {
            updateGdsVersionForProject(existing.getId());

            if (forceDelete) {
                removeDIPForProject(existing.getId());
            }

            deleteProjectPolicies(existing);
            projectService.delete(existing);

            projectService.onObjectChange(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);
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

        GdsPermission        gdsPermission = getGdsPermissionFromFilter(filter);
        PList<RangerProject> ret           = getUnscrubbedProjects(filter);

        for (RangerProject project : ret.getList()) {
            if (gdsPermission.equals(GdsPermission.LIST)) {
                scrubProjectForListing(project);
            }
        }

        LOG.debug("<== searchProjects({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public RangerDataShare createDataShare(RangerDataShare dataShare) {
        LOG.debug("==> createDataShare({})", dataShare);

        dataShare.setName(StringUtils.trim(dataShare.getName()));

        validator.validateCreate(dataShare);

        if (StringUtils.isBlank(dataShare.getGuid())) {
            dataShare.setGuid(guidUtil.genGUID());
        }

        if (dataShare.getAcl() == null) {
            dataShare.setAcl(new RangerGdsObjectACL());
        }

        addCreatorAsAclAdmin(dataShare.getAcl());

        RangerDataShare ret = dataShareService.create(dataShare);

        dataShareService.onObjectChange(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        updateGdsVersion();

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

        dataShare.setName(StringUtils.trim(dataShare.getName()));

        validator.validateUpdate(dataShare, existing);

        copyExistingBaseFields(dataShare, existing);

        RangerDataShare ret = dataShareService.update(dataShare);

        dataShareService.onObjectChange(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        updateGdsVersionForService(dataShare.getService());

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

        if (existing != null) {
            if (forceDelete) {
                removeDshInDsForDataShare(dataShareId);
                removeSharedResourcesForDataShare(dataShareId);
            }

            dataShareService.delete(existing);

            dataShareService.onObjectChange(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

            updateGdsVersionForService(existing.getService());
        }

        LOG.debug("<== deleteDataShare(dataShareId: {}, forceDelete: {})", dataShareId, forceDelete);
    }

    @Override
    public RangerDataShare getDataShare(Long dataShareId) throws Exception {
        LOG.debug("==> getDataShare({})", dataShareId);

        RangerDataShare ret = dataShareService.read(dataShareId);

        if (ret != null && !validator.hasPermission(ret.getAcl(), GdsPermission.VIEW)) {
            throw new Exception("no permission on dataShare id=" + dataShareId);
        }

        LOG.debug("<== getDataShare({}): ret={}", dataShareId, ret);

        return ret;
    }

    @Override
    public PList<RangerDataShare> searchDataShares(SearchFilter filter) {
        LOG.debug("==> searchDataShares({})", filter);

        PList<RangerDataShare> ret           = getUnscrubbedDataShares(filter);
        List<RangerDataShare>  dataShares    = ret.getList();
        GdsPermission          gdsPermission = getGdsPermissionFromFilter(filter);

        for (RangerDataShare dataShare : dataShares) {
            if (gdsPermission.equals(GdsPermission.LIST)) {
                scrubDataShareForListing(dataShare);
            }
        }

        LOG.debug("<== searchDataShares({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public List<RangerSharedResource> addSharedResources(List<RangerSharedResource> resources) {
        LOG.debug("==> addSharedResources({})", resources);

        List<RangerSharedResource> ret = new ArrayList<>();

        for (RangerSharedResource resource : resources) {
            resource.setName(StringUtils.trim(resource.getName()));

            validator.validateCreate(resource);

            if (StringUtils.isBlank(resource.getGuid())) {
                resource.setGuid(guidUtil.genGUID());
            }

            RangerSharedResource sharedResource = sharedResourceService.create(resource);

            ret.add(sharedResource);

            sharedResourceService.onObjectChange(sharedResource, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

            updateGdsVersionForDataShare(sharedResource.getDataShareId());
        }

        LOG.debug("<== addSharedResources({}): ret={}", resources, ret);

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

        resource.setName(StringUtils.trim(resource.getName()));

        validator.validateUpdate(resource, existing);

        copyExistingBaseFields(resource, existing);

        RangerSharedResource ret = sharedResourceService.update(resource);

        sharedResourceService.onObjectChange(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        updateGdsVersionForDataShare(ret.getDataShareId());

        LOG.debug("<== updateSharedResource({}): ret={}", resource, ret);

        return ret;
    }

    @Override
    public void removeSharedResources(List<Long> sharedResourceIds) {
        LOG.debug("==> removeSharedResources({})", sharedResourceIds);

        RangerSharedResource existing = null;

        for (Long sharedResourceId : sharedResourceIds) {
            try {
                existing = sharedResourceService.read(sharedResourceId);
            } catch (Exception excp) {
                // ignore
            }

            validator.validateDelete(sharedResourceId, existing);

            if (existing != null) {
                sharedResourceService.delete(existing);

                sharedResourceService.onObjectChange(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

                updateGdsVersionForDataShare(existing.getDataShareId());
            }
        }

        LOG.debug("<== removeSharedResources({})", sharedResourceIds);
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

        int maxRows    = filter.getMaxRows();
        int startIndex = filter.getStartIndex();

        final String resourceContains = filter.getParam(SearchFilter.RESOURCE_CONTAINS);

        filter.removeParam(SearchFilter.RESOURCE_CONTAINS);
        if (StringUtils.isNotEmpty(resourceContains)) {
            filter.setParam(SearchFilter.RETRIEVE_ALL_PAGES, "true");
        }

        RangerSharedResourceList   result          = sharedResourceService.searchSharedResources(filter);
        List<RangerSharedResource> sharedResources = new ArrayList<>();

        for (RangerSharedResource sharedResource : result.getList()) {
            // TODO: enforce RangerSharedResource.acl
            boolean includeResource = true;

            if (StringUtils.isNotEmpty(resourceContains)) {
                includeResource = false;

                if (sharedResource.getResource() != null) {
                    final Collection<RangerPolicyResource> resources = sharedResource.getResource().values();

                    if (CollectionUtils.isNotEmpty(resources)) {
                        includeResource = resources.stream().filter(Objects::nonNull)
                                .map(RangerPolicyResource::getValues).filter(Objects::nonNull)
                                .anyMatch(res -> hasResource(res, resourceContains));

                        if (!includeResource && sharedResource.getSubResource() != null && CollectionUtils.isNotEmpty(sharedResource.getSubResource().getValues())) {
                            includeResource = sharedResource.getSubResource().getValues().stream().filter(Objects::nonNull)
                                    .anyMatch(value -> value.contains(resourceContains));
                        }
                    }
                }
            }

            if (includeResource) {
                sharedResources.add(sharedResource);
            }
        }

        PList<RangerSharedResource> ret = getPList(sharedResources, startIndex, maxRows, result.getSortBy(), result.getSortType());

        LOG.debug("<== searchSharedResources({}): ret={}", filter, ret);

        return ret;
    }

    @Override
    public RangerDataShareInDataset addDataShareInDataset(RangerDataShareInDataset dataShareInDataset) throws Exception {
        LOG.debug("==> addDataShareInDataset({})", dataShareInDataset);

        validate(Collections.singletonList(dataShareInDataset));

        RangerDataShareInDataset ret = createDataShareInDataset(dataShareInDataset);

        LOG.debug("<== addDataShareInDataset({}): ret={}", dataShareInDataset, ret);

        return ret;
    }

    @Override
    public RangerDataShareInDataset updateDataShareInDataset(RangerDataShareInDataset dataShareInDataset) {
        LOG.debug("==> updateDataShareInDataset({})", dataShareInDataset);

        RangerDataShareInDataset existing = dataShareInDatasetService.read(dataShareInDataset.getId());

        validator.validateUpdate(dataShareInDataset, existing);

        copyExistingBaseFields(dataShareInDataset, existing);

        dataShareInDataset.setApprover(validator.needApproverUpdate(existing.getStatus(), dataShareInDataset.getStatus()) ? bizUtil.getCurrentUserLoginId() : existing.getApprover());

        RangerDataShareInDataset ret = dataShareInDatasetService.update(dataShareInDataset);

        dataShareInDatasetService.onObjectChange(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        updateGdsVersionForDataset(dataShareInDataset.getDatasetId());

        LOG.debug("<== updateDataShareInDataset({}): ret={}", dataShareInDataset, ret);

        return ret;
    }

    @Override
    public void removeDataShareInDataset(Long dataShareInDatasetId) {
        LOG.debug("==> removeDataShareInDataset({})", dataShareInDatasetId);

        RangerDataShareInDataset existing = dataShareInDatasetService.read(dataShareInDatasetId);

        validator.validateDelete(dataShareInDatasetId, existing);

        dataShareInDatasetService.delete(existing);

        dataShareInDatasetService.onObjectChange(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

        updateGdsVersionForDataset(existing.getDatasetId());

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

        int maxRows    = filter.getMaxRows();
        int startIndex = filter.getStartIndex();

        List<RangerDataShareInDataset> dataShareInDatasets = new ArrayList<>();
        RangerDataShareInDatasetList   result              = dataShareInDatasetService.searchDataShareInDatasets(filter);

        for (RangerDataShareInDataset dataShareInDataset : result.getList()) {
            // TODO: enforce RangerSharedResource.acl

            dataShareInDatasets.add(dataShareInDataset);
        }

        PList<RangerDataShareInDataset> ret = getPList(dataShareInDatasets, startIndex, maxRows, result.getSortBy(), result.getSortType());

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

        datasetInProjectService.onObjectChange(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);

        updateGdsVersionForDataset(datasetInProject.getDatasetId());

        LOG.debug("<== addDatasetInProject({}): ret={}", datasetInProject, ret);

        return ret;
    }

    @Override
    public RangerDatasetInProject updateDatasetInProject(RangerDatasetInProject datasetInProject) {
        LOG.debug("==> updateDatasetInProject({})", datasetInProject);

        RangerDatasetInProject existing = datasetInProjectService.read(datasetInProject.getId());

        validator.validateUpdate(datasetInProject, existing);

        copyExistingBaseFields(datasetInProject, existing);

        datasetInProject.setApprover(validator.needApproverUpdate(existing.getStatus(), datasetInProject.getStatus()) ? bizUtil.getCurrentUserLoginId() : existing.getApprover());

        RangerDatasetInProject ret = datasetInProjectService.update(datasetInProject);

        datasetInProjectService.onObjectChange(ret, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

        updateGdsVersionForDataset(datasetInProject.getDatasetId());

        LOG.debug("<== updateDatasetInProject({}): ret={}", datasetInProject, ret);

        return ret;
    }

    @Override
    public void removeDatasetInProject(Long datasetInProjectId) {
        LOG.debug("==> removeDatasetInProject({})", datasetInProjectId);

        RangerDatasetInProject existing = datasetInProjectService.read(datasetInProjectId);

        validator.validateDelete(datasetInProjectId, existing);

        datasetInProjectService.delete(existing);

        datasetInProjectService.onObjectChange(null, existing, RangerServiceService.OPERATION_DELETE_CONTEXT);

        updateGdsVersionForDataset(existing.getDatasetId());

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

        int maxRows    = filter.getMaxRows();
        int startIndex = filter.getStartIndex();

        List<RangerDatasetInProject> datasetInProjects = new ArrayList<>();
        RangerDatasetInProjectList   result            = datasetInProjectService.searchDatasetInProjects(filter);

        for (RangerDatasetInProject datasetInProject : result.getList()) {
            // TODO: enforce RangerDatasetInProject.acl

            datasetInProjects.add(datasetInProject);
        }

        PList<RangerDatasetInProject> ret = getPList(datasetInProjects, startIndex, maxRows, result.getSortBy(), result.getSortType());

        LOG.debug("<== searchDatasetInProjects({}): ret={}", filter, ret);

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

        updateGdsVersionForDataset(datasetId);

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

        updateGdsVersionForDataset(datasetId);

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

        updateGdsVersionForDataset(datasetId);

        LOG.debug("<== deleteDatasetPolicy({}, {})", datasetId, policyId);
    }

    @Override
    public void deleteDatasetPolicies(Long datasetId) throws Exception {
        LOG.debug("==> deleteDatasetPolicies({})", datasetId);

        RangerDataset dataset = datasetService.read(datasetId);

        deleteDatasetPolicies(dataset);

        updateGdsVersionForDataset(datasetId);

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
    public List<RangerPolicy> getDatasetPolicies(Long datasetId) {
        LOG.debug("==> getDatasetPolicies({})", datasetId);

        RangerDataset dataset = datasetService.read(datasetId);

        if (!validator.hasPermission(dataset.getAcl(), GdsPermission.AUDIT)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_TO_VIEW_DATASET_POLICIES);
        }

        List<RangerPolicy> ret = getPolicies(daoMgr.getXXGdsDatasetPolicyMap().getDatasetPolicyIds(datasetId));

        LOG.debug("<== getDatasetPolicies({}): ret={}", datasetId, ret);

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

        updateGdsVersionForProject(project.getId());

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

        updateGdsVersionForProject(project.getId());

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

        updateGdsVersionForProject(project.getId());

        LOG.debug("<== deleteProjectPolicy({}, {})", projectId, policyId);
    }

    @Override
    public void deleteProjectPolicies(Long projectId) throws Exception {
        LOG.debug("==> deleteProjectPolicies({})", projectId);

        RangerProject project = projectService.read(projectId);

        deleteProjectPolicies(project);

        updateGdsVersionForProject(project.getId());

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
    public List<RangerPolicy> getProjectPolicies(Long projectId) {
        LOG.debug("==> getProjectPolicies({})", projectId);

        RangerProject project = projectService.read(projectId);

        if (!validator.hasPermission(project.getAcl(), GdsPermission.AUDIT)) {
            throw restErrorUtil.create403RESTException(NOT_AUTHORIZED_TO_VIEW_PROJECT_POLICIES);
        }

        List<RangerPolicy> ret = getPolicies(daoMgr.getXXGdsProjectPolicyMap().getProjectPolicyIds(projectId));

        LOG.debug("<== getProjectPolicies({}): ret={}", projectId, ret);

        return ret;
    }

    @Override
    public void deleteAllGdsObjectsForService(Long serviceId) {
        LOG.debug("==> deleteAllGdsObjectsForService({})", serviceId);

        List<XXGdsDataShare> dataShares = daoMgr.getXXGdsDataShare().findByServiceId(serviceId);

        if (CollectionUtils.isNotEmpty(dataShares)) {
            LOG.info("Deleting {} dataShares associated with service id={}", dataShares.size(), serviceId);

            dataShares.forEach(dataShare -> {
                LOG.info("Deleting dataShare id={}, name={}", dataShare.getId(), dataShare.getName());

                deleteDataShare(dataShare.getId(), true);
            });
        }

        LOG.debug("<== deleteAllGdsObjectsForService({})", serviceId);
    }

    @Override
    public void deleteAllGdsObjectsForSecurityZone(Long zoneId) {
        LOG.debug("==> deleteAllGdsObjectsForSecurityZone({})", zoneId);

        List<XXGdsDataShare> dataShares = daoMgr.getXXGdsDataShare().findByZoneId(zoneId);

        if (CollectionUtils.isNotEmpty(dataShares)) {
            LOG.info("Deleting {} dataShares associated with securityZone id={}", dataShares.size(), zoneId);

            dataShares.forEach(dataShare -> {
                LOG.info("Deleting dataShare id={}, name={}", dataShare.getId(), dataShare.getName());

                deleteDataShare(dataShare.getId(), true);
            });
        }

        LOG.debug("<== deleteAllGdsObjectsForSecurityZone({})", zoneId);
    }

    @Override
    public void onSecurityZoneUpdate(Long zoneId, Collection<String> updatedServices, Collection<String> removedServices) {
        LOG.debug("==> onSecurityZoneUpdate({}, {}, {})", zoneId, updatedServices, removedServices);

        XXServiceDao      serviceDao   = daoMgr.getXXService();
        XXGdsDataShareDao dataShareDao = daoMgr.getXXGdsDataShare();

        if (zoneId != null && CollectionUtils.isNotEmpty(updatedServices)) {
            for (String serviceName : updatedServices) {
                Long serviceId = serviceDao.findIdByName(serviceName);

                if (serviceId == null) {
                    LOG.warn("onSecurityZoneUpdate(): updatedServices invalid service name={}. Ignored", serviceName);
                    continue;
                }

                List<XXGdsDataShare> dataShares = dataShareDao.findByServiceIdAndZoneId(serviceId, zoneId);

                if (CollectionUtils.isEmpty(dataShares)) {
                    continue;
                }

                updateGdsVersionForService(serviceId);
            }
        }

        if (zoneId != null && CollectionUtils.isNotEmpty(removedServices)) {
            for (String serviceName : removedServices) {
                Long serviceId = serviceDao.findIdByName(serviceName);

                if (serviceId == null) {
                    LOG.warn("onSecurityZoneUpdate(): removedServices invalid service name={}. Ignored", serviceName);
                    continue;
                }

                List<XXGdsDataShare> dataShares = dataShareDao.findByServiceIdAndZoneId(serviceId, zoneId);

                if (CollectionUtils.isEmpty(dataShares)) {
                    continue;
                }

                LOG.info("Deleting {} dataShares associated with service(name={}) in securityZone(id={})", dataShares.size(), serviceName, zoneId);

                dataShares.forEach(dataShare -> {
                    LOG.info("Deleting dataShare id={}, name={}", dataShare.getId(), dataShare.getName());

                    deleteDataShare(dataShare.getId(), true);
                });
            }
        }

        LOG.debug("<== onSecurityZoneUpdate({}, {}, {})", zoneId, updatedServices, removedServices);
    }

    public List<RangerDataShareInDataset> addDataSharesInDataset(List<RangerDataShareInDataset> dataSharesInDataset) throws Exception {
        LOG.debug("==> addDataSharesInDataset({})", dataSharesInDataset);

        List<RangerDataShareInDataset> ret = new ArrayList<>();

        validate(dataSharesInDataset);

        for (RangerDataShareInDataset dataShareInDataset : dataSharesInDataset) {
            ret.add(createDataShareInDataset(dataShareInDataset));
        }

        LOG.debug("<== addDataSharesInDataset({}): ret={}", dataSharesInDataset, ret);

        return ret;
    }

    public ServiceGdsInfo getGdsInfoIfUpdated(String serviceName, Long lastKnownVersion) {
        LOG.debug("==> GdsDBStore.getGdsInfoIfUpdated({}, {})", serviceName, lastKnownVersion);

        ServiceGdsInfo latest        = serviceGdsInfoCache.get(serviceName);
        Long           latestVersion = latest != null ? latest.getGdsVersion() : null;
        ServiceGdsInfo ret           = (lastKnownVersion == null || lastKnownVersion == -1 || !lastKnownVersion.equals(latestVersion)) ? latest : null;

        LOG.debug("<== GdsDBStore.getGdsInfoIfUpdated({}, {}): ret={}", serviceName, lastKnownVersion, ret);

        return ret;
    }

    public PList<DatasetSummary> getDatasetSummary(SearchFilter filter) {
        return getDatasetSummary(filter, false);
    }

    public DatasetsSummary getEnhancedDatasetSummary(SearchFilter filter) {
        return getDatasetSummary(filter, true);
    }

    public DatasetsSummary getDatasetSummary(SearchFilter filter, boolean includeAdditionalInfo) {
        LOG.debug("==> getDatasetSummary({}, {})", filter, includeAdditionalInfo);

        PList<RangerDataset>              datasets;
        Map<String, Map<String, Integer>> additionalInfo = null;

        if (includeAdditionalInfo) {
            List<RangerDataset> datasetsMatchingCriteria = fetchDatasetsBySearchCriteria(filter);
            additionalInfo = buildAdditionalInfoForDatasets(datasetsMatchingCriteria);
            datasets       = applyPaginataionAndSorting(datasetsMatchingCriteria, filter);
        } else {
            datasets       = getUnscrubbedDatasets(filter);
        }

        List<DatasetSummary>  datasetSummary          = toDatasetSummary(datasets.getList(), getGdsPermissionFromFilter(filter));
        PList<DatasetSummary> paginatedDatasetSummary = createdPaginatedDatasetSummary(datasets, datasetSummary);
        DatasetsSummary       ret                     = new DatasetsSummary(paginatedDatasetSummary, additionalInfo);

        LOG.debug("<== getDatasetSummary({}, {}): ret={}", filter, includeAdditionalInfo, ret);

        return ret;
    }

    private Map<String, Map<String, Integer>> buildAdditionalInfoForDatasets(List<RangerDataset> datasets) {
        Map<String, Map<String, Integer>> additionalInfo = new HashMap<>();
        for (RangerDataset dataset : datasets) {
            updateAdditionalInfo(LABELS, dataset.getLabels(), additionalInfo);
            updateAdditionalInfo(KEYWORDS, dataset.getKeywords(), additionalInfo);
        }
        return additionalInfo;
    }

    private void updateAdditionalInfo(String field, List<String> fieldValues, Map<String, Map<String, Integer>> additionalInfo) {
        if (CollectionUtils.isNotEmpty(fieldValues)) {
            Map<String, Integer> aggregatedFieldMap = additionalInfo.computeIfAbsent(field, key -> new HashMap<>());
            for (String value : fieldValues) {
                aggregatedFieldMap.put(value, aggregatedFieldMap.getOrDefault(value, 0) + 1);
            }
        }
    }

    private PList<DatasetSummary> createdPaginatedDatasetSummary(PList<RangerDataset> datasets, List<DatasetSummary> datasetSummary) {
        PList<DatasetSummary> paginatedDatasetSummary = new PList<>(
                datasetSummary,
                datasets.getStartIndex(),
                datasets.getPageSize(),
                datasets.getTotalCount(),
                datasets.getResultSize(),
                datasets.getSortType(),
                datasets.getSortBy());

        paginatedDatasetSummary.setQueryTimeMS(datasets.getQueryTimeMS());
        return paginatedDatasetSummary;
    }

    public PList<DataShareSummary> getDataShareSummary(SearchFilter filter) {
        LOG.debug("==> getDataShareSummary({})", filter);

        PList<RangerDataShare>  dataShares       = getUnscrubbedDataShares(filter);
        List<DataShareSummary>  dataShareSummary = toDataShareSummary(dataShares.getList(), getGdsPermissionFromFilter(filter));
        PList<DataShareSummary> ret              = new PList<>(dataShareSummary, dataShares.getStartIndex(), dataShares.getPageSize(), dataShares.getTotalCount(), dataShares.getResultSize(), dataShares.getSortType(), dataShares.getSortBy());

        ret.setQueryTimeMS(dataShares.getQueryTimeMS());

        LOG.debug("<== getDataShareSummary({}): ret={}", filter, ret);

        return ret;
    }

    public PList<DataShareInDatasetSummary> getDshInDsSummary(SearchFilter filter) {
        LOG.debug("==> getDshInDsSummary({})", filter);

        int maxRows    = filter.getMaxRows();
        int startIndex = filter.getStartIndex();

        filter.setParam(SearchFilter.GDS_PERMISSION, GdsPermission.ADMIN.name());

        if (filter.getParam(SearchFilter.CREATED_BY) != null) {
            setUserId(filter, SearchFilter.CREATED_BY);
        }

        if (filter.getParam(SearchFilter.APPROVER) != null) {
            setUserId(filter, SearchFilter.APPROVER);
        }

        if (filter.getParam(SearchFilter.SHARE_STATUS) != null) {
            String shareStatus = filter.getParam(SearchFilter.SHARE_STATUS);
            int    status      = GdsShareStatus.valueOf(shareStatus).ordinal();

            filter.setParam(SearchFilter.SHARE_STATUS, Integer.toString(status));
        }

        List<RangerDataset>             datasets       = getUnscrubbedDatasets(filter).getList();
        List<RangerDataShare>           dataShares     = getUnscrubbedDataShares(filter).getList();
        RangerDataShareInDatasetList    dshInDsList    = dataShareInDatasetService.searchDataShareInDatasets(filter);
        List<DataShareInDatasetSummary> dshInDsSummary = getDshInDsSummary(dataShares, datasets, dshInDsList);

        PList<DataShareInDatasetSummary> ret = getPList(dshInDsSummary, startIndex, maxRows, filter.getSortBy(), filter.getSortType());

        LOG.debug("<== getDshInDsSummary({}): ret={}", filter, ret);

        return ret;
    }

    public void deletePrincipalFromGdsAcl(String principalType, String principalName) {
        Map<Long, RangerGdsObjectACL> datsetAcls    = daoMgr.getXXGdsDataset().getDatasetIdsAndACLs();
        Map<Long, RangerGdsObjectACL> dataShareAcls = daoMgr.getXXGdsDataShare().getDataShareIdsAndACLs();
        Map<Long, RangerGdsObjectACL> projectAcls   = daoMgr.getXXGdsProject().getProjectIdsAndACLs();

        for (Map.Entry<Long, RangerGdsObjectACL> entry : datsetAcls.entrySet()) {
            Long               id  = entry.getKey();
            RangerGdsObjectACL acl = entry.getValue();

            if (deletePrincipalFromAcl(acl, principalName, principalType) != null) {
                RangerDataset dataset = datasetService.read(id);

                dataset.setAcl(acl);
                datasetService.update(dataset);
            }
        }

        for (Map.Entry<Long, RangerGdsObjectACL> entry : dataShareAcls.entrySet()) {
            Long               id  = entry.getKey();
            RangerGdsObjectACL acl = entry.getValue();

            if (deletePrincipalFromAcl(acl, principalName, principalType) != null) {
                RangerDataShare dataShare = dataShareService.read(id);

                dataShare.setAcl(acl);
                dataShareService.update(dataShare);
            }
        }

        for (Map.Entry<Long, RangerGdsObjectACL> entry : projectAcls.entrySet()) {
            Long               id  = entry.getKey();
            RangerGdsObjectACL acl = entry.getValue();

            if (deletePrincipalFromAcl(acl, principalName, principalType) != null) {
                RangerProject project = projectService.read(id);

                project.setAcl(acl);
                projectService.update(project);
            }
        }
    }

    private List<DataShareInDatasetSummary> getDshInDsSummary(List<RangerDataShare> dataShares, List<RangerDataset> datasets, RangerDataShareInDatasetList dshInDsList) {
        Set<DataShareInDatasetSummary> ret          = new LinkedHashSet<>();
        Map<Long, RangerDataset>       datasetMap   = toMap(datasets);
        Map<Long, RangerDataShare>     dataShareMap = toMap(dataShares);

        for (RangerDataShareInDataset dshInDs : dshInDsList.getList()) {
            RangerDataset   dataset   = datasetMap.get(dshInDs.getDatasetId());
            RangerDataShare dataShare = dataShareMap.get(dshInDs.getDataShareId());

            if (dataset != null || dataShare != null) {
                if (dataset == null) {
                    dataset = datasetService.read(dshInDs.getDatasetId());
                } else if (dataShare == null) {
                    dataShare = dataShareService.read(dshInDs.getDataShareId());
                }

                ret.add(toDshInDsSummary(dataset, dataShare, dshInDs));
            }
        }

        return Collections.unmodifiableList(new ArrayList<>(ret));
    }

    private <T extends RangerGdsBaseModelObject> Map<Long, T> toMap(List<T> gdsObjects) {
        return gdsObjects.stream().collect(Collectors.toMap(RangerGdsBaseModelObject::getId, Function.identity()));
    }

    private void updateGdsVersion() {
        transactionSynchronizationAdapter.executeOnTransactionCommit(new GlobalVersionUpdater(daoMgr, RANGER_GLOBAL_STATE_NAME_GDS));
    }

    private void setUserId(SearchFilter filter, String filterParam) {
        String userName = filter.getParam(filterParam);
        Long   userId   = daoMgr.getXXPortalUser().findByLoginId(userName).getId();
        filter.setParam(filterParam, Long.toString(userId));
    }

    private List<DatasetSummary> toDatasetSummary(List<RangerDataset> datasets, GdsPermission gdsPermission) {
        List<DatasetSummary> ret         = new ArrayList<>();
        String               currentUser = bizUtil.getCurrentUserLoginId();

        for (RangerDataset dataset : datasets) {
            GdsPermission permissionForCaller = validator.getGdsPermissionForUser(dataset.getAcl(), currentUser);

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
            datasetSummary.setValiditySchedule(dataset.getValiditySchedule());
            datasetSummary.setLabels(dataset.getLabels());
            datasetSummary.setKeywords(dataset.getKeywords());

            ret.add(datasetSummary);

            if (gdsPermission.equals(GdsPermission.LIST)) {
                continue;
            }

            datasetSummary.setProjectsCount(getDIPCountForDataset(dataset.getId()));
            datasetSummary.setPrincipalsCount(getPrincipalCountForDataset(dataset));
            datasetSummary.setAclPrincipalsCount(getAclPrincipalCountForDataset(dataset));

            SearchFilter                    filter            = new SearchFilter(SearchFilter.DATASET_ID, dataset.getId().toString());
            RangerDataShareList             dataShares        = dataShareService.searchDataShares(filter);
            List<DataShareInDatasetSummary> dataSharesSummary = getDataSharesSummary(dataShares, filter);

            datasetSummary.setDataShares(dataSharesSummary);
            datasetSummary.setTotalResourceCount(dataSharesSummary.stream()
                    .map(DataShareInDatasetSummary::getResourceCount)
                    .mapToLong(Long::longValue)
                    .sum());
        }

        return ret;
    }

    private List<DataShareSummary> toDataShareSummary(List<RangerDataShare> dataShares, GdsPermission gdsPermission) {
        List<DataShareSummary> ret         = new ArrayList<>();
        String                 currentUser = bizUtil.getCurrentUserLoginId();
        Map<String, Long>      zoneIds     = new HashMap<>();

        for (RangerDataShare dataShare : dataShares) {
            GdsPermission permissionForCaller = validator.getGdsPermissionForUser(dataShare.getAcl(), currentUser);

            if (permissionForCaller.equals(GdsPermission.NONE)) {
                continue;
            }

            DataShareSummary dataShareSummary = new DataShareSummary();

            dataShareSummary.setId(dataShare.getId());
            dataShareSummary.setName(dataShare.getName());
            dataShareSummary.setDescription(dataShare.getDescription());
            dataShareSummary.setCreateTime(dataShare.getCreateTime());
            dataShareSummary.setUpdateTime(dataShare.getUpdateTime());
            dataShareSummary.setCreatedBy(dataShare.getCreatedBy());
            dataShareSummary.setUpdatedBy(dataShare.getUpdatedBy());
            dataShareSummary.setIsEnabled(dataShare.getIsEnabled());
            dataShareSummary.setGuid(dataShare.getGuid());
            dataShareSummary.setVersion(dataShare.getVersion());
            dataShareSummary.setPermissionForCaller(permissionForCaller);

            dataShareSummary.setZoneName(dataShare.getZone());
            dataShareSummary.setZoneId(getZoneId(dataShare.getZone(), zoneIds));

            dataShareSummary.setServiceName(dataShare.getService());
            dataShareSummary.setServiceId(getServiceId(dataShare.getService()));
            dataShareSummary.setServiceType(getServiceType(dataShare.getService()));

            if (!gdsPermission.equals(GdsPermission.LIST)) {
                SearchFilter                    filter          = new SearchFilter(SearchFilter.DATA_SHARE_ID, dataShare.getId().toString());
                RangerDatasetList               datasets        = datasetService.searchDatasets(filter);
                List<DataShareInDatasetSummary> datasetsSummary = getDatasetsSummary(datasets, filter);

                dataShareSummary.setDatasets(datasetsSummary);
                dataShareSummary.setResourceCount(sharedResourceService.getResourceCountForDataShare(dataShare.getId()));
            }

            ret.add(dataShareSummary);
        }

        return ret;
    }

    private Long getDIPCountForDataset(Long datasetId) {
        return datasetInProjectService.getDatasetsInProjectCount(datasetId);
    }

    private Map<PrincipalType, Integer> getPrincipalCountForDataset(RangerDataset dataset) {
        Map<PrincipalType, Integer> ret    = new HashMap<>();
        Set<String>                 users  = Collections.emptySet();
        Set<String>                 groups = Collections.emptySet();
        Set<String>                 roles  = Collections.emptySet();

        if (validator.hasPermission(dataset.getAcl(), GdsPermission.AUDIT)) {
            users  = new HashSet<>();
            groups = new HashSet<>();
            roles  = new HashSet<>();

            for (RangerPolicy policy : getDatasetPolicies(dataset.getId())) {
                for (RangerPolicyItem policyItem : policy.getPolicyItems()) {
                    users.addAll(policyItem.getUsers());
                    groups.addAll(policyItem.getGroups());
                    roles.addAll(policyItem.getRoles());
                }
            }
        }

        ret.put(PrincipalType.USER, users.size());
        ret.put(PrincipalType.GROUP, groups.size());
        ret.put(PrincipalType.ROLE, roles.size());

        return ret;
    }

    private Map<PrincipalType, Integer> getAclPrincipalCountForDataset(RangerDataset dataset) {
        Map<PrincipalType, Integer> ret = new HashMap<>();

        ret.put(PrincipalType.USER, 0);
        ret.put(PrincipalType.GROUP, 0);
        ret.put(PrincipalType.ROLE, 0);

        RangerGdsObjectACL acl = dataset.getAcl();

        if (acl != null) {
            if (acl.getUsers() != null) {
                ret.put(PrincipalType.USER, acl.getUsers().size());
            }

            if (acl.getGroups() != null) {
                ret.put(PrincipalType.GROUP, acl.getGroups().size());
            }

            if (acl.getRoles() != null) {
                ret.put(PrincipalType.ROLE, acl.getRoles().size());
            }
        }

        return ret;
    }

    private PList<RangerProject> getUnscrubbedProjects(SearchFilter filter) {
        filter.setParam(SearchFilter.RETRIEVE_ALL_PAGES, "true");

        GdsPermission       gdsPermission  = getGdsPermissionFromFilter(filter);
        RangerProjectList   result         = projectService.searchProjects(filter);
        List<RangerProject> projects       = new ArrayList<>();
        boolean             isSharedWithMe = Boolean.parseBoolean(filter.getParam(SearchFilter.SHARED_WITH_ME));
        String              userName       = bizUtil.getCurrentUserLoginId();
        Collection<String>  groups         = null;
        Collection<String>  roles          = null;

        if (isSharedWithMe) {
            groups = validationDBProvider.getGroupsForUser(userName);
            roles  = validationDBProvider.getRolesForUserAndGroups(userName, groups);
        }

        for (RangerProject project : result.getList()) {
            if (project == null) {
                continue;
            }

            if (isSharedWithMe) {
                if (gdsPolicyAdminCache.isProjectSharedWith(project.getId(), userName, groups, roles)) {
                    projects.add(project);
                }
            } else if (validator.hasPermission(project.getAcl(), gdsPermission)) {
                projects.add(project);
            }
        }

        return getPList(projects, filter.getStartIndex(), filter.getMaxRows(), result.getSortBy(), result.getSortType());
    }

    private PList<RangerDataset> getUnscrubbedDatasets(SearchFilter filter) {
        List<RangerDataset> datasets = fetchDatasetsBySearchCriteria(filter);

        return applyPaginataionAndSorting(datasets, filter);
    }

    private List<RangerDataset> fetchDatasetsBySearchCriteria(SearchFilter filter) {
        filter.setParam(SearchFilter.RETRIEVE_ALL_PAGES, "true");

        GdsPermission       gdsPermission  = getGdsPermissionFromFilter(filter);
        RangerDatasetList   result         = datasetService.searchDatasets(filter);
        List<RangerDataset> datasets       = new ArrayList<>();
        boolean             isSharedWithMe = Boolean.parseBoolean(filter.getParam(SearchFilter.SHARED_WITH_ME));
        String              userName       = bizUtil.getCurrentUserLoginId();
        Collection<String>  groups         = null;
        Collection<String>  roles          = null;

        if (isSharedWithMe) {
            groups = validationDBProvider.getGroupsForUser(userName);
            roles  = validationDBProvider.getRolesForUserAndGroups(userName, groups);
        }

        for (RangerDataset dataset : result.getList()) {
            if (dataset == null) {
                continue;
            }

            if (isSharedWithMe) {
                if (gdsPolicyAdminCache.isDatasetSharedWith(dataset.getId(), userName, groups, roles)) {
                    datasets.add(dataset);
                }
            } else if (validator.hasPermission(dataset.getAcl(), gdsPermission)) {
                datasets.add(dataset);
            }
        }

        filterDatasetsByValidityExpiration(filter, datasets);

        return datasets;
    }

    public void filterDatasetsByValidityExpiration(SearchFilter filter, List<RangerDataset> datasets) {
        LOG.debug("==> filterDatasetsByValidityExpiration({}, {})", filter, datasets);
        String                         validityCheckStart           = filter.getParam(SearchFilter.VALIDITY_EXPIRY_START);
        String                         validityCheckEnd             = filter.getParam(SearchFilter.VALIDITY_EXPIRY_END);
        String                         validityTimeZone             = filter.getParam(SearchFilter.VALIDITY_TIME_ZONE);
        RangerValiditySchedule         validityCheckFilter          = new RangerValiditySchedule(validityCheckStart, validityCheckEnd, validityTimeZone, null);
        List<ValidationFailureDetails> failures                     = new ArrayList<>();
        RangerValiditySchedule         validatedValidityCheckFilter = validateValidityCheckFilter(validityCheckFilter, failures);

        if (validatedValidityCheckFilter != null) {
            RangerValidityScheduleEvaluator validityScheduleEvaluator = new RangerValidityScheduleEvaluator(validatedValidityCheckFilter);
            datasets.removeIf(dataset -> !isDatasetExpiring(dataset, validityScheduleEvaluator, failures));
        }

        if (CollectionUtils.isNotEmpty(failures)) {
            throw restErrorUtil.createRESTException("Error in finding datasets expiring between '" + validityCheckStart + "' and '" + validityCheckEnd + "': " + failures);
        }
        LOG.debug("==> filterDatasetsByValidityExpiration({}, {})", filter, datasets);
    }

    private boolean isDatasetExpiring(RangerDataset dataset, RangerValidityScheduleEvaluator validityScheduleEvaluator, List<ValidationFailureDetails> failures) {
        if (dataset.getValiditySchedule() == null) {
            return false;
        }
        String datasetValidityScheduleEndTime = dataset.getValiditySchedule().getEndTime();
        if (StringUtils.isEmpty(datasetValidityScheduleEndTime)) {
            return false;
        }

        try {
            Date datasetExpiryTime = DATE_FORMATTER.get().parse(datasetValidityScheduleEndTime);
            return validityScheduleEvaluator.isApplicable(datasetExpiryTime.getTime());
        } catch (ParseException pe) {
            failures.add(new ValidationFailureDetails(0, "endTime", "", false, true, false, "Error parsing endTime:" + datasetValidityScheduleEndTime));
        }
        return false;
    }

    private RangerValiditySchedule validateValidityCheckFilter(RangerValiditySchedule validityCheckFilter, List<ValidationFailureDetails> failures) {
        String startTime = validityCheckFilter.getStartTime();
        String endTime   = validityCheckFilter.getEndTime();
        String timeZone  = validityCheckFilter.getTimeZone();

        if (StringUtils.isEmpty(startTime) && StringUtils.isEmpty(endTime)) {
            return null;
        }

        if (StringUtils.isEmpty(startTime) || StringUtils.isEmpty(endTime)) {
            failures.add(new ValidationFailureDetails(0, "startTime,endTime", "", true, true, false, "empty values"));
            return null;
        }

        if (StringUtils.isEmpty(timeZone)) {
            validityCheckFilter.setTimeZone(SearchFilter.DEFAULT_TIME_ZONE);
        }

        RangerValidityScheduleValidator validator = new RangerValidityScheduleValidator(validityCheckFilter);

        return validator.validate(failures);
    }

    private PList<RangerDataset> applyPaginataionAndSorting(List<RangerDataset> datasets, SearchFilter filter) {
        int maxRows    = filter.getMaxRows();
        int startIndex = filter.getStartIndex();

        return getPList(datasets, startIndex, maxRows, filter.getSortBy(), filter.getSortType());
    }

    private PList<RangerDataShare> getUnscrubbedDataShares(SearchFilter filter) {
        filter.setParam(SearchFilter.RETRIEVE_ALL_PAGES, "true");

        String     datasetId           = filter.getParam(SearchFilter.DATASET_ID);
        boolean    excludeDatasetId    = Boolean.parseBoolean(filter.getParam(SearchFilter.EXCLUDE_DATASET_ID));
        List<Long> dataSharesToExclude = null;

        if (excludeDatasetId) {
            filter.removeParam(SearchFilter.DATASET_ID);

            dataSharesToExclude = daoMgr.getXXGdsDataShareInDataset().findDataShareIdsInStatuses(Long.parseLong(datasetId), SHARE_STATUS_AGR);
        }

        if (dataSharesToExclude == null) {
            dataSharesToExclude = Collections.emptyList();
        }

        GdsPermission         gdsPermission = getGdsPermissionFromFilter(filter);
        RangerDataShareList   result        = dataShareService.searchDataShares(filter);
        List<RangerDataShare> dataShares    = new ArrayList<>();

        for (RangerDataShare dataShare : result.getList()) {
            if (dataShare == null) {
                continue;
            }

            if (validator.hasPermission(dataShare.getAcl(), gdsPermission)) {
                if (!dataSharesToExclude.contains(dataShare.getId())) {
                    dataShares.add(dataShare);
                }
            }
        }

        return getPList(dataShares, filter.getStartIndex(), filter.getMaxRows(), result.getSortBy(), result.getSortType());
    }

    private <T> PList<T> getPList(List<T> list, int startIndex, int maxEntries, String sortBy, String sortType) {
        List<T> subList = startIndex < list.size() ? list.subList(startIndex, Math.min(startIndex + maxEntries, list.size())) : Collections.emptyList();

        return new PList<>(subList, startIndex, maxEntries, list.size(), subList.size(), sortType, sortBy);
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
        dataset.setAcl(getPublicAclIfAllowed(dataset.getAcl()));
        dataset.setOptions(null);
        dataset.setAdditionalInfo(null);
    }

    private void scrubProjectForListing(RangerProject project) {
        project.setAcl(getPublicAclIfAllowed(project.getAcl()));
        project.setOptions(null);
        project.setAdditionalInfo(null);
    }

    private void scrubDataShareForListing(RangerDataShare dataShare) {
        dataShare.setAcl(getPublicAclIfAllowed(dataShare.getAcl()));
        dataShare.setOptions(null);
        dataShare.setAdditionalInfo(null);
    }

    private RangerGdsObjectACL getPublicAclIfAllowed(RangerGdsObjectACL acl) {
        RangerGdsObjectACL ret           = null;
        GdsPermission      grpPublicPerm = acl != null && acl.getGroups() != null ? acl.getGroups().get(RangerConstants.GROUP_PUBLIC) : null;

        if (grpPublicPerm != null) {
            ret = new RangerGdsObjectACL();

            ret.setGroups(Collections.singletonMap(RangerConstants.GROUP_PUBLIC, grpPublicPerm));
        }

        return ret;
    }

    private void removeDshInDsForDataShare(Long dataShareId) {
        SearchFilter                 filter      = new SearchFilter(SearchFilter.DATA_SHARE_ID, dataShareId.toString());
        RangerDataShareInDatasetList dshInDsList = dataShareInDatasetService.searchDataShareInDatasets(filter);

        for (RangerDataShareInDataset dshInDs : dshInDsList.getList()) {
            final boolean dshInDsDeleted = dataShareInDatasetService.delete(dshInDs);

            if (!dshInDsDeleted) {
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
        validator.validateCreateOrUpdate(policy);
        policy.setName("DATASET: " + dataset.getName() + GDS_POLICY_NAME_TIMESTAMP_SEP + System.currentTimeMillis());
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
        policy.setName("PROJECT: " + project.getName() + GDS_POLICY_NAME_TIMESTAMP_SEP + System.currentTimeMillis());
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

    private void updateDatasetNameInPolicy(RangerDataset dataset, RangerPolicy policy) {
        int    sepPos = StringUtils.indexOf(policy.getName(), GDS_POLICY_NAME_TIMESTAMP_SEP);
        String suffix = sepPos != -1 ? policy.getName().substring(sepPos) : (GDS_POLICY_NAME_TIMESTAMP_SEP + System.currentTimeMillis());

        policy.setName("DATASET: " + dataset.getName() + suffix);
        policy.setDescription("Policy for dataset: " + dataset.getName());
    }

    private void updateProjectNameInPolicy(RangerProject project, RangerPolicy policy) {
        int    sepPos = StringUtils.indexOf(policy.getName(), GDS_POLICY_NAME_TIMESTAMP_SEP);
        String suffix = sepPos != -1 ? policy.getName().substring(sepPos) : (GDS_POLICY_NAME_TIMESTAMP_SEP + System.currentTimeMillis());

        policy.setName("PROJECT: " + project.getName() + suffix);
        policy.setDescription("Policy for project: " + project.getName());
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

    private void removeDIPForProject(Long projectId) {
        XXGdsDatasetInProjectDao    dipDao    = daoMgr.getXXGdsDatasetInProject();
        List<XXGdsDatasetInProject> dshidList = dipDao.findByProjectId(projectId);

        for (XXGdsDatasetInProject dip : dshidList) {
            boolean dipDeleted = dipDao.remove(dip.getId());

            if (!dipDeleted) {
                throw restErrorUtil.createRESTException("DatasetInProject could not be deleted",
                        MessageEnums.ERROR_DELETE_OBJECT, dip.getId(), "DatasetInProjectId", null,
                        HttpStatus.SC_INTERNAL_SERVER_ERROR);
            }
        }
    }

    private void addCreatorAsAclAdmin(RangerGdsObjectACL acl) {
        String                     currentUser = bizUtil.getCurrentUserLoginId();
        Map<String, GdsPermission> userAcl     = acl.getUsers();

        if (userAcl == null) {
            userAcl = new HashMap<>();

            acl.setUsers(userAcl);
        }

        if (acl.getUsers().get(currentUser) != GdsPermission.ADMIN) {
            acl.getUsers().put(currentUser, GdsPermission.ADMIN);
        }
    }

    private List<DataShareInDatasetSummary> getDataSharesSummary(RangerDataShareList dataShares, SearchFilter filter) {
        List<DataShareInDatasetSummary> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(dataShares.getList())) {
            RangerDataShareInDatasetList dshInDsList = dataShareInDatasetService.searchDataShareInDatasets(filter);

            if (CollectionUtils.isNotEmpty(dshInDsList.getList())) {
                for (RangerDataShare dataShare : dataShares.getList()) {
                    ret.add(toDshInDsSummary(dataShare, dshInDsList.getList()));
                }
            }
        }

        return ret;
    }

    private List<DataShareInDatasetSummary> getDatasetsSummary(RangerDatasetList datasets, SearchFilter filter) {
        List<DataShareInDatasetSummary> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(datasets.getList())) {
            RangerDataShareInDatasetList dshInDsList = dataShareInDatasetService.searchDataShareInDatasets(filter);

            if (CollectionUtils.isNotEmpty(dshInDsList.getList())) {
                for (RangerDataset dataset : datasets.getList()) {
                    ret.add(toDshInDsSummary(dataset, dshInDsList.getList()));
                }
            }
        }

        return ret;
    }

    private DataShareInDatasetSummary toDshInDsSummary(RangerDataShare dataShare, List<RangerDataShareInDataset> dshInDsList) {
        Optional<RangerDataShareInDataset> dshInDs = dshInDsList.stream().filter(d -> d.getDataShareId().equals(dataShare.getId())).findFirst();

        if (!dshInDs.isPresent()) {
            throw restErrorUtil.createRESTException("DataShareInDataset not found", MessageEnums.DATA_NOT_FOUND, dataShare.getId(), "SharedResourceId", null, HttpStatus.SC_NOT_FOUND);
        }

        DataShareInDatasetSummary summary = new DataShareInDatasetSummary();

        summary.setId(dshInDs.get().getId());
        summary.setDataShareId(dataShare.getId());
        summary.setDataShareName(dataShare.getName());
        summary.setCreatedBy(dataShare.getCreatedBy());
        summary.setCreateTime(dataShare.getCreateTime());
        summary.setUpdatedBy(dataShare.getUpdatedBy());
        summary.setUpdateTime(dataShare.getUpdateTime());
        summary.setGuid(dataShare.getGuid());
        summary.setIsEnabled(dataShare.getIsEnabled());
        summary.setVersion(dataShare.getVersion());

        summary.setServiceId(getServiceId(dataShare.getService()));
        summary.setServiceName(dataShare.getService());
        summary.setZoneId(getZoneId(dataShare.getZone(), null));
        summary.setZoneName(dataShare.getZone());
        summary.setShareStatus(dshInDs.get().getStatus());
        summary.setApprover(dshInDs.get().getApprover());
        summary.setResourceCount(sharedResourceService.getResourceCountForDataShare(dataShare.getId()));

        return summary;
    }

    private DataShareInDatasetSummary toDshInDsSummary(RangerDataset dataset, List<RangerDataShareInDataset> dshInDsList) {
        Optional<RangerDataShareInDataset> dshInDs = dshInDsList.stream().filter(d -> d.getDatasetId().equals(dataset.getId())).findFirst();

        if (!dshInDs.isPresent()) {
            throw restErrorUtil.createRESTException("DataShareInDataset not found", MessageEnums.DATA_NOT_FOUND, dataset.getId(), "DatasetId", null, HttpStatus.SC_NOT_FOUND);
        }

        DataShareInDatasetSummary summary = new DataShareInDatasetSummary();

        summary.setId(dshInDs.get().getId());
        summary.setDatasetId(dataset.getId());
        summary.setDatasetName(dataset.getName());
        summary.setCreatedBy(dataset.getCreatedBy());
        summary.setCreateTime(dataset.getCreateTime());
        summary.setUpdatedBy(dataset.getUpdatedBy());
        summary.setUpdateTime(dataset.getUpdateTime());
        summary.setGuid(dataset.getGuid());
        summary.setIsEnabled(dataset.getIsEnabled());
        summary.setVersion(dataset.getVersion());

        summary.setShareStatus(dshInDs.get().getStatus());
        summary.setApprover(dshInDs.get().getApprover());

        return summary;
    }

    private DataShareInDatasetSummary toDshInDsSummary(RangerDataset dataset, RangerDataShare dataShare,
            RangerDataShareInDataset dshInDs) {
        Map<String, Long>         zoneIds = new HashMap<>();
        DataShareInDatasetSummary summary = new DataShareInDatasetSummary();

        summary.setId(dshInDs.getId());
        summary.setGuid(dshInDs.getGuid());
        summary.setCreatedBy(dshInDs.getCreatedBy());
        summary.setCreateTime(dshInDs.getCreateTime());
        summary.setUpdatedBy(dshInDs.getUpdatedBy());
        summary.setUpdateTime(dshInDs.getUpdateTime());

        summary.setApprover(dshInDs.getApprover());
        summary.setShareStatus(dshInDs.getStatus());
        summary.setDatasetId(dataset.getId());
        summary.setDatasetName(dataset.getName());
        summary.setDataShareId(dataShare.getId());
        summary.setDataShareName(dataShare.getName());
        if (dataShare.getZone() != null && !dataShare.getZone().isEmpty()) {
            summary.setZoneName(dataShare.getZone());
            summary.setZoneId(getZoneId(dataShare.getZone(), zoneIds));
        }
        summary.setServiceName(dataShare.getService());
        summary.setServiceId(getServiceId(dataShare.getService()));
        summary.setDataShareName(dataShare.getName());
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

    private String getServiceType(String serviceName) {
        String serviceTpe = daoMgr.getXXServiceDef().findServiceDefTypeByServiceName(serviceName);

        if (StringUtils.isEmpty(serviceTpe)) {
            throw restErrorUtil.createRESTException("Service type not found", MessageEnums.DATA_NOT_FOUND, null, "ServiceName", null, HttpStatus.SC_NOT_FOUND);
        }

        return serviceTpe;
    }

    private boolean hasResource(List<String> resources, String resourceValue) {
        return resources.stream().filter(Objects::nonNull).anyMatch(resource -> resource.contains(resourceValue));
    }

    private void validate(List<RangerDataShareInDataset> dataSharesInDataset) throws Exception {
        XXGdsDataShareInDatasetDao dshInDsDao = daoMgr.getXXGdsDataShareInDataset();

        if (CollectionUtils.isNotEmpty(dataSharesInDataset)) {
            for (RangerDataShareInDataset dataShareInDataset : dataSharesInDataset) {
                XXGdsDataShareInDataset existing = dshInDsDao.findByDataShareIdAndDatasetId(dataShareInDataset.getDataShareId(), dataShareInDataset.getDatasetId());

                if (existing != null) {
                    throw new Exception("data share id='" + dataShareInDataset.getDataShareId() + "' already shared with dataset id='" + dataShareInDataset.getDatasetId() + "': dataShareInDatasetId=" + existing.getId());
                }

                validator.validateCreate(dataShareInDataset);
            }
        }
    }

    private RangerDataShareInDataset createDataShareInDataset(RangerDataShareInDataset dataShareInDataset) {
        switch (dataShareInDataset.getStatus()) {
            case GRANTED:
            case DENIED:
            case ACTIVE:
                dataShareInDataset.setApprover(bizUtil.getCurrentUserLoginId());
                break;
            default:
                dataShareInDataset.setApprover(null);
                break;
        }

        if (StringUtils.isBlank(dataShareInDataset.getGuid())) {
            dataShareInDataset.setGuid(guidUtil.genGUID());
        }

        RangerDataShareInDataset ret = dataShareInDatasetService.create(dataShareInDataset);

        dataShareInDatasetService.onObjectChange(ret, null, RangerServiceService.OPERATION_CREATE_CONTEXT);
        updateGdsVersionForDataset(dataShareInDataset.getDatasetId());

        return ret;
    }

    private Long getZoneId(String zoneName, Map<String, Long> zoneIds) {
        Long ret = null;

        if (StringUtils.isNotBlank(zoneName)) {
            ret = zoneIds != null ? zoneIds.get(zoneName) : null;

            if (ret == null) {
                XXSecurityZone xxSecurityZone = daoMgr.getXXSecurityZoneDao().findByZoneName(zoneName);

                if (xxSecurityZone == null) {
                    throw restErrorUtil.createRESTException("Security Zone not found", MessageEnums.DATA_NOT_FOUND, null, "ZoneName", null, HttpStatus.SC_NOT_FOUND);
                }

                ret = xxSecurityZone.getId();

                if (zoneIds != null) {
                    zoneIds.put(zoneName, ret);
                }
            }
        }

        return ret;
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

    private void updateGdsVersionForService(Long serviceId) {
        updateGdsVersion();

        Runnable serviceVersionUpdater = new ServiceDBStore.ServiceVersionUpdater(daoMgr, serviceId, ServiceDBStore.VERSION_TYPE.GDS_VERSION, RangerPolicyDelta.CHANGE_TYPE_GDS_UPDATE);

        daoMgr.getRangerTransactionSynchronizationAdapter().executeOnTransactionCommit(serviceVersionUpdater);
    }

    private void updateGdsVersionForService(String serviceName) {
        Long serviceId = daoMgr.getXXService().findIdByName(serviceName);

        if (serviceId != null) {
            updateGdsVersionForService(serviceId);
        }
    }

    private void updateGdsVersionForProject(Long projectId) {
        updateGdsVersion();

        List<Long> serviceIds = daoMgr.getXXGdsProject().findServiceIdsForProject(projectId);

        for (Long serviceId : serviceIds) {
            Runnable serviceVersionUpdater = new ServiceDBStore.ServiceVersionUpdater(daoMgr, serviceId, ServiceDBStore.VERSION_TYPE.GDS_VERSION, RangerPolicyDelta.CHANGE_TYPE_GDS_UPDATE);

            daoMgr.getRangerTransactionSynchronizationAdapter().executeOnTransactionCommit(serviceVersionUpdater);
        }
    }

    private void updateGdsVersionForDataset(Long datasetId) {
        updateGdsVersion();

        List<Long> serviceIds = daoMgr.getXXGdsDataset().findServiceIdsForDataset(datasetId);

        for (Long serviceId : serviceIds) {
            Runnable serviceVersionUpdater = new ServiceDBStore.ServiceVersionUpdater(daoMgr, serviceId, ServiceDBStore.VERSION_TYPE.GDS_VERSION, RangerPolicyDelta.CHANGE_TYPE_GDS_UPDATE);

            daoMgr.getRangerTransactionSynchronizationAdapter().executeOnTransactionCommit(serviceVersionUpdater);
        }
    }

    private void updateGdsVersionForDataShare(Long dataShareId) {
        XXGdsDataShare dataShare = daoMgr.getXXGdsDataShare().getById(dataShareId);

        if (dataShare != null) {
            updateGdsVersionForService(dataShare.getServiceId());
        }
    }

    private GdsPermission deletePrincipalFromAcl(RangerGdsObjectACL acl, String principalName, String principalType) {
        final Map<String, GdsPermission> principalAcls;

        if (principalType.equalsIgnoreCase(REMOVE_REF_TYPE.USER.toString())) {
            principalAcls = acl.getUsers();
        } else if (principalType.equalsIgnoreCase(REMOVE_REF_TYPE.GROUP.toString())) {
            principalAcls = acl.getGroups();
        } else if (principalType.equalsIgnoreCase(REMOVE_REF_TYPE.ROLE.toString())) {
            principalAcls = acl.getRoles();
        } else {
            principalAcls = null;
        }

        return principalAcls != null ? principalAcls.remove(principalName) : null;
    }

    private void copyExistingBaseFields(RangerGdsBaseModelObject objToUpdate, RangerGdsBaseModelObject existingObj) {
        if (objToUpdate != null && existingObj != null) {
            // retain existing values for: guid, createdBy, createTime
            objToUpdate.setGuid(existingObj.getGuid());
            objToUpdate.setCreatedBy(existingObj.getCreatedBy());
            objToUpdate.setCreateTime(existingObj.getCreateTime());

            // retain existing values if objToUpdate has null for: isEnabled, description, options, additionalInfo
            if (objToUpdate.getIsEnabled() == null) {
                objToUpdate.setIsEnabled(existingObj.getIsEnabled());
            }

            if (objToUpdate.getDescription() == null) {
                objToUpdate.setDescription(existingObj.getDescription());
            }

            if (objToUpdate.getOptions() == null) {
                objToUpdate.setOptions(existingObj.getOptions());
            }

            if (objToUpdate.getAdditionalInfo() == null) {
                objToUpdate.setAdditionalInfo(existingObj.getAdditionalInfo());
            }
        }
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
                LOG.error("Failed to update GlobalState version for state:[{}]", stateName, e);
            }
        }
    }
}
