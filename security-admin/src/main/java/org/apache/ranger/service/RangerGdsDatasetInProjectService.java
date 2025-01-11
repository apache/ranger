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

package org.apache.ranger.service;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.db.XXPortalUserDao;
import org.apache.ranger.entity.XXGdsDataset;
import org.apache.ranger.entity.XXGdsDatasetInProject;
import org.apache.ranger.entity.XXGdsProject;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.plugin.model.RangerGds.RangerDatasetInProject;
import org.apache.ranger.plugin.model.RangerValiditySchedule;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.view.RangerGdsVList.RangerDatasetInProjectList;
import org.apache.ranger.view.VXMessage;
import org.apache.ranger.view.VXResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
@Scope("singleton")
public class RangerGdsDatasetInProjectService extends RangerGdsBaseModelService<XXGdsDatasetInProject, RangerDatasetInProject> {
    private static final Logger LOG = LoggerFactory.getLogger(RangerGdsDatasetInProjectService.class);

    @Autowired
    GUIDUtil guidUtil;

    @Autowired
    XXPortalUserDao xxPortalUserDao;

    public RangerGdsDatasetInProjectService() {
        super(AppConstants.CLASS_TYPE_GDS_DATASET_IN_PROJECT, AppConstants.CLASS_TYPE_GDS_DATASET);

        searchFields.add(new SearchField(SearchFilter.DATASET_IN_PROJECT_ID, "obj.id", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.DATASET_ID, "obj.datasetId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.DATASET_NAME, "d.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL, "XXGdsDataset d", "obj.datasetId = d.id"));
        searchFields.add(new SearchField(SearchFilter.DATASET_NAME_PARTIAL, "d.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDataset d", "obj.datasetId = d.id"));
        searchFields.add(new SearchField(SearchFilter.PROJECT_ID, "obj.projectId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.PROJECT_NAME, "p.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL, "XXGdsProject p", "obj.projectId = p.id"));
        searchFields.add(new SearchField(SearchFilter.PROJECT_NAME_PARTIAL, "p.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXGdsProject p", "obj.projectId = p.id"));
        searchFields.add(new SearchField(SearchFilter.SERVICE_ID, "dsh.serviceId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXGdsDataShareInDataset dshid, XXGdsDataShare dsh", "obj.datasetId = dshid.datasetId and dshid.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.SERVICE_NAME, "s.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL, "XXGdsDataShareInDataset dshid, XXGdsDataShare dsh, XXService s", "obj.datasetId = dshid.datasetId and dshid.dataShareId = dsh.id and dsh.serviceId = s.id"));
        searchFields.add(new SearchField(SearchFilter.SERVICE_NAME_PARTIAL, "s.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDataShareInDataset dshid, XXGdsDataShare dsh, XXService s", "obj.datasetId = dshid.datasetId and dshid.dataShareId = dsh.id and dsh.serviceId = s.id"));

        sortFields.add(new SortField(SearchFilter.CREATE_TIME, "obj.createTime"));
        sortFields.add(new SortField(SearchFilter.UPDATE_TIME, "obj.updateTime"));
        sortFields.add(new SortField(SearchFilter.DATASET_IN_PROJECT_ID, "obj.id", true, SortField.SORT_ORDER.ASC));

        trxLogAttrs.put("datasetId", new VTrxLogAttr("datasetId", "Dataset ID"));
        trxLogAttrs.put("projectId", new VTrxLogAttr("projectId", "Project ID"));
        trxLogAttrs.put("status", new VTrxLogAttr("status", "Status", true));
        trxLogAttrs.put("validitySchedule", new VTrxLogAttr("validitySchedule", "Validity Schedule"));
        trxLogAttrs.put("profiles", new VTrxLogAttr("profiles", "Profiles"));
        trxLogAttrs.put("approver", new VTrxLogAttr("approver", "Approver"));
    }

    @Override
    public RangerDatasetInProject postCreate(XXGdsDatasetInProject xObj) {
        RangerDatasetInProject ret = super.postCreate(xObj);

        // TODO:

        return ret;
    }

    @Override
    public RangerDatasetInProject postUpdate(XXGdsDatasetInProject xObj) {
        RangerDatasetInProject ret = super.postUpdate(xObj);

        // TODO:

        return ret;
    }

    @Override
    protected XXGdsDatasetInProject mapViewToEntityBean(RangerDatasetInProject vObj, XXGdsDatasetInProject xObj, int operationContext) {
        XXGdsDataset xDataset = daoMgr.getXXGdsDataset().getById(vObj.getDatasetId());

        if (xDataset == null) {
            throw restErrorUtil.createRESTException("No dataset found with ID: " + vObj.getDatasetId(), MessageEnums.INVALID_INPUT_DATA);
        }

        XXGdsProject xProject = daoMgr.getXXGdsProject().getById(vObj.getProjectId());

        if (xProject == null) {
            throw restErrorUtil.createRESTException("No project found with ID: " + vObj.getProjectId(), MessageEnums.INVALID_INPUT_DATA);
        }

        xObj.setGuid(vObj.getGuid());
        xObj.setIsEnabled(vObj.getIsEnabled());
        xObj.setDescription(vObj.getDescription());
        xObj.setProjectId(vObj.getProjectId());
        xObj.setDatasetId(vObj.getDatasetId());
        xObj.setStatus((short) vObj.getStatus().ordinal());
        xObj.setValidityPeriod(JsonUtils.objectToJson(vObj.getValiditySchedule()));
        xObj.setProfiles(JsonUtils.objectToJson(vObj.getProfiles()));
        xObj.setOptions(JsonUtils.mapToJson(vObj.getOptions()));
        xObj.setAdditionalInfo(JsonUtils.mapToJson(vObj.getAdditionalInfo()));

        final XXPortalUser user = xxPortalUserDao.findByLoginId(vObj.getApprover());

        xObj.setApproverId(user == null ? null : user.getId());

        return xObj;
    }

    @Override
    protected RangerDatasetInProject mapEntityToViewBean(RangerDatasetInProject vObj, XXGdsDatasetInProject xObj) {
        vObj.setGuid(xObj.getGuid());
        vObj.setIsEnabled(xObj.getIsEnabled());
        vObj.setVersion(xObj.getVersion());
        vObj.setDescription(xObj.getDescription());
        vObj.setProjectId(xObj.getProjectId());
        vObj.setDatasetId(xObj.getDatasetId());
        vObj.setStatus(toShareStatus(xObj.getStatus()));
        vObj.setValiditySchedule(JsonUtils.jsonToObject(xObj.getValidityPeriod(), RangerValiditySchedule.class));
        vObj.setProfiles(JsonUtils.jsonToSetString(xObj.getProfiles()));
        vObj.setOptions(JsonUtils.jsonToMapStringString(xObj.getOptions()));
        vObj.setAdditionalInfo(JsonUtils.jsonToMapStringString(xObj.getAdditionalInfo()));
        vObj.setApprover(getUserName(xObj.getApproverId()));

        return vObj;
    }

    @Override
    protected void validateForCreate(RangerDatasetInProject vObj) {
        List<VXMessage> msgList = null;

        if (vObj.getDatasetId() == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "datasetId"));
        }

        XXGdsDataset xDataset = daoMgr.getXXGdsDataset().getById(vObj.getDatasetId());

        if (xDataset == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "datasetId"));
        }

        if (vObj.getProjectId() == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "projectId"));
        }

        XXGdsProject xProject = daoMgr.getXXGdsProject().getById(vObj.getProjectId());

        if (xProject == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "projectId"));
        }

        if (CollectionUtils.isNotEmpty(msgList)) {
            VXResponse gjResponse = new VXResponse();

            gjResponse.setStatusCode(VXResponse.STATUS_ERROR);
            gjResponse.setMsgDesc("Validation failure");
            gjResponse.setMessageList(msgList);

            LOG.debug("Validation failure in createProject({}): error={}", vObj, gjResponse);

            throw restErrorUtil.createRESTException(gjResponse);
        }

        if (StringUtils.isBlank(vObj.getGuid())) {
            vObj.setGuid(guidUtil.genGUID());
        }

        if (vObj.getIsEnabled() == null) {
            vObj.setIsEnabled(Boolean.TRUE);
        }
    }

    @Override
    protected void validateForUpdate(RangerDatasetInProject vObj, XXGdsDatasetInProject xObj) {
        List<VXMessage> msgList = null;

        if (vObj.getDatasetId() == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "datasetId"));
        }

        XXGdsDataset xDataset = daoMgr.getXXGdsDataset().getById(vObj.getDatasetId());

        if (xDataset == null || !Objects.equals(xDataset.getId(), xObj.getDatasetId())) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "datasetId"));
        }

        if (vObj.getProjectId() == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "projectId"));
        }

        XXGdsProject xProject = daoMgr.getXXGdsProject().getById(vObj.getProjectId());

        if (xProject == null || !Objects.equals(xProject.getId(), xObj.getProjectId())) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "projectId"));
        }

        if (CollectionUtils.isNotEmpty(msgList)) {
            VXResponse gjResponse = new VXResponse();

            gjResponse.setStatusCode(VXResponse.STATUS_ERROR);
            gjResponse.setMsgDesc("Validation failure");
            gjResponse.setMessageList(msgList);

            LOG.debug("Validation failure in updateProject({}): error={}", vObj, gjResponse);

            throw restErrorUtil.createRESTException(gjResponse);
        }

        if (vObj.getIsEnabled() == null) {
            vObj.setIsEnabled(Boolean.TRUE);
        }
    }

    @Override
    public XXGdsDatasetInProject preDelete(Long id) {
        // Update ServiceVersionInfo for each service in the zone
        XXGdsDatasetInProject ret = super.preDelete(id);

        // TODO:

        return ret;
    }

    @Override
    public String getParentObjectName(RangerDatasetInProject obj, RangerDatasetInProject oldObj) {
        Long         datasetId = obj != null ? obj.getDatasetId() : null;
        XXGdsDataset dataset   = datasetId != null ? daoMgr.getXXGdsDataset().getById(datasetId) : null;

        return dataset != null ? dataset.getName() : null;
    }

    @Override
    public Long getParentObjectId(RangerDatasetInProject obj, RangerDatasetInProject oldObj) {
        return obj != null ? obj.getDatasetId() : null;
    }

    public RangerDatasetInProject getPopulatedViewObject(XXGdsDatasetInProject xObj) {
        return this.populateViewBean(xObj);
    }

    public RangerDatasetInProjectList searchDatasetInProjects(SearchFilter filter) {
        LOG.debug("==> searchDatasetInProjects({})", filter);

        RangerDatasetInProjectList  ret      = new RangerDatasetInProjectList();
        List<XXGdsDatasetInProject> datasets = super.searchResources(filter, searchFields, sortFields, ret);

        if (datasets != null) {
            for (XXGdsDatasetInProject dataset : datasets) {
                ret.getList().add(getPopulatedViewObject(dataset));
            }
        }

        LOG.debug("<== searchDatasetInProjects({}): ret={}", filter, ret);

        return ret;
    }

    public Long getDatasetsInProjectCount(long datasetId) {
        LOG.debug("==> getDatasetsInProjectCount({})", datasetId);

        SearchFilter filter = new SearchFilter();

        filter.setParam(SearchFilter.DATASET_ID, String.valueOf(datasetId));

        Long ret = super.getCountForSearchQuery(filter, searchFields);

        LOG.debug("<== getDatasetsInProjectCount({}): ret={}", datasetId, ret);

        return ret;
    }
}
