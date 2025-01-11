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
import org.apache.ranger.entity.XXGdsDataShare;
import org.apache.ranger.entity.XXGdsDataShareInDataset;
import org.apache.ranger.entity.XXGdsDataset;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShareInDataset;
import org.apache.ranger.plugin.model.RangerValiditySchedule;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.view.RangerGdsVList.RangerDataShareInDatasetList;
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
public class RangerGdsDataShareInDatasetService extends RangerGdsBaseModelService<XXGdsDataShareInDataset, RangerDataShareInDataset> {
    private static final Logger LOG = LoggerFactory.getLogger(RangerGdsDataShareInDatasetService.class);

    @Autowired
    GUIDUtil guidUtil;

    @Autowired
    XXPortalUserDao xxPortalUserDao;

    public RangerGdsDataShareInDatasetService() {
        super(AppConstants.CLASS_TYPE_GDS_DATA_SHARE_IN_DATASET, AppConstants.CLASS_TYPE_GDS_DATASET);

        searchFields.add(new SearchField(SearchFilter.DATA_SHARE_IN_DATASET_ID, "obj.id", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.DATA_SHARE_ID, "obj.dataShareId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.DATA_SHARE_NAME, "dsh.name", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXGdsDataShare dsh", "obj.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.DATA_SHARE_NAME_PARTIAL, "dsh.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDataShare dsh", "obj.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.SERVICE_ID, "dsh.serviceId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXGdsDataShare dsh", "obj.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.SERVICE_NAME, "s.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL, "XXGdsDataShare dsh, XXService s", "obj.dataShareId = dsh.id and dsh.serviceId = s.id"));
        searchFields.add(new SearchField(SearchFilter.SERVICE_NAME_PARTIAL, "s.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDataShare dsh, XXService s", "obj.dataShareId = dsh.id and dsh.serviceId = s.id"));
        searchFields.add(new SearchField(SearchFilter.ZONE_ID, "dsh.zoneId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXGdsDataShare dsh, XXSecurityZone z", "obj.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.ZONE_NAME, "z.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL, "XXGdsDataShare dsh, XXSecurityZone z", "obj.dataShareId = dsh.id and dsh.zoneId = z.id"));
        searchFields.add(new SearchField(SearchFilter.ZONE_NAME_PARTIAL, "z.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDataShare dsh, XXSecurityZone z", "obj.dataShareId = dsh.id and dsh.zoneId = z.id"));
        searchFields.add(new SearchField(SearchFilter.DATASET_ID, "obj.datasetId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.DATASET_NAME, "d.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL, "XXGdsDataset d", "obj.datasetId = d.id"));
        searchFields.add(new SearchField(SearchFilter.DATASET_NAME_PARTIAL, "d.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDataset d", "obj.datasetId = d.id"));
        searchFields.add(new SearchField(SearchFilter.PROJECT_ID, "dip.projectId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXGdsDataset d, XXGdsDatasetInProject dip", "obj.datasetId = d.id and d.id = dip.datasetId"));
        searchFields.add(new SearchField(SearchFilter.PROJECT_NAME, "p.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL, "XXGdsDataset d, XXGdsDatasetInProject dip, XXGdsProject p", "obj.datasetId = d.id and d.id = dip.datasetId and dip.projectId = p.id"));
        searchFields.add(new SearchField(SearchFilter.PROJECT_NAME_PARTIAL, "p.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDataset d, XXGdsDatasetInProject dip, XXGdsProject p", "obj.datasetId = d.id and d.id = dip.datasetId and dip.projectId = p.id"));
        searchFields.add(new SearchField(SearchFilter.APPROVER, "obj.approverId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.CREATED_BY, "obj.addedByUserId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.SHARE_STATUS, "obj.status", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));

        sortFields.add(new SortField(SearchFilter.CREATE_TIME, "obj.createTime"));
        sortFields.add(new SortField(SearchFilter.UPDATE_TIME, "obj.updateTime"));
        sortFields.add(new SortField(SearchFilter.DATA_SHARE_IN_DATASET_ID, "obj.id", true, SortField.SORT_ORDER.ASC));

        trxLogAttrs.put("dataShareId", new VTrxLogAttr("dataShareId", "DataShare ID"));
        trxLogAttrs.put("datasetId", new VTrxLogAttr("datasetId", "Dataset ID"));
        trxLogAttrs.put("status", new VTrxLogAttr("status", "Status", true));
        trxLogAttrs.put("validitySchedule", new VTrxLogAttr("validitySchedule", "Validity Schedule"));
        trxLogAttrs.put("profiles", new VTrxLogAttr("profiles", "Profiles"));
        trxLogAttrs.put("approver", new VTrxLogAttr("approver", "Approver"));
    }

    @Override
    public RangerDataShareInDataset postCreate(XXGdsDataShareInDataset xObj) {
        RangerDataShareInDataset ret = super.postCreate(xObj);

        // TODO:

        return ret;
    }

    @Override
    public RangerDataShareInDataset postUpdate(XXGdsDataShareInDataset xObj) {
        RangerDataShareInDataset ret = super.postUpdate(xObj);

        // TODO:

        return ret;
    }

    @Override
    protected XXGdsDataShareInDataset mapViewToEntityBean(RangerDataShareInDataset vObj, XXGdsDataShareInDataset xObj, int operationContext) {
        XXGdsDataShare xDataShare = daoMgr.getXXGdsDataShare().getById(vObj.getDataShareId());

        if (xDataShare == null) {
            throw restErrorUtil.createRESTException("No data share found with ID: " + vObj.getDataShareId(), MessageEnums.INVALID_INPUT_DATA);
        }

        XXGdsDataset xDataset = daoMgr.getXXGdsDataset().getById(vObj.getDatasetId());

        if (xDataset == null) {
            throw restErrorUtil.createRESTException("No dataset found with ID: " + vObj.getDatasetId(), MessageEnums.INVALID_INPUT_DATA);
        }

        xObj.setGuid(vObj.getGuid());
        xObj.setIsEnabled(vObj.getIsEnabled());
        xObj.setDescription(vObj.getDescription());
        xObj.setDataShareId(vObj.getDataShareId());
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
    protected RangerDataShareInDataset mapEntityToViewBean(RangerDataShareInDataset vObj, XXGdsDataShareInDataset xObj) {
        vObj.setGuid(xObj.getGuid());
        vObj.setIsEnabled(xObj.getIsEnabled());
        vObj.setVersion(xObj.getVersion());
        vObj.setDescription(xObj.getDescription());
        vObj.setDataShareId(xObj.getDataShareId());
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
    protected void validateForCreate(RangerDataShareInDataset vObj) {
        List<VXMessage> msgList = null;

        if (vObj.getDataShareId() == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "dataShareId"));
        }

        XXGdsDataShare xDataShare = daoMgr.getXXGdsDataShare().getById(vObj.getDataShareId());

        if (xDataShare == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "dataShareId"));
        }

        if (vObj.getDatasetId() == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "datasetId"));
        }

        XXGdsDataset xDataset = daoMgr.getXXGdsDataset().getById(vObj.getDatasetId());

        if (xDataset == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "datasetId"));
        }

        if (CollectionUtils.isNotEmpty(msgList)) {
            VXResponse gjResponse = new VXResponse();

            gjResponse.setStatusCode(VXResponse.STATUS_ERROR);
            gjResponse.setMsgDesc("Validation failure");
            gjResponse.setMessageList(msgList);

            LOG.debug("Validation failure in createDataShare({}): error={}", vObj, gjResponse);

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
    protected void validateForUpdate(RangerDataShareInDataset vObj, XXGdsDataShareInDataset xObj) {
        List<VXMessage> msgList = null;

        if (vObj.getDataShareId() == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "dataShareId"));
        }

        XXGdsDataShare xDataShare = daoMgr.getXXGdsDataShare().getById(vObj.getDataShareId());

        if (xDataShare == null || !Objects.equals(xDataShare.getId(), xObj.getDataShareId())) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "dataShareId"));
        }

        if (vObj.getDatasetId() == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "datasetId"));
        }

        XXGdsDataset xDataset = daoMgr.getXXGdsDataset().getById(vObj.getDatasetId());

        if (xDataset == null || !Objects.equals(xDataset.getId(), xObj.getDatasetId())) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "datasetId"));
        }

        if (CollectionUtils.isNotEmpty(msgList)) {
            VXResponse gjResponse = new VXResponse();

            gjResponse.setStatusCode(VXResponse.STATUS_ERROR);
            gjResponse.setMsgDesc("Validation failure");
            gjResponse.setMessageList(msgList);

            LOG.debug("Validation failure in updateDataShare({}): error={}", vObj, gjResponse);

            throw restErrorUtil.createRESTException(gjResponse);
        }

        if (vObj.getIsEnabled() == null) {
            vObj.setIsEnabled(Boolean.TRUE);
        }
    }

    @Override
    public XXGdsDataShareInDataset preDelete(Long id) {
        // Update ServiceVersionInfo for each service in the zone
        XXGdsDataShareInDataset ret = super.preDelete(id);

        // TODO:

        return ret;
    }

    @Override
    public String getParentObjectName(RangerDataShareInDataset obj, RangerDataShareInDataset oldObj) {
        Long         datasetId = obj != null ? obj.getDatasetId() : null;
        XXGdsDataset dataset   = datasetId != null ? daoMgr.getXXGdsDataset().getById(datasetId) : null;

        return dataset != null ? dataset.getName() : null;
    }

    @Override
    public Long getParentObjectId(RangerDataShareInDataset obj, RangerDataShareInDataset oldObj) {
        return obj != null ? obj.getDatasetId() : null;
    }

    public RangerDataShareInDataset getPopulatedViewObject(XXGdsDataShareInDataset xObj) {
        return this.populateViewBean(xObj);
    }

    public RangerDataShareInDatasetList searchDataShareInDatasets(SearchFilter filter) {
        LOG.debug("==> searchDataShareInDatasets({})", filter);

        RangerDataShareInDatasetList  ret      = new RangerDataShareInDatasetList();
        List<XXGdsDataShareInDataset> datasets = super.searchResources(filter, searchFields, sortFields, ret);

        if (datasets != null) {
            for (XXGdsDataShareInDataset dataset : datasets) {
                ret.getList().add(getPopulatedViewObject(dataset));
            }
        }

        LOG.debug("<== searchDataShareInDatasets({}): ret={}", filter, ret);

        return ret;
    }
}
