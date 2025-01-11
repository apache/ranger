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
import org.apache.ranger.entity.XXGdsProject;
import org.apache.ranger.plugin.model.RangerGds;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.view.RangerGdsVList.RangerProjectList;
import org.apache.ranger.view.VXMessage;
import org.apache.ranger.view.VXResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Scope("singleton")
public class RangerGdsProjectService extends RangerGdsBaseModelService<XXGdsProject, RangerProject> {
    private static final Logger LOG = LoggerFactory.getLogger(RangerGdsProjectService.class);

    @Autowired
    GUIDUtil guidUtil;

    public RangerGdsProjectService() {
        super(AppConstants.CLASS_TYPE_GDS_PROJECT);

        searchFields.add(new SearchField(SearchFilter.PROJECT_NAME, "obj.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.PROJECT_NAME_PARTIAL, "obj.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
        searchFields.add(new SearchField(SearchFilter.PROJECT_ID, "obj.id", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.DATASET_NAME, "d.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL, "XXGdsDatasetInProject dip, XXGdsDataset d", "obj.id = dip.projectId and dip.datasetId = d.id"));
        searchFields.add(new SearchField(SearchFilter.DATASET_NAME_PARTIAL, "d.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDatasetInProject dip, XXGdsDataset d", "obj.id = dip.projectId and dip.datasetId = d.id"));
        searchFields.add(new SearchField(SearchFilter.DATASET_ID, "dip.datasetId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXGdsDatasetInProject dip", "obj.id = dip.projectId"));
        searchFields.add(new SearchField(SearchFilter.DATA_SHARE_ID, "dshid.dataShareId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXGdsDatasetInProject dip, XXGdsDataShareInDataset dshid, XXGdsDataShareInDataset dsid", "obj.id = dip.projectId and dip.datasetId = dshid.datasetId"));
        searchFields.add(new SearchField(SearchFilter.DATA_SHARE_NAME, "dsh.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL, "XXGdsDatasetInProject dip, XXGdsDataShareInDataset dshid, XXGdsDataShare dsh", "obj.id = dip.projectId and dip.datasetId = dshid.datasetId and dshid.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.DATA_SHARE_NAME_PARTIAL, "dsh.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDatasetInProject dip, XXGdsDataShareInDataset dshid, XXGdsDataShare dsh", "obj.id = dip.projectId and dip.datasetId = dshid.datasetId and dshid.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.SERVICE_ID, "dsh.serviceId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXGdsDatasetInProject dip, XXGdsDataShareInDataset dshid, XXGdsDataShare dsh", "obj.id = dip.projectId and dip.datasetId = dshid.datasetId and dshid.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.SERVICE_NAME, "s.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL, "XXGdsDatasetInProject dip, XXGdsDataShareInDataset dshid, XXGdsDataShare dsh, XXService s", "obj.id = dip.projectId and dip.datasetId = dshid.datasetId and dshid.dataShareId = dsh.id and dsh.serviceId = s.id"));
        searchFields.add(new SearchField(SearchFilter.SERVICE_NAME_PARTIAL, "s.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDatasetInProject dip, XXGdsDataShareInDataset dshid, XXGdsDataShare dsh, XXService s", "obj.id = dip.projectId and dip.datasetId = dshid.datasetId and dshid.dataShareId = dsh.id and dsh.serviceId = s.id"));
        searchFields.add(new SearchField(SearchFilter.ZONE_ID, "dsh.zoneId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXGdsDatasetInProject dip, XXGdsDataShareInDataset dshid, XXGdsDataShare dsh", "obj.id = dip.projectId and dip.datasetId = dshid.datasetId and dshid.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.ZONE_NAME, "z.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL, "XXGdsDatasetInProject dip, XXGdsDataShareInDataset dshid, XXGdsDataShare dsh, XXSecurityZone z", "obj.id = dip.projectId and dip.datasetId = dshid.datasetId and dshid.dataShareId = dsh.id and dsh.zoneId = z.id"));
        searchFields.add(new SearchField(SearchFilter.ZONE_NAME_PARTIAL, "z.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDatasetInProject dip, XXGdsDataShareInDataset dshid, XXGdsDataShare dsh, XXSecurityZone z", "obj.id = dip.projectId and dip.datasetId = dshid.datasetId and dshid.dataShareId = dsh.id and dsh.zoneId = z.id"));

        sortFields.add(new SortField(SearchFilter.CREATE_TIME, "obj.createTime"));
        sortFields.add(new SortField(SearchFilter.UPDATE_TIME, "obj.updateTime"));
        sortFields.add(new SortField(SearchFilter.PROJECT_ID, "obj.id", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField(SearchFilter.PROJECT_NAME, "obj.name"));

        trxLogAttrs.put("name", new VTrxLogAttr("name", "Name", false, true));
        trxLogAttrs.put("acl", new VTrxLogAttr("acl", "ACL"));
        trxLogAttrs.put("termsOfUse", new VTrxLogAttr("termsOfUse", "Terms of use"));
    }

    @Override
    public RangerProject postCreate(XXGdsProject xObj) {
        RangerProject ret = super.postCreate(xObj);

        // TODO:

        return ret;
    }

    @Override
    public RangerProject postUpdate(XXGdsProject xObj) {
        RangerProject ret = super.postUpdate(xObj);

        // TODO:

        return ret;
    }

    @Override
    protected XXGdsProject mapViewToEntityBean(RangerProject vObj, XXGdsProject xObj, int operationContext) {
        xObj.setGuid(vObj.getGuid());
        xObj.setIsEnabled(vObj.getIsEnabled());
        xObj.setName(vObj.getName());
        xObj.setDescription(vObj.getDescription());
        xObj.setAcl(JsonUtils.objectToJson(vObj.getAcl()));
        xObj.setTermsOfUse(vObj.getTermsOfUse());
        xObj.setOptions(JsonUtils.mapToJson(vObj.getOptions()));
        xObj.setAdditionalInfo(JsonUtils.mapToJson(vObj.getAdditionalInfo()));

        return xObj;
    }

    @Override
    protected RangerProject mapEntityToViewBean(RangerProject vObj, XXGdsProject xObj) {
        vObj.setGuid(xObj.getGuid());
        vObj.setIsEnabled(xObj.getIsEnabled());
        vObj.setVersion(xObj.getVersion());
        vObj.setName(xObj.getName());
        vObj.setDescription(xObj.getDescription());
        vObj.setAcl(JsonUtils.jsonToObject(xObj.getAcl(), RangerGds.RangerGdsObjectACL.class));
        vObj.setTermsOfUse(xObj.getTermsOfUse());
        vObj.setOptions(JsonUtils.jsonToMapStringString(xObj.getOptions()));
        vObj.setAdditionalInfo(JsonUtils.jsonToMapStringString(xObj.getAdditionalInfo()));

        return vObj;
    }

    @Override
    protected void validateForCreate(RangerProject vObj) {
        List<VXMessage> msgList = null;

        if (StringUtils.isBlank(vObj.getName())) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "name"));
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
    protected void validateForUpdate(RangerProject vObj, XXGdsProject xObj) {
        List<VXMessage> msgList = null;

        if (StringUtils.isBlank(vObj.getName())) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "name"));
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
    public XXGdsProject preDelete(Long id) {
        // Update ServiceVersionInfo for each service in the zone
        XXGdsProject ret = super.preDelete(id);

        // TODO:

        return ret;
    }

    public RangerProject getPopulatedViewObject(XXGdsProject xObj) {
        return this.populateViewBean(xObj);
    }

    public RangerProjectList searchProjects(SearchFilter filter) {
        LOG.debug("==> searchProjects({})", filter);

        RangerProjectList  ret      = new RangerProjectList();
        List<XXGdsProject> projects = super.searchResources(filter, searchFields, sortFields, ret);

        if (projects != null) {
            for (XXGdsProject project : projects) {
                ret.getList().add(getPopulatedViewObject(project));
            }
        }

        LOG.debug("<== searchProjects({}): ret={}", filter, ret);

        return ret;
    }
}
