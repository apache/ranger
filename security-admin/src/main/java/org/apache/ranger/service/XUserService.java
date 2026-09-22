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

package org.apache.ranger.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.entity.XXGroupUser;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXPortalUserRole;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants;
import org.apache.ranger.view.VXUser;
import org.apache.ranger.view.VXUserList;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@Scope("singleton")
public class XUserService extends XUserServiceBase<XXUser, VXUser> {
    private final Long createdByUserId;

    String hiddenPassword;

    public XUserService() {
        searchFields.add(new SearchField("name", "obj.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));

        searchFields.add(new SearchField("emailAddress", "xXPortalUser.emailAddress", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL, "XXPortalUser xXPortalUser", "xXPortalUser.loginId = obj.name "));

        searchFields.add(new SearchField("userName", "obj.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));

        searchFields.add(new SearchField("userSource", "xXPortalUser.userSource", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXPortalUser xXPortalUser", "xXPortalUser.loginId = obj.name "));

        searchFields.add(new SearchField("userRoleList", "xXPortalUserRole.userRole", SearchField.DATA_TYPE.STR_LIST, SearchField.SEARCH_TYPE.FULL, "XXPortalUser xXPortalUser, XXPortalUserRole xXPortalUserRole", "xXPortalUser.id=xXPortalUserRole.userId and xXPortalUser.loginId = obj.name "));

        searchFields.add(new SearchField("isVisible", "obj.isVisible", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));

        searchFields.add(new SearchField("status", "xXPortalUser.status", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXPortalUser xXPortalUser", "xXPortalUser.loginId = obj.name "));
        searchFields.add(new SearchField("userRole", "xXPortalUserRole.userRole", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL, "XXPortalUser xXPortalUser, XXPortalUserRole xXPortalUserRole", "xXPortalUser.id=xXPortalUserRole.userId and xXPortalUser.loginId = obj.name "));

        searchFields.add(new SearchField("syncSource", "obj.syncSource", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));

        createdByUserId = PropertiesUtil.getLongProperty("ranger.xuser.createdByUserId", 1);

        hiddenPassword = PropertiesUtil.getProperty("ranger.password.hidden", "*****");

        sortFields.add(new SortField("name", "obj.name", true, SortField.SORT_ORDER.ASC));
    }

    public VXUser getXUserByUserName(String userName) {
        XXUser xxUser = daoManager.getXXUser().findByUserName(userName);

        if (xxUser == null) {
            throw restErrorUtil.createRESTException(userName + " is Not Found", MessageEnums.DATA_NOT_FOUND);
        }

        return populateViewBean(xxUser);
    }

    public VXUser createXUserWithOutLogin(VXUser vxUser) {
        // Prefer Ranger id when present so a UPN/name rename updates in place. Identity
        // (otherAttributes full_name/cloud_id) is checked before name so that a rename landing
        // on an unrelated existing name can't hijack that row; name is only a fallback for
        // callers that never set otherAttributes at all (e.g. internal/service user creation).
        XXUser  xxUser     = null;
        boolean userExists = true;

        if (vxUser.getId() != null) {
            xxUser = getDao().getById(vxUser.getId());
        }
        if (xxUser == null) {
            xxUser = findUserByOtherAttributeIdentity(vxUser.getOtherAttributes());
        }
        if (xxUser == null) {
            xxUser = daoManager.getXXUser().findByUserName(vxUser.getName());
        }
        if (xxUser == null) {
            xxUser     = new XXUser();
            userExists = false;
        }

        XXPortalUser xxPortalUser = daoManager.getXXPortalUser().findByLoginId(vxUser.getName());

        // Only preserve DB visibility when the caller did not send one. Usersync always
        // sends isVisible on upsert and must be able to un-hide after soft-delete.
        if (xxPortalUser != null && xxPortalUser.getUserSource() == RangerCommonEnums.USER_EXTERNAL && vxUser.getIsVisible() == null && userExists) {
            vxUser.setIsVisible(xxUser.getIsVisible());
        }

        xxUser = mapViewToEntityBean(vxUser, xxUser, 0);

        XXPortalUser xXPortalUser = daoManager.getXXPortalUser().getById(createdByUserId);

        if (xXPortalUser != null) {
            xxUser.setAddedByUserId(createdByUserId);
            xxUser.setUpdatedByUserId(createdByUserId);
        }

        if (userExists) {
            xxUser = getDao().update(xxUser);
        } else {
            xxUser = getDao().create(xxUser);
        }

        vxUser = postCreate(xxUser);

        return vxUser;
    }

    public VXUser readResourceWithOutLogin(Long id) {
        XXUser resource = getDao().getById(id);

        if (resource == null) {
            // Returns code 400 with DATA_NOT_FOUND as the error message
            throw restErrorUtil.createRESTException(getResourceName() + " not found", MessageEnums.DATA_NOT_FOUND, id, null, "preRead: " + id + " not found.");
        }

        return populateViewBean(resource);
    }

    public Map<Long, XXUser> getXXPortalUserIdXXUserMap() {
        Map<Long, XXUser> xXPortalUserIdXXUserMap = new HashMap<>();

        try {
            Map<String, XXUser> xXUserMap  = new HashMap<>();
            List<XXUser>        xXUserList = daoManager.getXXUser().getAll();

            if (!CollectionUtils.isEmpty(xXUserList)) {
                for (XXUser xxUser : xXUserList) {
                    xXUserMap.put(xxUser.getName(), xxUser);
                }
            }

            List<XXPortalUser> xXPortalUserList = daoManager.getXXPortalUser().getAll();

            if (!CollectionUtils.isEmpty(xXPortalUserList)) {
                for (XXPortalUser xXPortalUser : xXPortalUserList) {
                    if (xXUserMap.containsKey(xXPortalUser.getLoginId())) {
                        xXPortalUserIdXXUserMap.put(xXPortalUser.getId(), xXUserMap.get(xXPortalUser.getLoginId()));
                    }
                }
            }
        } catch (Exception ex) {
            // ignored
        }

        return xXPortalUserIdXXUserMap;
    }

    public VXUserList lookupXUsers(SearchCriteria searchCriteria, VXUserList vXUserList) {
        List<VXUser> xUserList  = new ArrayList<>();
        List<XXUser> resultList = searchResources(searchCriteria, searchFields, sortFields, vXUserList);

        for (XXUser xXUser : resultList) {
            VXUser vObj = super.mapEntityToViewBean(createViewObject(), xXUser);

            vObj.setIsVisible(xXUser.getIsVisible());

            xUserList.add(vObj);
        }

        vXUserList.setVXUsers(xUserList);

        return vXUserList;
    }

    public Map<Long, Object[]> getXXPortalUserIdXXUserNameMap() {
        Map<Long, Object[]> xXPortalUserIdXXUserMap = new HashMap<>();

        try {
            List<Object[]> xxUserList = daoManager.getXXUser().getAllUserIdNames();

            if (!CollectionUtils.isEmpty(xxUserList)) {
                for (Object[] obj : xxUserList) {
                    xXPortalUserIdXXUserMap.put((Long) obj[0], obj);
                }
            }
        } catch (Exception ex) {
            // ignored
        }

        return xXPortalUserIdXXUserMap;
    }

    @Override
    public VXUser populateViewBean(XXUser xUser) {
        VXUser vObj = super.populateViewBean(xUser);

        vObj.setIsVisible(xUser.getIsVisible());

        populateGroupList(xUser.getId(), vObj);

        return vObj;
    }

    @Override
    protected void validateForCreate(VXUser vObj) {
        XXUser xUser = daoManager.getXXUser().findByUserName(vObj.getName());

        if (xUser != null) {
            throw restErrorUtil.createRESTException(vObj.getName() + " already exists", MessageEnums.ERROR_DUPLICATE_OBJECT);
        }
    }

    @Override
    protected void validateForUpdate(VXUser vObj, XXUser mObj) {
        String vObjName = vObj.getName();
        String mObjName = mObj.getName();

        if (vObjName != null && mObjName != null) {
            if (!vObjName.trim().equalsIgnoreCase(mObjName.trim())) {
                validateForCreate(vObj);
            }
        }
    }

    @Override
    protected VXUser mapEntityToViewBean(VXUser vObj, XXUser mObj) {
        VXUser ret      = super.mapEntityToViewBean(vObj, mObj);
        String userName = ret.getName();

        populateUserAttributes(userName, ret);

        return ret;
    }

    private void populateGroupList(Long xUserId, VXUser vObj) {
        List<XXGroupUser> xGroupUserList = daoManager.getXXGroupUser().findByUserId(xUserId);
        Set<Long>         groupIdList    = new LinkedHashSet<>();
        Set<String>       groupNameList  = new LinkedHashSet<>();

        if (xGroupUserList != null) {
            for (XXGroupUser xGroupUser : xGroupUserList) {
                groupIdList.add(xGroupUser.getParentGroupId());

                groupNameList.add(xGroupUser.getName());
            }
        }
        List<Long>   groups     = new ArrayList<>(groupIdList);
        List<String> groupNames = new ArrayList<>(groupNameList);

        vObj.setGroupIdList(groups);
        vObj.setGroupNameList(groupNames);
    }

    private void populateUserAttributes(String userName, VXUser vObj) {
        if (userName != null && !userName.isEmpty()) {
            List<String> userRoleList = new ArrayList<>();
            XXPortalUser xXPortalUser = daoManager.getXXPortalUser().findByLoginId(userName);

            if (xXPortalUser != null) {
                vObj.setFirstName(xXPortalUser.getFirstName());
                vObj.setLastName(xXPortalUser.getLastName());
                vObj.setPassword(PropertiesUtil.getProperty("ranger.password.hidden"));

                String emailAddress = xXPortalUser.getEmailAddress();

                if (emailAddress != null && stringUtil.validateEmail(emailAddress)) {
                    vObj.setEmailAddress(xXPortalUser.getEmailAddress());
                }

                vObj.setStatus(xXPortalUser.getStatus());
                vObj.setUserSource(xXPortalUser.getUserSource());

                List<XXPortalUserRole> gjUserRoleList = daoManager.getXXPortalUserRole().findByParentId(xXPortalUser.getId());

                for (XXPortalUserRole gjUserRole : gjUserRoleList) {
                    userRoleList.add(gjUserRole.getUserRole());
                }
            }

            if (userRoleList.isEmpty()) {
                userRoleList.add(RangerConstants.ROLE_USER);
            }

            vObj.setUserRoleList(userRoleList);
        }
    }

    private XXUser findUserByOtherAttributeIdentity(String otherAttributes) {
        if (StringUtils.isBlank(otherAttributes)) {
            return null;
        }
        Map<String, String> incoming = JsonUtils.jsonToMapStringString(otherAttributes);
        if (incoming == null || incoming.isEmpty()) {
            return null;
        }
        String fullName   = incoming.get("full_name");
        String cloudId    = incoming.get("cloud_id");
        String syncSource = incoming.get(UgsyncCommonConstants.SYNC_SOURCE);
        if (StringUtils.isBlank(fullName) && StringUtils.isBlank(cloudId)) {
            return null;
        }
        XXUser match = null;
        if (StringUtils.isNotBlank(fullName)) {
            List<XXUser> candidates = daoManager.getXXUser().findByOtherAttributesLike(OtherAttributesMatcher.likePattern(fullName));

            match = OtherAttributesMatcher.findExactMatch(candidates, XXUser::getOtherAttributes, "full_name", fullName, syncSource);
        }
        if (match == null && StringUtils.isNotBlank(cloudId)) {
            List<XXUser> candidates = daoManager.getXXUser().findByOtherAttributesLike(OtherAttributesMatcher.likePattern(cloudId));
            match = OtherAttributesMatcher.findExactMatch(candidates, XXUser::getOtherAttributes, "cloud_id", cloudId, syncSource);
        }
        return match;
    }
}
