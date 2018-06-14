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

 package org.apache.ranger.rest;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.SessionMgr;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.annotation.RangerAnnotationClassName;
import org.apache.ranger.common.annotation.RangerAnnotationJSMgrName;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.security.context.RangerAPIList;
import org.apache.ranger.service.AuthSessionService;
import org.apache.ranger.service.XAuditMapService;
import org.apache.ranger.service.XGroupGroupService;
import org.apache.ranger.service.XGroupPermissionService;
import org.apache.ranger.service.XGroupService;
import org.apache.ranger.service.XGroupUserService;
import org.apache.ranger.service.XModuleDefService;
import org.apache.ranger.service.XPermMapService;
import org.apache.ranger.service.XResourceService;
import org.apache.ranger.service.XUserPermissionService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;


@Path("xusers")
@Component
@Scope("request")
@RangerAnnotationJSMgrName("XUserMgr")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class XUserREST {

	@Autowired
	SearchUtil searchUtil;

	@Autowired
	XUserMgr xUserMgr;

	@Autowired
	XGroupService xGroupService;

	@Autowired
	XModuleDefService xModuleDefService;

	@Autowired
	XUserPermissionService xUserPermissionService;

	@Autowired
	XGroupPermissionService xGroupPermissionService;

	@Autowired
	XUserService xUserService;

	@Autowired
	XGroupUserService xGroupUserService;

	@Autowired
	XGroupGroupService xGroupGroupService;

	@Autowired
	XPermMapService xPermMapService;

	@Autowired
	XAuditMapService xAuditMapService;

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	RangerDaoManager rangerDaoManager;

	@Autowired
	SessionMgr sessionMgr;
	
	@Autowired
	AuthSessionService authSessionService;

	@Autowired
	RangerBizUtil bizUtil;
	
	@Autowired
	XResourceService xResourceService;

	// Handle XGroup
	@GET
	@Path("/groups/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_GROUP + "\")")
	public VXGroup getXGroup(@PathParam("id") Long id) {
		return xUserMgr.getXGroup(id);
	}

	@GET
	@Path("/secure/groups/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SECURE_GET_X_GROUP + "\")")
	public VXGroup secureGetXGroup(@PathParam("id") Long id) {
		return xUserMgr.getXGroup(id);
	}

	@POST
	@Path("/groups")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public VXGroup createXGroup(VXGroup vXGroup) {
		return xUserMgr.createXGroupWithoutLogin(vXGroup);
	}

	@POST
	@Path("/groups/groupinfo")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public VXGroupUserInfo createXGroupUserFromMap(VXGroupUserInfo vXGroupUserInfo) {
		return  xUserMgr.createXGroupUserFromMap(vXGroupUserInfo);
	}
	
	@POST
	@Path("/secure/groups")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public VXGroup secureCreateXGroup(VXGroup vXGroup) {
		return xUserMgr.createXGroup(vXGroup);
	}

	@PUT
	@Path("/groups")
	@Produces({ "application/xml", "application/json" })
	public VXGroup updateXGroup(VXGroup vXGroup) {
		return xUserMgr.updateXGroup(vXGroup);
	}

	@PUT
	@Path("/secure/groups/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXGroup secureUpdateXGroup(VXGroup vXGroup) {
		return xUserMgr.updateXGroup(vXGroup);
	}

	@PUT
	@Path("/secure/groups/visibility")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.MODIFY_GROUPS_VISIBILITY + "\")")
	public void modifyGroupsVisibility(HashMap<Long, Integer> groupVisibilityMap){
		 xUserMgr.modifyGroupsVisibility(groupVisibilityMap);
	}
	
	@DELETE
	@Path("/groups/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@RangerAnnotationClassName(class_name = VXGroup.class)
	public void deleteXGroup(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		String forceDeleteStr = request.getParameter("forceDelete");
		boolean forceDelete = false;
		if(!StringUtils.isEmpty(forceDeleteStr) && "true".equalsIgnoreCase(forceDeleteStr.trim())) {
			forceDelete = true;
		}
		xUserMgr.deleteXGroup(id, forceDelete);
	}

	/**
	 * Implements the traditional search functionalities for XGroups
	 *
	 * @param request
	 * @return
	 */
	@GET
	@Path("/groups")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_X_GROUPS + "\")")
	public VXGroupList searchXGroups(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupService.sortFields);
		searchUtil.extractString(request, searchCriteria, "name", "group name", null);
		searchUtil.extractInt(request, searchCriteria, "isVisible", "Group Visibility");
		searchUtil.extractString(request, searchCriteria, "groupSource", "group source", null);
//		searchUtil.extractInt(request, searchCriteria, "groupSource", "group source");
		return xUserMgr.searchXGroups(searchCriteria);
	}

	@GET
	@Path("/groups/count")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_X_GROUPS + "\")")
	public VXLong countXGroups(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupService.sortFields);

		return xUserMgr.getXGroupSearchCount(searchCriteria);
	}

	// Handle XUser
	@GET
	@Path("/users/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_USER + "\")")
	public VXUser getXUser(@PathParam("id") Long id) {
		return xUserMgr.getXUser(id);
	}

	@GET
	@Path("/secure/users/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SECURE_GET_X_USER + "\")")
	public VXUser secureGetXUser(@PathParam("id") Long id) {
		return xUserMgr.getXUser(id);
	}

	@POST
	@Path("/users")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public VXUser createXUser(VXUser vXUser) {
		return xUserMgr.createXUserWithOutLogin(vXUser);
	}
	
	@POST
	@Path("/users/userinfo")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public VXUserGroupInfo createXUserGroupFromMap(VXUserGroupInfo vXUserGroupInfo) {
		return  xUserMgr.createXUserGroupFromMap(vXUserGroupInfo);
	}
	
	@POST
	@Path("/secure/users")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public VXUser secureCreateXUser(VXUser vXUser) {

		bizUtil.checkUserAccessible(vXUser);
		return xUserMgr.createXUser(vXUser);
	}

	@PUT
	@Path("/users")
	@Produces({ "application/xml", "application/json" })
	public VXUser updateXUser(VXUser vXUser) {
                bizUtil.checkUserAccessible(vXUser);
		return xUserMgr.updateXUser(vXUser);
	}
	
	@PUT
	@Path("/secure/users/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXUser secureUpdateXUser(VXUser vXUser) {

		bizUtil.checkUserAccessible(vXUser);
		return xUserMgr.updateXUser(vXUser);
	}

	@PUT
	@Path("/secure/users/visibility")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.MODIFY_USER_VISIBILITY + "\")")
	public void modifyUserVisibility(HashMap<Long, Integer> visibilityMap){
		 xUserMgr.modifyUserVisibility(visibilityMap);
	}

	@DELETE
	@Path("/users/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@RangerAnnotationClassName(class_name = VXUser.class)
	public void deleteXUser(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		String forceDeleteStr = request.getParameter("forceDelete");
		boolean forceDelete = false;
		if(!StringUtils.isEmpty(forceDeleteStr) && forceDeleteStr.equalsIgnoreCase("true")) {
			forceDelete = true;
		}
		xUserMgr.deleteXUser(id, forceDelete);
	}

	/**
	 * Implements the traditional search functionalities for XUsers
	 *
	 * @param request
	 * @return
	 */
	@GET
	@Path("/users")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_X_USERS + "\")")
	public VXUserList searchXUsers(@Context HttpServletRequest request) {
		String UserRoleParamName = RangerConstants.ROLE_USER;
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xUserService.sortFields);
		String userName = null;
		if (request.getUserPrincipal() != null){
			userName = request.getUserPrincipal().getName();
		}
		searchUtil.extractString(request, searchCriteria, "name", "User name",null);
		searchUtil.extractString(request, searchCriteria, "emailAddress", "Email Address",
				null);		
		searchUtil.extractInt(request, searchCriteria, "userSource", "User Source");
		searchUtil.extractInt(request, searchCriteria, "isVisible", "User Visibility");
		searchUtil.extractInt(request, searchCriteria, "status", "User Status");
		List<String> userRolesList = searchUtil.extractStringList(request, searchCriteria, "userRoleList", "User Role List", "userRoleList", null,
				null);
		searchUtil.extractString(request, searchCriteria, "userRole", "UserRole", null);
		if (CollectionUtils.isNotEmpty(userRolesList) && CollectionUtils.size(userRolesList) == 1 && userRolesList.get(0).equalsIgnoreCase(UserRoleParamName)) {
			if (!(searchCriteria.getParamList().containsKey("name"))) {
				searchCriteria.addParam("name", userName);
			}
			else if ((searchCriteria.getParamList().containsKey("name")) && userName!= null && userName.contains((String) searchCriteria.getParamList().get("name"))) {
				searchCriteria.addParam("name", userName);
			}
			else {
				String randomString = new Random().toString();
				searchCriteria.addParam("name", randomString);
			}
		}

		return xUserMgr.searchXUsers(searchCriteria);
	}

	@GET
	@Path("/users/count")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_X_USERS + "\")")
	public VXLong countXUsers(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xUserService.sortFields);

		return xUserMgr.getXUserSearchCount(searchCriteria);
	}

	// Handle XGroupUser
	@GET
	@Path("/groupusers/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_GROUP_USER + "\")")
	public VXGroupUser getXGroupUser(@PathParam("id") Long id) {
		return xUserMgr.getXGroupUser(id);
	}

	@POST
	@Path("/groupusers")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public VXGroupUser createXGroupUser(VXGroupUser vXGroupUser) {
		return xUserMgr.createXGroupUser(vXGroupUser);
	}

	@PUT
	@Path("/groupusers")
	@Produces({ "application/xml", "application/json" })
	public VXGroupUser updateXGroupUser(VXGroupUser vXGroupUser) {
		return xUserMgr.updateXGroupUser(vXGroupUser);
	}

	@DELETE
	@Path("/groupusers/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@RangerAnnotationClassName(class_name = VXGroupUser.class)
	public void deleteXGroupUser(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = true;
		xUserMgr.deleteXGroupUser(id, force);
	}

	/**
	 * Implements the traditional search functionalities for XGroupUsers
	 *
	 * @param request
	 * @return
	 */
	@GET
	@Path("/groupusers")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_X_GROUP_USERS + "\")")
	public VXGroupUserList searchXGroupUsers(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupUserService.sortFields);
		return xUserMgr.searchXGroupUsers(searchCriteria);
	}
	
	/**
	 * Implements the traditional search functionalities for XGroupUsers by Group name
	 *
	 * @param request
	 * @return
	 */
	@GET
	@Path("/groupusers/groupName/{groupName}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_GROUP_USERS_BY_GROUP_NAME + "\")")
	public VXGroupUserInfo getXGroupUsersByGroupName(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName) {
		return xUserMgr.getXGroupUserFromMap(groupName);
	}

	@GET
	@Path("/groupusers/count")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_X_GROUP_USERS + "\")")
	public VXLong countXGroupUsers(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupUserService.sortFields);

		return xUserMgr.getXGroupUserSearchCount(searchCriteria);
	}

	// Handle XGroupGroup
	@GET
	@Path("/groupgroups/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_GROUP_GROUP + "\")")
	public VXGroupGroup getXGroupGroup(@PathParam("id") Long id) {
		return xUserMgr.getXGroupGroup(id);
	}

	@POST
	@Path("/groupgroups")
	@Produces({ "application/xml", "application/json" })
	public VXGroupGroup createXGroupGroup(VXGroupGroup vXGroupGroup) {
		return xUserMgr.createXGroupGroup(vXGroupGroup);
	}

	@PUT
	@Path("/groupgroups")
	@Produces({ "application/xml", "application/json" })
	public VXGroupGroup updateXGroupGroup(VXGroupGroup vXGroupGroup) {
		return xUserMgr.updateXGroupGroup(vXGroupGroup);
	}

	@DELETE
	@Path("/groupgroups/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@RangerAnnotationClassName(class_name = VXGroupGroup.class)
	public void deleteXGroupGroup(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = false;
		xUserMgr.deleteXGroupGroup(id, force);
	}

	/**
	 * Implements the traditional search functionalities for XGroupGroups
	 *
	 * @param request
	 * @return
	 */
	@GET
	@Path("/groupgroups")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_X_GROUP_GROUPS + "\")")
	public VXGroupGroupList searchXGroupGroups(
			@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupGroupService.sortFields);
		return xUserMgr.searchXGroupGroups(searchCriteria);
	}

	@GET
	@Path("/groupgroups/count")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_X_GROUP_GROUPS + "\")")
	public VXLong countXGroupGroups(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupGroupService.sortFields);

		return xUserMgr.getXGroupGroupSearchCount(searchCriteria);
	}

	// Handle XPermMap
	@GET
	@Path("/permmaps/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_PERM_MAP + "\")")
	public VXPermMap getXPermMap(@PathParam("id") Long id) {
		VXPermMap permMap = xUserMgr.getXPermMap(id);

		if (permMap != null) {
			if (xResourceService.readResource(permMap.getResourceId()) == null) {
				throw restErrorUtil.createRESTException("Invalid Input Data - No resource found with Id: " + permMap.getResourceId(), MessageEnums.INVALID_INPUT_DATA);
			}
		}

		return permMap;
	}

	@POST
	@Path("/permmaps")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_X_PERM_MAP + "\")")
	public VXPermMap createXPermMap(VXPermMap vXPermMap) {

		if (vXPermMap != null) {
			if (xResourceService.readResource(vXPermMap.getResourceId()) == null) {
				throw restErrorUtil.createRESTException("Invalid Input Data - No resource found with Id: " + vXPermMap.getResourceId(), MessageEnums.INVALID_INPUT_DATA);
			}
		}

		return xUserMgr.createXPermMap(vXPermMap);
	}

	@PUT
	@Path("/permmaps")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_X_PERM_MAP + "\")")
	public VXPermMap updateXPermMap(VXPermMap vXPermMap) {
		VXPermMap vXPermMapRet = null;
		if (vXPermMap != null) {
			if (xResourceService.readResource(vXPermMap.getResourceId()) == null) {
				throw restErrorUtil.createRESTException("Invalid Input Data - No resource found with Id: " + vXPermMap.getResourceId());
			}
			else{
				vXPermMapRet = xUserMgr.updateXPermMap(vXPermMap);
			}
		}

			return vXPermMapRet;
	}

	@DELETE
	@Path("/permmaps/{id}")
	@RangerAnnotationClassName(class_name = VXPermMap.class)
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_X_PERM_MAP + "\")")
	public void deleteXPermMap(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = false;
		xUserMgr.deleteXPermMap(id, force);
	}

	/**
	 * Implements the traditional search functionalities for XPermMaps
	 *
	 * @param request
	 * @return
	 */
	@GET
	@Path("/permmaps")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_X_PERM_MAPS + "\")")
	public VXPermMapList searchXPermMaps(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xPermMapService.sortFields);
		return xUserMgr.searchXPermMaps(searchCriteria);
	}

	@GET
	@Path("/permmaps/count")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_X_PERM_MAPS + "\")")
	public VXLong countXPermMaps(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xPermMapService.sortFields);

		return xUserMgr.getXPermMapSearchCount(searchCriteria);
	}

	// Handle XAuditMap
	@GET
	@Path("/auditmaps/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_AUDIT_MAP + "\")")
	public VXAuditMap getXAuditMap(@PathParam("id") Long id) {
		VXAuditMap vXAuditMap = xUserMgr.getXAuditMap(id);

		if (vXAuditMap != null) {
			if (xResourceService.readResource(vXAuditMap.getResourceId()) == null) {
				throw restErrorUtil.createRESTException("Invalid Input Data - No resource found with Id: " + vXAuditMap.getResourceId(), MessageEnums.INVALID_INPUT_DATA);
			}
		}

		return vXAuditMap;
	}

	@POST
	@Path("/auditmaps")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_X_AUDIT_MAP + "\")")
	public VXAuditMap createXAuditMap(VXAuditMap vXAuditMap) {

		if (vXAuditMap != null) {
			if (xResourceService.readResource(vXAuditMap.getResourceId()) == null) {
				throw restErrorUtil.createRESTException("Invalid Input Data - No resource found with Id: " + vXAuditMap.getResourceId(), MessageEnums.INVALID_INPUT_DATA);
			}
		}

		return xUserMgr.createXAuditMap(vXAuditMap);
	}

	@PUT
	@Path("/auditmaps")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_X_AUDIT_MAP + "\")")
	public VXAuditMap updateXAuditMap(VXAuditMap vXAuditMap) {
		VXAuditMap vXAuditMapRet = null;
		if (vXAuditMap != null) {
			if (xResourceService.readResource(vXAuditMap.getResourceId()) == null) {
				throw restErrorUtil.createRESTException("Invalid Input Data - No resource found with Id: " + vXAuditMap.getResourceId(), MessageEnums.INVALID_INPUT_DATA);
			}
			else{
				vXAuditMapRet = xUserMgr.updateXAuditMap(vXAuditMap);
			}
		}

		return vXAuditMapRet;
	}

	@DELETE
	@Path("/auditmaps/{id}")
	@RangerAnnotationClassName(class_name = VXAuditMap.class)
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_X_AUDIT_MAP + "\")")
	public void deleteXAuditMap(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = false;
		xUserMgr.deleteXAuditMap(id, force);
	}

	/**
	 * Implements the traditional search functionalities for XAuditMaps
	 *
	 * @param request
	 * @return
	 */
	@GET
	@Path("/auditmaps")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_X_AUDIT_MAPS + "\")")
	public VXAuditMapList searchXAuditMaps(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xAuditMapService.sortFields);
		return xUserMgr.searchXAuditMaps(searchCriteria);
	}

	@GET
	@Path("/auditmaps/count")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_X_AUDIT_MAPS + "\")")
	public VXLong countXAuditMaps(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xAuditMapService.sortFields);

		return xUserMgr.getXAuditMapSearchCount(searchCriteria);
	}

	// Handle XUser
	@GET
	@Path("/users/userName/{userName}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_USER_BY_USER_NAME + "\")")
	public VXUser getXUserByUserName(@Context HttpServletRequest request,
			@PathParam("userName") String userName) {
		return xUserMgr.getXUserByUserName(userName);
	}

	@GET
	@Path("/groups/groupName/{groupName}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_GROUP_BY_GROUP_NAME + "\")")
	public VXGroup getXGroupByGroupName(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName) {
		return xGroupService.getGroupByGroupName(groupName);
	}

	@DELETE
	@Path("/users/userName/{userName}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteXUserByUserName(@PathParam("userName") String userName,
			@Context HttpServletRequest request) {
		String forceDeleteStr = request.getParameter("forceDelete");
		boolean forceDelete = false;
		if(!StringUtils.isEmpty(forceDeleteStr) && forceDeleteStr.equalsIgnoreCase("true")) {
			forceDelete = true;
		}
		VXUser vxUser = xUserService.getXUserByUserName(userName);
		xUserMgr.deleteXUser(vxUser.getId(), forceDelete);
	}

	@DELETE
	@Path("/groups/groupName/{groupName}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteXGroupByGroupName(
			@PathParam("groupName") String groupName,
			@Context HttpServletRequest request) {
		String forceDeleteStr = request.getParameter("forceDelete");
		boolean forceDelete = false;
		if(!StringUtils.isEmpty(forceDeleteStr) && forceDeleteStr.equalsIgnoreCase("true")) {
			forceDelete = true;
		}
		VXGroup vxGroup = xGroupService.getGroupByGroupName(groupName);
		xUserMgr.deleteXGroup(vxGroup.getId(), forceDelete);
	}

	@DELETE
	@Path("/group/{groupName}/user/{userName}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteXGroupAndXUser(@PathParam("groupName") String groupName,
			@PathParam("userName") String userName,
			@Context HttpServletRequest request) {
		xUserMgr.deleteXGroupAndXUser(groupName, userName);
	}
	
	@GET
	@Path("/{userId}/groups")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_USER_GROUPS + "\")")
	public VXGroupList getXUserGroups(@Context HttpServletRequest request,
			@PathParam("userId") Long id){
		return xUserMgr.getXUserGroups(id);
	}

	@GET
	@Path("/{groupId}/users")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_GROUP_USERS + "\")")
	public VXUserList getXGroupUsers(@Context HttpServletRequest request,
			@PathParam("groupId") Long id){
                SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
                                request, xGroupUserService.sortFields);
                searchCriteria.addParam("xGroupId", id);
                return xUserMgr.getXGroupUsers(searchCriteria);
	}

	@GET
	@Path("/authSessions")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_AUTH_SESSIONS + "\")")
	public VXAuthSessionList getAuthSessions(@Context HttpServletRequest request){
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, AuthSessionService.AUTH_SESSION_SORT_FLDS);
		searchUtil.extractLong(request, searchCriteria, "id", "Auth Session Id");
		searchUtil.extractLong(request, searchCriteria, "userId", "User Id");
		searchUtil.extractInt(request, searchCriteria, "authStatus", "Auth Status");
                searchUtil.extractInt(request, searchCriteria, "authType", "Login Type");
		searchUtil.extractInt(request, searchCriteria, "deviceType", "Device Type");
		searchUtil.extractString(request, searchCriteria, "firstName", "User First Name", StringUtil.VALIDATION_NAME);
		searchUtil.extractString(request, searchCriteria, "lastName", "User Last Name", StringUtil.VALIDATION_NAME);
		searchUtil.extractString(request, searchCriteria, "requestUserAgent", "User Agent", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "requestIP", "Request IP Address", StringUtil.VALIDATION_IP_ADDRESS);
		searchUtil.extractString(request, searchCriteria, "loginId", "Login ID", StringUtil.VALIDATION_TEXT);
                searchUtil.extractDate(request, searchCriteria, "startDate", "Start Date", null);
                searchUtil.extractDate(request, searchCriteria, "endDate", "End Date", null);
		return sessionMgr.searchAuthSessions(searchCriteria);
	}
	
	@GET
	@Path("/authSessions/info")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_AUTH_SESSION + "\")")
	public VXAuthSession getAuthSession(@Context HttpServletRequest request){
		String authSessionId = request.getParameter("extSessionId");
		return sessionMgr.getAuthSessionBySessionId(authSessionId);
	}

	// Handle module permissions
	@POST
	@Path("/permission")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_X_MODULE_DEF_PERMISSION + "\")")
	public VXModuleDef createXModuleDefPermission(VXModuleDef vXModuleDef) {
		xUserMgr.checkAdminAccess();
                bizUtil.blockAuditorRoleUser();
		return xUserMgr.createXModuleDefPermission(vXModuleDef);
	}

	@GET
	@Path("/permission/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_MODULE_DEF_PERMISSION + "\")")
	public VXModuleDef getXModuleDefPermission(@PathParam("id") Long id) {
		return xUserMgr.getXModuleDefPermission(id);
	}

	@PUT
	@Path("/permission/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_X_MODULE_DEF_PERMISSION + "\")")
	public VXModuleDef updateXModuleDefPermission(VXModuleDef vXModuleDef) {
		xUserMgr.checkAdminAccess();
                bizUtil.blockAuditorRoleUser();
		return xUserMgr.updateXModuleDefPermission(vXModuleDef);
	}

	@DELETE
	@Path("/permission/{id}")
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_X_MODULE_DEF_PERMISSION + "\")")
	public void deleteXModuleDefPermission(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = true;
		xUserMgr.checkAdminAccess();
                bizUtil.blockAuditorRoleUser();
		xUserMgr.deleteXModuleDefPermission(id, force);
	}

	@GET
	@Path("/permission")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_X_MODULE_DEF + "\")")
	public VXModuleDefList searchXModuleDef(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xModuleDefService.sortFields);

		searchUtil.extractString(request, searchCriteria, "module",
				"modulename", null);

		searchUtil.extractString(request, searchCriteria, "moduleDefList",
				"id", null);
		searchUtil.extractString(request, searchCriteria, "userName",
				"userName", null);
		searchUtil.extractString(request, searchCriteria, "groupName",
				"groupName", null);

		return xUserMgr.searchXModuleDef(searchCriteria);
	}

	@GET
	@Path("/permission/count")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_X_MODULE_DEF + "\")")
	public VXLong countXModuleDef(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xModuleDefService.sortFields);
		return xUserMgr.getXModuleDefSearchCount(searchCriteria);
	}

	// Handle user permissions
	@POST
	@Path("/permission/user")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_X_USER_PERMISSION + "\")")
	public VXUserPermission createXUserPermission(
			VXUserPermission vXUserPermission) {
		xUserMgr.checkAdminAccess();
                bizUtil.blockAuditorRoleUser();
		return xUserMgr.createXUserPermission(vXUserPermission);
	}

	@GET
	@Path("/permission/user/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_USER_PERMISSION + "\")")
	public VXUserPermission getXUserPermission(@PathParam("id") Long id) {
		return xUserMgr.getXUserPermission(id);
	}

	@PUT
	@Path("/permission/user/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_X_USER_PERMISSION + "\")")
	public VXUserPermission updateXUserPermission(
			VXUserPermission vXUserPermission) {
		xUserMgr.checkAdminAccess();
                bizUtil.blockAuditorRoleUser();
		return xUserMgr.updateXUserPermission(vXUserPermission);
	}

	@DELETE
	@Path("/permission/user/{id}")
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_X_USER_PERMISSION + "\")")
	public void deleteXUserPermission(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = true;
		xUserMgr.checkAdminAccess();
		xUserMgr.deleteXUserPermission(id, force);
	}

	@GET
	@Path("/permission/user")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_X_USER_PERMISSION + "\")")
	public VXUserPermissionList searchXUserPermission(
			@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xUserPermissionService.sortFields);
		searchUtil.extractString(request, searchCriteria, "id", "id",
				StringUtil.VALIDATION_NAME);

		searchUtil.extractString(request, searchCriteria, "userPermissionList",
				"userId", StringUtil.VALIDATION_NAME);
		return xUserMgr.searchXUserPermission(searchCriteria);
	}

	@GET
	@Path("/permission/user/count")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_X_USER_PERMISSION + "\")")
	public VXLong countXUserPermission(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xUserPermissionService.sortFields);
		return xUserMgr.getXUserPermissionSearchCount(searchCriteria);
	}

	// Handle group permissions
	@POST
	@Path("/permission/group")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_X_GROUP_PERMISSION + "\")")
	public VXGroupPermission createXGroupPermission(
			VXGroupPermission vXGroupPermission) {
		xUserMgr.checkAdminAccess();
                bizUtil.blockAuditorRoleUser();
		return xUserMgr.createXGroupPermission(vXGroupPermission);
	}

	@GET
	@Path("/permission/group/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_GROUP_PERMISSION + "\")")
	public VXGroupPermission getXGroupPermission(@PathParam("id") Long id) {
		return xUserMgr.getXGroupPermission(id);
	}

	@PUT
	@Path("/permission/group/{id}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_X_GROUP_PERMISSION + "\")")
	public VXGroupPermission updateXGroupPermission(
			VXGroupPermission vXGroupPermission) {
		xUserMgr.checkAdminAccess();
                bizUtil.blockAuditorRoleUser();
		return xUserMgr.updateXGroupPermission(vXGroupPermission);
	}

	@DELETE
	@Path("/permission/group/{id}")
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_X_GROUP_PERMISSION + "\")")
	public void deleteXGroupPermission(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = true;
		xUserMgr.checkAdminAccess();
                bizUtil.blockAuditorRoleUser();
		xUserMgr.deleteXGroupPermission(id, force);
	}

	@GET
	@Path("/permission/group")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_X_GROUP_PERMISSION + "\")")
	public VXGroupPermissionList searchXGroupPermission(
			@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupPermissionService.sortFields);
		searchUtil.extractString(request, searchCriteria, "id", "id",
				StringUtil.VALIDATION_NAME);
		searchUtil.extractString(request, searchCriteria,
				"groupPermissionList", "groupId", StringUtil.VALIDATION_NAME);
		return xUserMgr.searchXGroupPermission(searchCriteria);
	}

	@GET
	@Path("/permission/group/count")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_X_GROUP_PERMISSION + "\")")
	public VXLong countXGroupPermission(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupPermissionService.sortFields);
		return xUserMgr.getXGroupPermissionSearchCount(searchCriteria);
	}

	@PUT
	@Path("/secure/users/activestatus")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.MODIFY_USER_ACTIVE_STATUS + "\")")
	public void modifyUserActiveStatus(HashMap<Long, Integer> statusMap){
		 xUserMgr.modifyUserActiveStatus(statusMap);
	}

	@PUT
	@Path("/secure/users/roles/{userId}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SET_USER_ROLES_BY_ID + "\")")
	public VXStringList setUserRolesByExternalID(@PathParam("userId") Long userId,
			VXStringList roleList) {
		return xUserMgr.setUserRolesByExternalID(userId, roleList.getVXStrings());
	}

	@PUT
	@Path("/secure/users/roles/userName/{userName}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SET_USER_ROLES_BY_NAME + "\")")
	public VXStringList setUserRolesByName(@PathParam("userName") String userName,
			VXStringList roleList) {
		return xUserMgr.setUserRolesByName(userName, roleList.getVXStrings());
	}

	@GET
	@Path("/secure/users/external/{userId}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_USER_ROLES_BY_ID + "\")")
	public VXStringList getUserRolesByExternalID(@PathParam("userId") Long userId) {
		VXStringList vXStringList=new VXStringList();
		vXStringList=xUserMgr.getUserRolesByExternalID(userId);
		return vXStringList;
	}

	@GET
	@Path("/secure/users/roles/userName/{userName}")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_USER_ROLES_BY_NAME + "\")")
	public VXStringList getUserRolesByName(@PathParam("userName") String userName) {
		VXStringList vXStringList=new VXStringList();
		vXStringList=xUserMgr.getUserRolesByName(userName);
		return vXStringList;
	}


	@DELETE
	@Path("/secure/users/delete")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteUsersByUserName(@Context HttpServletRequest request,VXStringList userList){
		String forceDeleteStr = request.getParameter("forceDelete");
		boolean forceDelete = false;
		if(StringUtils.isNotEmpty(forceDeleteStr) && "true".equalsIgnoreCase(forceDeleteStr)) {
			forceDelete = true;
		}
		if(userList!=null && userList.getList()!=null){
			for(VXString userName:userList.getList()){
				if(StringUtils.isNotEmpty(userName.getValue())){
					VXUser vxUser = xUserService.getXUserByUserName(userName.getValue());
					xUserMgr.deleteXUser(vxUser.getId(), forceDelete);
				}
			}
		}
	}


	@DELETE
	@Path("/secure/groups/delete")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteGroupsByGroupName(
			@Context HttpServletRequest request,VXStringList groupList) {
		String forceDeleteStr = request.getParameter("forceDelete");
		boolean forceDelete = false;
		if(StringUtils.isNotEmpty(forceDeleteStr) && "true".equalsIgnoreCase(forceDeleteStr)) {
			forceDelete = true;
		}
		if(groupList!=null && groupList.getList()!=null){
			for(VXString groupName:groupList.getList()){
				if(StringUtils.isNotEmpty(groupName.getValue())){
					VXGroup vxGroup = xGroupService.getGroupByGroupName(groupName.getValue());
					xUserMgr.deleteXGroup(vxGroup.getId(), forceDelete);
				}
			}
		}
	}

        @DELETE
        @Path("/secure/users/{userName}")
        @Produces({ "application/xml", "application/json" })
        @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
        public void deleteSingleUserByUserName(@Context HttpServletRequest request, @PathParam("userName") String userName) {
                String forceDeleteStr = request.getParameter("forceDelete");
                boolean forceDelete = false;
                if (StringUtils.isNotEmpty(forceDeleteStr) && "true".equalsIgnoreCase(forceDeleteStr)) {
                        forceDelete = true;
                }

                if (StringUtils.isNotEmpty(userName)) {
                        VXUser vxUser = xUserService.getXUserByUserName(userName);
                        xUserMgr.deleteXUser(vxUser.getId(), forceDelete);
                }
        }

        @DELETE
        @Path("/secure/groups/{groupName}")
        @Produces({ "application/xml", "application/json" })
        @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
        public void deleteSingleGroupByGroupName(@Context HttpServletRequest request, @PathParam("groupName") String groupName) {
                String forceDeleteStr = request.getParameter("forceDelete");
                boolean forceDelete = false;
                if (StringUtils.isNotEmpty(forceDeleteStr) && "true".equalsIgnoreCase(forceDeleteStr)) {
                        forceDelete = true;
                }
                if (StringUtils.isNotEmpty(groupName)) {
                        VXGroup vxGroup = xGroupService.getGroupByGroupName(groupName.trim());
                        xUserMgr.deleteXGroup(vxGroup.getId(), forceDelete);
                }
        }

        @DELETE
        @Path("/secure/users/id/{userId}")
        @Produces({ "application/xml", "application/json" })
        @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
        public void deleteSingleUserByUserId(@Context HttpServletRequest request, @PathParam("userId") Long userId) {
                String forceDeleteStr = request.getParameter("forceDelete");
                boolean forceDelete = false;
                if (StringUtils.isNotEmpty(forceDeleteStr) && "true".equalsIgnoreCase(forceDeleteStr)) {
                        forceDelete = true;
                }
                if (userId != null) {
                        xUserMgr.deleteXUser(userId, forceDelete);
                }
        }

        @DELETE
        @Path("/secure/groups/id/{groupId}")
        @Produces({ "application/xml", "application/json" })
        @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
        public void deleteSingleGroupByGroupId(@Context HttpServletRequest request, @PathParam("groupId") Long groupId) {
                String forceDeleteStr = request.getParameter("forceDelete");
                boolean forceDelete = false;
                if (StringUtils.isNotEmpty(forceDeleteStr) && "true".equalsIgnoreCase(forceDeleteStr)) {
                        forceDelete = true;
                }
                if (groupId != null) {
                        xUserMgr.deleteXGroup(groupId, forceDelete);
                }
        }

	@POST
	@Path("/ugsync/auditinfo")
	@Produces({ "application/xml", "application/json" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public VXUgsyncAuditInfo postUserGroupAuditInfo(VXUgsyncAuditInfo vxUgsyncAuditInfo) {

		return xUserMgr.postUserGroupAuditInfo(vxUgsyncAuditInfo);
	}
}
