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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.apache.log4j.Logger;
import org.apache.ranger.biz.SessionMgr;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.annotation.RangerAnnotationClassName;
import org.apache.ranger.common.annotation.RangerAnnotationJSMgrName;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.service.AuthSessionService;
import org.apache.ranger.service.XAuditMapService;
import org.apache.ranger.service.XGroupGroupService;
import org.apache.ranger.service.XGroupPermissionService;
import org.apache.ranger.service.XGroupService;
import org.apache.ranger.service.XGroupUserService;
import org.apache.ranger.service.XModuleDefService;
import org.apache.ranger.service.XPermMapService;
import org.apache.ranger.service.XUserPermissionService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.VXAuditMap;
import org.apache.ranger.view.VXAuditMapList;
import org.apache.ranger.view.VXAuthSession;
import org.apache.ranger.view.VXAuthSessionList;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXGroupGroup;
import org.apache.ranger.view.VXGroupGroupList;
import org.apache.ranger.view.VXGroupList;
import org.apache.ranger.view.VXGroupPermission;
import org.apache.ranger.view.VXGroupPermissionList;
import org.apache.ranger.view.VXGroupUser;
import org.apache.ranger.view.VXGroupUserList;
import org.apache.ranger.view.VXLong;
import org.apache.ranger.view.VXModuleDef;
import org.apache.ranger.view.VXModuleDefList;
import org.apache.ranger.view.VXPermMap;
import org.apache.ranger.view.VXPermMapList;
import org.apache.ranger.view.VXPortalUser;
import org.apache.ranger.view.VXUser;
import org.apache.ranger.view.VXUserGroupInfo;
import org.apache.ranger.view.VXUserList;
import org.apache.ranger.view.VXUserPermission;
import org.apache.ranger.view.VXUserPermissionList;
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
	static Logger logger = Logger.getLogger(XUserREST.class);

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

	// Handle XGroup
	@GET
	@Path("/groups/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXGroup getXGroup(@PathParam("id") Long id) {
		return xUserMgr.getXGroup(id);
	}

	@GET
	@Path("/secure/groups/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXGroup secureGetXGroup(@PathParam("id") Long id) {
		return xUserMgr.getXGroup(id);
	}

	@POST
	@Path("/groups")
	@Produces({ "application/xml", "application/json" })
	public VXGroup createXGroup(VXGroup vXGroup) {
		return xUserMgr.createXGroupWithoutLogin(vXGroup);
	}

	@POST
	@Path("/secure/groups")
	@Produces({ "application/xml", "application/json" })
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
	public void modifyGroupsVisibility(HashMap<Long, Integer> groupVisibilityMap){
		 xUserMgr.modifyGroupsVisibility(groupVisibilityMap);
	}
	
	@DELETE
	@Path("/groups/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@RangerAnnotationClassName(class_name = VXGroup.class)
	public void deleteXGroup(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = true;
		xUserMgr.deleteXGroup(id, force);
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
	public VXGroupList searchXGroups(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupService.sortFields);
		searchUtil.extractString(request, searchCriteria, "name", "group name", 
				StringUtil.VALIDATION_NAME);
		searchUtil.extractInt(request, searchCriteria, "isVisible", "Group Visibility");
		searchUtil.extractString(request, searchCriteria, "groupSource", "group source", 
				StringUtil.VALIDATION_NAME);
		return xUserMgr.searchXGroups(searchCriteria);
	}

	@GET
	@Path("/groups/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countXGroups(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupService.sortFields);

		return xUserMgr.getXGroupSearchCount(searchCriteria);
	}

	// Handle XUser
	@GET
	@Path("/users/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXUser getXUser(@PathParam("id") Long id) {
		return xUserMgr.getXUser(id);
	}

	@GET
	@Path("/secure/users/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXUser secureGetXUser(@PathParam("id") Long id) {
		return xUserMgr.getXUser(id);
	}

	@POST
	@Path("/users")
	@Produces({ "application/xml", "application/json" })
	public VXUser createXUser(VXUser vXUser) {
		return xUserMgr.createXUserWithOutLogin(vXUser);
	}
	
	@POST
	@Path("/users/userinfo")
	@Produces({ "application/xml", "application/json" })
	public VXUserGroupInfo createXUserGroupFromMap(VXUserGroupInfo vXUserGroupInfo) {
		return  xUserMgr.createXUserGroupFromMap(vXUserGroupInfo);
	} 
	
	@POST
	@Path("/secure/users")
	@Produces({ "application/xml", "application/json" })
	public VXUser secureCreateXUser(VXUser vXUser) {
		return xUserMgr.createXUser(vXUser);
	}

	@PUT
	@Path("/users")
	@Produces({ "application/xml", "application/json" })
	public VXUser updateXUser(VXUser vXUser) {
		return xUserMgr.updateXUser(vXUser);
	}
	
	@PUT
	@Path("/secure/users/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXUser secureUpdateXUser(VXUser vXUser) {
		return xUserMgr.updateXUser(vXUser);
	}

	@PUT
	@Path("/secure/users/visibility")
	@Produces({ "application/xml", "application/json" })
	public void modifyUserVisibility(HashMap<Long, Integer> visibilityMap){
		 xUserMgr.modifyUserVisibility(visibilityMap);
	}

	@DELETE
	@Path("/users/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@RangerAnnotationClassName(class_name = VXUser.class)
	public void deleteXUser(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = true;
		xUserMgr.deleteXUser(id, force);
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
	public VXUserList searchXUsers(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xUserService.sortFields);

		searchUtil.extractString(request, searchCriteria, "name", "User name",
				StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "emailAddress", "Email Address",
				null);		
		searchUtil.extractInt(request, searchCriteria, "userSource", "User Source");
		searchUtil.extractInt(request, searchCriteria, "isVisible", "User Visibility");
		searchUtil.extractString(request, searchCriteria, "userRoleList", "User Role",
				null);
		return xUserMgr.searchXUsers(searchCriteria);
	}

	@GET
	@Path("/users/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countXUsers(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xUserService.sortFields);

		return xUserMgr.getXUserSearchCount(searchCriteria);
	}

	// Handle XGroupUser
	@GET
	@Path("/groupusers/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXGroupUser getXGroupUser(@PathParam("id") Long id) {
		return xUserMgr.getXGroupUser(id);
	}

	@POST
	@Path("/groupusers")
	@Produces({ "application/xml", "application/json" })
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
	public VXGroupUserList searchXGroupUsers(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupUserService.sortFields);
		return xUserMgr.searchXGroupUsers(searchCriteria);
	}

	@GET
	@Path("/groupusers/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countXGroupUsers(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupUserService.sortFields);

		return xUserMgr.getXGroupUserSearchCount(searchCriteria);
	}

	// Handle XGroupGroup
	@GET
	@Path("/groupgroups/{id}")
	@Produces({ "application/xml", "application/json" })
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
	public VXGroupGroupList searchXGroupGroups(
			@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupGroupService.sortFields);
		return xUserMgr.searchXGroupGroups(searchCriteria);
	}

	@GET
	@Path("/groupgroups/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countXGroupGroups(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupGroupService.sortFields);

		return xUserMgr.getXGroupGroupSearchCount(searchCriteria);
	}

	// Handle XPermMap
	@GET
	@Path("/permmaps/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXPermMap getXPermMap(@PathParam("id") Long id) {
		return xUserMgr.getXPermMap(id);
	}

	@POST
	@Path("/permmaps")
	@Produces({ "application/xml", "application/json" })
	public VXPermMap createXPermMap(VXPermMap vXPermMap) {
		return xUserMgr.createXPermMap(vXPermMap);
	}

	@PUT
	@Path("/permmaps")
	@Produces({ "application/xml", "application/json" })
	public VXPermMap updateXPermMap(VXPermMap vXPermMap) {
		return xUserMgr.updateXPermMap(vXPermMap);
	}

	@DELETE
	@Path("/permmaps/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@RangerAnnotationClassName(class_name = VXPermMap.class)
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
	public VXPermMapList searchXPermMaps(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xPermMapService.sortFields);
		return xUserMgr.searchXPermMaps(searchCriteria);
	}

	@GET
	@Path("/permmaps/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countXPermMaps(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xPermMapService.sortFields);

		return xUserMgr.getXPermMapSearchCount(searchCriteria);
	}

	// Handle XAuditMap
	@GET
	@Path("/auditmaps/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXAuditMap getXAuditMap(@PathParam("id") Long id) {
		return xUserMgr.getXAuditMap(id);
	}

	@POST
	@Path("/auditmaps")
	@Produces({ "application/xml", "application/json" })
	public VXAuditMap createXAuditMap(VXAuditMap vXAuditMap) {
		return xUserMgr.createXAuditMap(vXAuditMap);
	}

	@PUT
	@Path("/auditmaps")
	@Produces({ "application/xml", "application/json" })
	public VXAuditMap updateXAuditMap(VXAuditMap vXAuditMap) {
		return xUserMgr.updateXAuditMap(vXAuditMap);
	}

	@DELETE
	@Path("/auditmaps/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@RangerAnnotationClassName(class_name = VXAuditMap.class)
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
	public VXAuditMapList searchXAuditMaps(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xAuditMapService.sortFields);
		return xUserMgr.searchXAuditMaps(searchCriteria);
	}

	@GET
	@Path("/auditmaps/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countXAuditMaps(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xAuditMapService.sortFields);

		return xUserMgr.getXAuditMapSearchCount(searchCriteria);
	}

	// Handle XUser
	@GET
	@Path("/users/userName/{userName}")
	@Produces({ "application/xml", "application/json" })
	public VXUser getXUserByUserName(@Context HttpServletRequest request,
			@PathParam("userName") String userName) {
		return xUserMgr.getXUserByUserName(userName);
	}

	@GET
	@Path("/groups/groupName/{groupName}")
	@Produces({ "application/xml", "application/json" })
	public VXGroup getXGroupByGroupName(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName) {
		return xGroupService.getGroupByGroupName(groupName);
	}

	@DELETE
	@Path("/users/userName/{userName}")
	public void deleteXUserByUserName(@PathParam("userName") String userName,
			@Context HttpServletRequest request) {
		boolean force = true;
		VXUser vxUser = xUserService.getXUserByUserName(userName);
		xUserMgr.deleteXUser(vxUser.getId(), force);
	}

	@DELETE
	@Path("/groups/groupName/{groupName}")
	public void deleteXGroupByGroupName(
			@PathParam("groupName") String groupName,
			@Context HttpServletRequest request) {
		boolean force = true;
		VXGroup vxGroup = xGroupService.getGroupByGroupName(groupName);
		xUserMgr.deleteXGroup(vxGroup.getId(), force);
	}

	// @POST
	// @Path("/group/{groupName}/user/{userName}")
	// @Produces({ "application/xml", "application/json" })
	// public void createXGroupAndXUser(@PathParam("groupName") String
	// groupName,
	// @PathParam("userName") String userName,
	// @Context HttpServletRequest request) {
	// xUserMgr.createXGroupAndXUser(groupName, userName);
	// }
	//
	@DELETE
	@Path("/group/{groupName}/user/{userName}")
	public void deleteXGroupAndXUser(@PathParam("groupName") String groupName,
			@PathParam("userName") String userName,
			@Context HttpServletRequest request) {
		xUserMgr.deleteXGroupAndXUser(groupName, userName);
	}
	
	@GET
	@Path("/{userId}/groups")
	@Produces({ "application/xml", "application/json" })
	public VXGroupList getXUserGroups(@Context HttpServletRequest request, 
			@PathParam("userId") Long id){
		return xUserMgr.getXUserGroups(id);
	}

	@GET
	@Path("/{groupId}/users")
	@Produces({ "application/xml", "application/json" })
	public VXUserList getXGroupUsers(@Context HttpServletRequest request, 
			@PathParam("groupId") Long id){
		return xUserMgr.getXGroupUsers(id);
	}

	@GET
	@Path("/authSessions")
	@Produces({ "application/xml", "application/json" })
	public VXAuthSessionList getAuthSessions(@Context HttpServletRequest request){
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, authSessionService.AUTH_SESSION_SORT_FLDS);
		searchUtil.extractLong(request, searchCriteria, "id", "Auth Session Id");
		searchUtil.extractLong(request, searchCriteria, "userId", "User Id");
		searchUtil.extractInt(request, searchCriteria, "authStatus", "Auth Status");
		searchUtil.extractInt(request, searchCriteria, "authType", "Auth Type");
		searchUtil.extractInt(request, searchCriteria, "deviceType", "Device Type");
		searchUtil.extractString(request, searchCriteria, "firstName", "User First Name", StringUtil.VALIDATION_NAME);
		searchUtil.extractString(request, searchCriteria, "lastName", "User Last Name", StringUtil.VALIDATION_NAME);
		searchUtil.extractString(request, searchCriteria, "requestUserAgent", "User Agent", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "requestIP", "Request IP Address", StringUtil.VALIDATION_IP_ADDRESS);
		searchUtil.extractString(request, searchCriteria, "loginId", "Login ID", StringUtil.VALIDATION_TEXT);
		searchUtil.extractDate(request, searchCriteria, "startDate", "Start date for search", null);
		searchUtil.extractDate(request, searchCriteria, "endDate", "End date for search", null);						
		return sessionMgr.searchAuthSessions(searchCriteria);
	}
	
	@GET
	@Path("/authSessions/info")
	@Produces({ "application/xml", "application/json" })
	public VXAuthSession getAuthSession(@Context HttpServletRequest request){
		String authSessionId = request.getParameter("extSessionId");
		return sessionMgr.getAuthSessionBySessionId(authSessionId);
	}

	// Handle module permissions
	@POST
	@Path("/permission")
	@Produces({ "application/xml", "application/json" })
	public VXModuleDef createXModuleDefPermission(VXModuleDef vXModuleDef) {
		return xUserMgr.createXModuleDefPermission(vXModuleDef);
	}

	@GET
	@Path("/permission/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXModuleDef getXModuleDefPermission(@PathParam("id") Long id) {
		return xUserMgr.getXModuleDefPermission(id);
	}

	@PUT
	@Path("/permission/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXModuleDef updateXModuleDefPermission(VXModuleDef vXModuleDef) {
		return xUserMgr.updateXModuleDefPermission(vXModuleDef);
	}

	@DELETE
	@Path("/permission/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteXModuleDefPermission(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = true;
		xUserMgr.deleteXModuleDefPermission(id, force);
	}

	@GET
	@Path("/permission")
	@Produces({ "application/xml", "application/json" })
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
	public VXLong countXModuleDef(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xModuleDefService.sortFields);
		return xUserMgr.getXModuleDefSearchCount(searchCriteria);
	}

	// Handle user permissions
	@POST
	@Path("/permission/user")
	@Produces({ "application/xml", "application/json" })
	public VXUserPermission createXUserPermission(
			VXUserPermission vXUserPermission) {
		return xUserMgr.createXUserPermission(vXUserPermission);
	}

	@GET
	@Path("/permission/user/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXUserPermission getXUserPermission(@PathParam("id") Long id) {
		return xUserMgr.getXUserPermission(id);
	}

	@PUT
	@Path("/permission/user/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXUserPermission updateXUserPermission(
			VXUserPermission vXUserPermission) {
		return xUserMgr.updateXUserPermission(vXUserPermission);
	}

	@DELETE
	@Path("/permission/user/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteXUserPermission(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = true;
		xUserMgr.deleteXUserPermission(id, force);
	}

	@GET
	@Path("/permission/user")
	@Produces({ "application/xml", "application/json" })
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
	public VXLong countXUserPermission(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xUserPermissionService.sortFields);
		return xUserMgr.getXUserPermissionSearchCount(searchCriteria);
	}

	// Handle group permissions
	@POST
	@Path("/permission/group")
	@Produces({ "application/xml", "application/json" })
	public VXGroupPermission createXGroupPermission(
			VXGroupPermission vXGroupPermission) {
		return xUserMgr.createXGroupPermission(vXGroupPermission);
	}

	@GET
	@Path("/permission/group/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXGroupPermission getXGroupPermission(@PathParam("id") Long id) {
		return xUserMgr.getXGroupPermission(id);
	}

	@PUT
	@Path("/permission/group/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXGroupPermission updateXGroupPermission(
			VXGroupPermission vXGroupPermission) {
		return xUserMgr.updateXGroupPermission(vXGroupPermission);
	}

	@DELETE
	@Path("/permission/group/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteXGroupPermission(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		boolean force = true;
		xUserMgr.deleteXGroupPermission(id, force);
	}

	@GET
	@Path("/permission/group")
	@Produces({ "application/xml", "application/json" })
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
	public VXLong countXGroupPermission(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xGroupPermissionService.sortFields);
		return xUserMgr.getXGroupPermissionSearchCount(searchCriteria);
	}
}
