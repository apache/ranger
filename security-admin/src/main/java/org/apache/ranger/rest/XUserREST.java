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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;

import org.apache.log4j.Logger;
import org.apache.ranger.biz.*;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.XAConstants;
import org.apache.ranger.common.annotation.XAAnnotationClassName;
import org.apache.ranger.common.annotation.XAAnnotationJSMgrName;
import org.apache.ranger.db.XADaoManager;
import org.apache.ranger.entity.XXAuthSession;
import org.apache.ranger.service.*;
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
@XAAnnotationJSMgrName("XUserMgr")
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
	XADaoManager xADaoManager;

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

	
	@DELETE
	@Path("/groups/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@XAAnnotationClassName(class_name = VXGroup.class)
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

	@DELETE
	@Path("/users/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@XAAnnotationClassName(class_name = VXUser.class)
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
	@XAAnnotationClassName(class_name = VXGroupUser.class)
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
	@XAAnnotationClassName(class_name = VXGroupGroup.class)
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
	@XAAnnotationClassName(class_name = VXPermMap.class)
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
	@XAAnnotationClassName(class_name = VXAuditMap.class)
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

}
