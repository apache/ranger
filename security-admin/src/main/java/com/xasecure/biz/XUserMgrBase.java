package com.xasecure.biz;
/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure
 */

import com.xasecure.common.*;
import com.xasecure.service.*;
import com.xasecure.view.*;
import org.springframework.beans.factory.annotation.Autowired;
public class XUserMgrBase {

	@Autowired
	RESTErrorUtil restErrorUtil;

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
	public VXGroup getXGroup(Long id){
		return (VXGroup)xGroupService.readResource(id);
	}

	public VXGroup createXGroup(VXGroup vXGroup){
		vXGroup =  (VXGroup)xGroupService.createResource(vXGroup);
		return vXGroup;
	}

	public VXGroup updateXGroup(VXGroup vXGroup) {
		vXGroup =  (VXGroup)xGroupService.updateResource(vXGroup);
		return vXGroup;
	}

	public void deleteXGroup(Long id, boolean force) {
		 if (force) {
			 xGroupService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXGroupList searchXGroups(SearchCriteria searchCriteria) {
		return xGroupService.searchXGroups(searchCriteria);
	}

	public VXLong getXGroupSearchCount(SearchCriteria searchCriteria) {
		return xGroupService.getSearchCount(searchCriteria,
				xGroupService.searchFields);
	}

	public VXUser getXUser(Long id){
		return (VXUser)xUserService.readResource(id);
	}

	public VXUser createXUser(VXUser vXUser){
		vXUser =  (VXUser)xUserService.createResource(vXUser);
		return vXUser;
	}

	public VXUser updateXUser(VXUser vXUser) {
		vXUser =  (VXUser)xUserService.updateResource(vXUser);
		return vXUser;
	}

	public void deleteXUser(Long id, boolean force) {
		 if (force) {
			 xUserService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXUserList searchXUsers(SearchCriteria searchCriteria) {
		return xUserService.searchXUsers(searchCriteria);
	}

	public VXLong getXUserSearchCount(SearchCriteria searchCriteria) {
		return xUserService.getSearchCount(searchCriteria,
				xUserService.searchFields);
	}

	public VXGroupUser getXGroupUser(Long id){
		return (VXGroupUser)xGroupUserService.readResource(id);
	}

	public VXGroupUser createXGroupUser(VXGroupUser vXGroupUser){
		vXGroupUser =  (VXGroupUser)xGroupUserService.createResource(vXGroupUser);
		return vXGroupUser;
	}

	public VXGroupUser updateXGroupUser(VXGroupUser vXGroupUser) {
		vXGroupUser =  (VXGroupUser)xGroupUserService.updateResource(vXGroupUser);
		return vXGroupUser;
	}

	public void deleteXGroupUser(Long id, boolean force) {
		 if (force) {
			 xGroupUserService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXGroupUserList searchXGroupUsers(SearchCriteria searchCriteria) {
		return xGroupUserService.searchXGroupUsers(searchCriteria);
	}

	public VXLong getXGroupUserSearchCount(SearchCriteria searchCriteria) {
		return xGroupUserService.getSearchCount(searchCriteria,
				xGroupUserService.searchFields);
	}

	public VXGroupGroup getXGroupGroup(Long id){
		return (VXGroupGroup)xGroupGroupService.readResource(id);
	}

	public VXGroupGroup createXGroupGroup(VXGroupGroup vXGroupGroup){
		vXGroupGroup =  (VXGroupGroup)xGroupGroupService.createResource(vXGroupGroup);
		return vXGroupGroup;
	}

	public VXGroupGroup updateXGroupGroup(VXGroupGroup vXGroupGroup) {
		vXGroupGroup =  (VXGroupGroup)xGroupGroupService.updateResource(vXGroupGroup);
		return vXGroupGroup;
	}

	public void deleteXGroupGroup(Long id, boolean force) {
		 if (force) {
			 xGroupGroupService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXGroupGroupList searchXGroupGroups(SearchCriteria searchCriteria) {
		return xGroupGroupService.searchXGroupGroups(searchCriteria);
	}

	public VXLong getXGroupGroupSearchCount(SearchCriteria searchCriteria) {
		return xGroupGroupService.getSearchCount(searchCriteria,
				xGroupGroupService.searchFields);
	}

	public VXPermMap getXPermMap(Long id){
		return (VXPermMap)xPermMapService.readResource(id);
	}

	public VXPermMap createXPermMap(VXPermMap vXPermMap){
		vXPermMap =  (VXPermMap)xPermMapService.createResource(vXPermMap);
		return vXPermMap;
	}

	public VXPermMap updateXPermMap(VXPermMap vXPermMap) {
		vXPermMap =  (VXPermMap)xPermMapService.updateResource(vXPermMap);
		return vXPermMap;
	}

	public void deleteXPermMap(Long id, boolean force) {
		 if (force) {
			 xPermMapService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXPermMapList searchXPermMaps(SearchCriteria searchCriteria) {
		return xPermMapService.searchXPermMaps(searchCriteria);
	}

	public VXLong getXPermMapSearchCount(SearchCriteria searchCriteria) {
		return xPermMapService.getSearchCount(searchCriteria,
				xPermMapService.searchFields);
	}

	public VXAuditMap getXAuditMap(Long id){
		return (VXAuditMap)xAuditMapService.readResource(id);
	}

	public VXAuditMap createXAuditMap(VXAuditMap vXAuditMap){
		vXAuditMap =  (VXAuditMap)xAuditMapService.createResource(vXAuditMap);
		return vXAuditMap;
	}

	public VXAuditMap updateXAuditMap(VXAuditMap vXAuditMap) {
		vXAuditMap =  (VXAuditMap)xAuditMapService.updateResource(vXAuditMap);
		return vXAuditMap;
	}

	public void deleteXAuditMap(Long id, boolean force) {
		 if (force) {
			 xAuditMapService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXAuditMapList searchXAuditMaps(SearchCriteria searchCriteria) {
		return xAuditMapService.searchXAuditMaps(searchCriteria);
	}

	public VXLong getXAuditMapSearchCount(SearchCriteria searchCriteria) {
		return xAuditMapService.getSearchCount(searchCriteria,
				xAuditMapService.searchFields);
	}

}
