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
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.common.annotation.XAAnnotationClassName;
import org.apache.ranger.common.annotation.XAAnnotationJSMgrName;
import org.apache.ranger.rest.*;
import org.apache.ranger.service.*;
import org.apache.ranger.view.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Path("xaudit")
@Component
@Scope("request")
@XAAnnotationJSMgrName("XAuditMgr")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class XAuditREST {
	static Logger logger = Logger.getLogger(XAuditREST.class);

	@Autowired
	SearchUtil searchUtil;

	@Autowired
	XAuditMgr xAuditMgr;

	@Autowired
	XTrxLogService xTrxLogService;

	@Autowired
	XAccessAuditService xAccessAuditService;
	// Handle XTrxLog
	@GET
	@Path("/trx_log/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXTrxLog getXTrxLog(
			@PathParam("id") Long id) {
		 return xAuditMgr.getXTrxLog(id);
	}

	@POST
	@Path("/trx_log")
	@Produces({ "application/xml", "application/json" })
	public VXTrxLog createXTrxLog(VXTrxLog vXTrxLog) {
		 return xAuditMgr.createXTrxLog(vXTrxLog);
	}

	@PUT
	@Path("/trx_log")
	@Produces({ "application/xml", "application/json" })
	public VXTrxLog updateXTrxLog(VXTrxLog vXTrxLog) {
		 return xAuditMgr.updateXTrxLog(vXTrxLog);
	}

	@DELETE
	@Path("/trx_log/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@XAAnnotationClassName(class_name = VXTrxLog.class)
	public void deleteXTrxLog(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		 boolean force = false;
		 xAuditMgr.deleteXTrxLog(id, force);
	}

	/**
	 * Implements the traditional search functionalities for XTrxLogs
	 *
	 * @param request
	 * @return
	 */
	@GET
	@Path("/trx_log")
	@Produces({ "application/xml", "application/json" })
	public VXTrxLogList searchXTrxLogs(@Context HttpServletRequest request) {
		 SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
		 request, xTrxLogService.sortFields);
		 return xAuditMgr.searchXTrxLogs(searchCriteria);
	}

	@GET
	@Path("/trx_log/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countXTrxLogs(@Context HttpServletRequest request) {
		 SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
		 request, xTrxLogService.sortFields);

		 return xAuditMgr.getXTrxLogSearchCount(searchCriteria);
	}


	/**
	 * Implements the traditional search functionalities for XAccessAudits
	 *
	 * @param request
	 * @return
	 */
	@GET
	@Path("/access_audit")
	@Produces({ "application/xml", "application/json" })
	public VXAccessAuditList searchXAccessAudits(@Context HttpServletRequest request) {
		 SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
		 request, xAccessAuditService.sortFields);
		 return xAuditMgr.searchXAccessAudits(searchCriteria);
	}

	@GET
	@Path("/access_audit/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countXAccessAudits(@Context HttpServletRequest request) {
		 SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
		 request, xAccessAuditService.sortFields);

		 return xAuditMgr.getXAccessAuditSearchCount(searchCriteria);
	}

}
