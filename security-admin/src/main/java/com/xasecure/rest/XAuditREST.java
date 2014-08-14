package com.xasecure.rest;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;

import com.xasecure.common.SearchCriteria;
import com.xasecure.common.SearchUtil;
import com.xasecure.service.*;
import com.xasecure.biz.*;
import com.xasecure.view.*;

import com.xasecure.rest.*;
import com.xasecure.biz.*;
import com.xasecure.common.annotation.XAAnnotationClassName;
import com.xasecure.common.annotation.XAAnnotationJSMgrName;
import com.xasecure.view.*;
import com.xasecure.service.*;

import org.apache.log4j.Logger;
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
