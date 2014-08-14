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
public class XAuditMgrBase {

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	XTrxLogService xTrxLogService;

	@Autowired
	XAccessAuditService xAccessAuditService;
	public VXTrxLog getXTrxLog(Long id){
		return (VXTrxLog)xTrxLogService.readResource(id);
	}

	public VXTrxLog createXTrxLog(VXTrxLog vXTrxLog){
		vXTrxLog =  (VXTrxLog)xTrxLogService.createResource(vXTrxLog);
		return vXTrxLog;
	}

	public VXTrxLog updateXTrxLog(VXTrxLog vXTrxLog) {
		vXTrxLog =  (VXTrxLog)xTrxLogService.updateResource(vXTrxLog);
		return vXTrxLog;
	}

	public void deleteXTrxLog(Long id, boolean force) {
		 if (force) {
			 xTrxLogService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXTrxLogList searchXTrxLogs(SearchCriteria searchCriteria) {
		return xTrxLogService.searchXTrxLogs(searchCriteria);
	}

	public VXLong getXTrxLogSearchCount(SearchCriteria searchCriteria) {
		return xTrxLogService.getSearchCount(searchCriteria,
				xTrxLogService.searchFields);
	}

	public VXAccessAudit getXAccessAudit(Long id){
		return (VXAccessAudit)xAccessAuditService.readResource(id);
	}

	public VXAccessAudit createXAccessAudit(VXAccessAudit vXAccessAudit){
		vXAccessAudit =  (VXAccessAudit)xAccessAuditService.createResource(vXAccessAudit);
		return vXAccessAudit;
	}

	public VXAccessAudit updateXAccessAudit(VXAccessAudit vXAccessAudit) {
		vXAccessAudit =  (VXAccessAudit)xAccessAuditService.updateResource(vXAccessAudit);
		return vXAccessAudit;
	}

	public void deleteXAccessAudit(Long id, boolean force) {
		 if (force) {
			 xAccessAuditService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXAccessAuditList searchXAccessAudits(SearchCriteria searchCriteria) {
		return xAccessAuditService.searchXAccessAudits(searchCriteria);
	}

	public VXLong getXAccessAuditSearchCount(SearchCriteria searchCriteria) {
		return xAccessAuditService.getSearchCount(searchCriteria,
				xAccessAuditService.searchFields);
	}

}
