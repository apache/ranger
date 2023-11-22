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

 package org.apache.ranger.biz;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.service.RangerTrxLogV2Service;
import org.apache.ranger.service.XAccessAuditService;
import org.apache.ranger.view.VXAccessAudit;
import org.apache.ranger.view.VXAccessAuditList;
import org.apache.ranger.view.VXLong;
import org.apache.ranger.view.VXTrxLog;
import org.apache.ranger.view.VXTrxLogList;
import org.apache.ranger.view.VXTrxLogV2;
import org.springframework.beans.factory.annotation.Autowired;

public class XAuditMgrBase {

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	RangerTrxLogV2Service xTrxLogService;

	@Autowired
	XAccessAuditService xAccessAuditService;
	public VXTrxLog getXTrxLog(Long id){
		VXTrxLogV2 ret = xTrxLogService.readResource(id);

		return ret != null ? VXTrxLogV2.toVXTrxLog(ret) : null;
	}

	public VXTrxLog createXTrxLog(VXTrxLog vXTrxLog){
		VXTrxLogV2 ret = xTrxLogService.createResource(new VXTrxLogV2(vXTrxLog));

		return ret != null ? VXTrxLogV2.toVXTrxLog(ret) : null;
	}

	public VXTrxLog updateXTrxLog(VXTrxLog vXTrxLog) {
		VXTrxLogV2 ret =  xTrxLogService.updateResource(new VXTrxLogV2(vXTrxLog));

		return ret != null ? VXTrxLogV2.toVXTrxLog(ret) : null;
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
		PList<VXTrxLogV2> vXTrxLogsV2 = xTrxLogService.searchTrxLogs(searchCriteria);
		List<VXTrxLog>    vxTrxLogs   = vXTrxLogsV2.getList().stream().map(VXTrxLogV2::toVXTrxLog).collect(Collectors.toList());
		VXTrxLogList      ret         = new VXTrxLogList(vxTrxLogs);

		ret.setStartIndex(vXTrxLogsV2.getStartIndex());
		ret.setPageSize(vXTrxLogsV2.getPageSize());
		ret.setTotalCount(vXTrxLogsV2.getTotalCount());
		ret.setResultSize(vXTrxLogsV2.getResultSize());
		ret.setSortBy(vXTrxLogsV2.getSortBy());
		ret.setSortType(vXTrxLogsV2.getSortType());

		return ret;
	}

	public VXLong getXTrxLogSearchCount(SearchCriteria searchCriteria) {
		long count = xTrxLogService.getTrxLogsCount(searchCriteria);

		VXLong ret = new VXLong();

		ret.setValue(count);

		return ret;
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
