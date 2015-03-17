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

import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.solr.SolrAccessAuditsService;
import org.apache.ranger.view.VXAccessAudit;
import org.apache.ranger.view.VXAccessAuditList;
import org.apache.ranger.view.VXLong;
import org.springframework.beans.factory.annotation.Autowired;

public class XAuditMgr extends XAuditMgrBase {

	@Autowired
	SolrAccessAuditsService solrAccessAuditsService;

	@Autowired
	RangerBizUtil rangerBizUtil;

	@Override
	public VXAccessAudit getXAccessAudit(Long id) {
		// TODO Auto-generated method stub
		return super.getXAccessAudit(id);
	}

	@Override
	public VXAccessAuditList searchXAccessAudits(SearchCriteria searchCriteria) {
		if (rangerBizUtil.getAuditDBType().equalsIgnoreCase("solr")) {
			return solrAccessAuditsService.searchXAccessAudits(searchCriteria);
		} else {
			return super.searchXAccessAudits(searchCriteria);
		}
	}

	@Override
	public VXLong getXAccessAuditSearchCount(SearchCriteria searchCriteria) {
		if (rangerBizUtil.getAuditDBType().equalsIgnoreCase("solr")) {
			return solrAccessAuditsService
					.getXAccessAuditSearchCount(searchCriteria);
		} else {
			return super.getXAccessAuditSearchCount(searchCriteria);
		}
	}

}
