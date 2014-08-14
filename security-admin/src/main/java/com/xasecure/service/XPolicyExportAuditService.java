package com.xasecure.service;

import com.xasecure.common.SearchField;
import com.xasecure.common.SearchField.DATA_TYPE;
import com.xasecure.common.SearchField.SEARCH_TYPE;
import com.xasecure.common.SortField.SORT_ORDER;
import com.xasecure.common.SortField;

import com.xasecure.view.*;
import com.xasecure.entity.*;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class XPolicyExportAuditService extends XPolicyExportAuditServiceBase<XXPolicyExportAudit, VXPolicyExportAudit> {

	public XPolicyExportAuditService(){
		searchFields.add(new SearchField("httpRetCode", "obj.httpRetCode",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("clientIP", "obj.clientIP",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("agentId", "obj.agentId",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("repositoryName", "obj.repositoryName",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("startDate", "obj.createTime", 
				DATA_TYPE.DATE, SEARCH_TYPE.GREATER_EQUAL_THAN));
		searchFields.add(new SearchField("endDate", "obj.createTime", 
				DATA_TYPE.DATE, SEARCH_TYPE.LESS_EQUAL_THAN));
		
		sortFields.add(new SortField("createDate", "obj.createTime", true, SORT_ORDER.DESC));
	}
	
	@Override
	protected void validateForCreate(VXPolicyExportAudit vObj) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateForUpdate(VXPolicyExportAudit vObj, XXPolicyExportAudit mObj) {
		// TODO Auto-generated method stub

	}

}
