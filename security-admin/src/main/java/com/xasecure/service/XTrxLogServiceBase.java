package com.xasecure.service;
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

/**
 * 
 */

import java.util.ArrayList;
import java.util.List;

import com.xasecure.common.*;
import com.xasecure.entity.*;
import com.xasecure.view.*;
import com.xasecure.service.*;

public abstract class XTrxLogServiceBase<T extends XXTrxLog, V extends VXTrxLog>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XTrxLog";

	public XTrxLogServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXTrxLog mapViewToEntityBean(VXTrxLog vObj, XXTrxLog mObj, int OPERATION_CONTEXT) {
		mObj.setObjectClassType( vObj.getObjectClassType());
		mObj.setObjectId( vObj.getObjectId());
		mObj.setParentObjectId( vObj.getParentObjectId());
		mObj.setParentObjectClassType( vObj.getParentObjectClassType());
		mObj.setParentObjectName( vObj.getParentObjectName());
		mObj.setObjectName( vObj.getObjectName());
		mObj.setAttributeName( vObj.getAttributeName());
		mObj.setPreviousValue( vObj.getPreviousValue());
		mObj.setNewValue( vObj.getNewValue());
		mObj.setTransactionId( vObj.getTransactionId());
		mObj.setAction( vObj.getAction());
		mObj.setSessionId( vObj.getSessionId());
		mObj.setRequestId( vObj.getRequestId());
		mObj.setSessionType( vObj.getSessionType());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXTrxLog mapEntityToViewBean(VXTrxLog vObj, XXTrxLog mObj) {
		vObj.setObjectClassType( mObj.getObjectClassType());
		vObj.setObjectId( mObj.getObjectId());
		vObj.setParentObjectId( mObj.getParentObjectId());
		vObj.setParentObjectClassType( mObj.getParentObjectClassType());
		vObj.setParentObjectName( mObj.getParentObjectName());
		vObj.setObjectName( mObj.getObjectName());
		vObj.setAttributeName( mObj.getAttributeName());
		vObj.setPreviousValue( mObj.getPreviousValue());
		vObj.setNewValue( mObj.getNewValue());
		vObj.setTransactionId( mObj.getTransactionId());
		vObj.setAction( mObj.getAction());
		vObj.setSessionId( mObj.getSessionId());
		vObj.setRequestId( mObj.getRequestId());
		vObj.setSessionType( mObj.getSessionType());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXTrxLogList searchXTrxLogs(SearchCriteria searchCriteria) {
		VXTrxLogList returnList = new VXTrxLogList();
		List<VXTrxLog> xTrxLogList = new ArrayList<VXTrxLog>();

		@SuppressWarnings("unchecked")
		List<XXTrxLog> resultList = (List<XXTrxLog>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXTrxLog gjXTrxLog : resultList) {
			@SuppressWarnings("unchecked")
			VXTrxLog vXTrxLog = populateViewBean((T)gjXTrxLog);
			xTrxLogList.add(vXTrxLog);
		}

		returnList.setVXTrxLogs(xTrxLogList);
		return returnList;
	}

}
