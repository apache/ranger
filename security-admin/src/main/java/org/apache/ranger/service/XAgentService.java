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

 package org.apache.ranger.service;

import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class XAgentService {

	@Autowired
	StringUtil stringUtil;

	@Autowired
	DateUtil dateUtil;
	
	private static Logger logger = Logger.getLogger(XAgentService.class);
	
	protected String defaultDBDateFormat="yyyy-MM-dd";
	protected boolean auditSupported = false;
	
	public XAgentService() {
		defaultDBDateFormat = PropertiesUtil.getProperty("ranger.db.defaultDateformat", defaultDBDateFormat);
		auditSupported = PropertiesUtil.getBooleanProperty("xa.audit.supported", 
				false);
	}
	
	private boolean isHDFSLog(String loggerName, int fieldCount) {
		boolean ret = false ;
		if (loggerName != null) {
			ret = loggerName.startsWith("org.") ;
		}
		else {
			ret = (fieldCount == 5) ;
		}
		return ret;
	}
	
	private boolean isHiveLog(String loggerName, int fieldCount) {
		boolean ret = false ;
		if (loggerName != null) {
			ret = loggerName.startsWith("org.apache.ranger.authorization.hive")  || loggerName.startsWith("org.apache.ranger.pdp.hive.") ;
		}
		else {
			ret = (fieldCount == 11) ;
		}
		return ret;
	}

	private boolean isHBaseLog(String loggerName, int fieldCount) {
		boolean ret = false ;
		if (loggerName != null) {
			ret = loggerName.startsWith("org.apache.ranger.authorization.hbase") ;
		}
		else {
			ret = ((fieldCount != 5) && (fieldCount != 11)) ;
		}
		return ret;
	}


	// The resource type field in the message has "@" at the start
	// remove and then compare
	
	private int getResourceType(String field) {
		field = field.startsWith("@") ? field.substring(1) : field;
		
		int resourceType = AppConstants.RESOURCE_UNKNOWN;
		if (field.equals("db")) {
			resourceType = AppConstants.RESOURCE_DB;
		} else if (field.equals("table")) {
			resourceType = AppConstants.RESOURCE_TABLE;
		} else if (field.equals("column")) {
			resourceType = AppConstants.RESOURCE_COLUMN;
		}
		
		return resourceType;
	}

	private String bulidWhereClause(SearchCriteria searchCriteria) {
		StringBuilder whereClause = new StringBuilder();
		Date startDate = (Date) searchCriteria.getParamValue("startDate");
		Date endDate = (Date) searchCriteria.getParamValue("endDate");

		if (startDate == null) {
			startDate = new Date(0);
		}

		if (endDate == null) {
			endDate = DateUtil.getUTCDate();

		}
		String startDateString = DateUtil.dateToString(startDate, defaultDBDateFormat);
		String endDateString = DateUtil.dateToString(endDate, defaultDBDateFormat);
		
		whereClause.append(" DATE(inserted_date)  BETWEEN  '" + startDateString
				+ "' AND  '" + endDateString + "'");
		
		if (whereClause.length() != 0) {
			return "WHERE " + whereClause.toString();
		}
		
		return "";
	}

}
