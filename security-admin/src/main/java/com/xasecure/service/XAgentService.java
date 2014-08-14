package com.xasecure.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.xasecure.common.DateUtil;
import com.xasecure.common.PropertiesUtil;
import com.xasecure.common.SearchCriteria;
import com.xasecure.common.StringUtil;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.xasecure.common.AppConstants;
import com.xasecure.common.TimedEventUtil;
import com.xasecure.view.VXAuditRecord;
import com.xasecure.view.VXAuditRecordList;

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
		defaultDBDateFormat = PropertiesUtil.getProperty(
				"xa.db.defaultDateformat", defaultDBDateFormat);
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
			ret = loggerName.startsWith("com.xasecure.authorization.hive")  || loggerName.startsWith("com.xasecure.pdp.hive.") ;
		}
		else {
			ret = (fieldCount == 11) ;
		}
		return ret;
	}

	private boolean isHBaseLog(String loggerName, int fieldCount) {
		boolean ret = false ;
		if (loggerName != null) {
			ret = loggerName.startsWith("com.xasecure.authorization.hbase") ;
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
		StringBuffer whereClause = new StringBuffer();
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
