package org.apache.ranger.services.hive.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;

public class HiveResourceMgr {

	public static final Logger LOG = Logger.getLogger(HiveResourceMgr.class);
	
	private static final String  DATABASE 	  = "database";
	private static final String  TABLE	 	  = "table";
	private static final String  UDF	 	  = "udf";
	private static final String  COLUMN	 	  = "column";

	
	public static HashMap<String, Object> testConnection(String serviceName, Map<String, String> configs) throws Exception {
		HashMap<String, Object> ret = null;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== HiveResourceMgr.testConnection ServiceName: "+ serviceName + "Configs" + configs ) ;
		}	
		
		try {
			ret = HiveClient.testConnection(serviceName, configs);
		} catch (Exception e) {
			LOG.error("<== HiveResourceMgr.testConnection Error: " + e) ;
		  throw e;
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== HiveResourceMgr.testConnection Result : "+ ret  ) ;
		}	
		
		return ret;
	}

	public static List<String> getHiveResources(String serviceName, Map<String, String> configs,ResourceLookupContext context) throws Exception  {
		
		
		String 					  	userInput    = context.getUserInput();
		String 					  	resource	 = context.getResourceName();
		Map<String, List<String>> 	resourceMap  = context.getResources();
		List<String> 			  	resultList 	 = null;
		List<String> 			  	databaseList = null;
		List<String> 				tableList	 = null;
		List<String> 				udfList	  	 = null;
		List<String> 				columnList	 = null;
		String  					databaseName = null;
		String  					tableName	 = null;
		String  					udfName	  	 = null;
		String  					columnName	 = null;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== HiveResourceMgr.getHiveResources()  UserInput: \""+ userInput  + "\" resource : " + resource + " resourceMap: "  + resourceMap) ;
		}	
		
		if ( userInput != null && resource != null) {
			if ( resourceMap != null  && !resourceMap.isEmpty() &&
			   ( resourceMap.get(DATABASE) != null  ||  resourceMap.get(TABLE) != null ||
			   resourceMap.get(UDF) != null  ||  resourceMap.get(COLUMN) != null	) ) { 
				switch (resource.trim().toLowerCase()) {
				case DATABASE:
						databaseName = userInput;
						databaseList = resourceMap.get(DATABASE); 
						break;
				case TABLE:
						tableName = userInput;
						tableList = resourceMap.get(TABLE); 
						break;
				case UDF:
						udfName = userInput;
						udfList = resourceMap.get(UDF); 
						break;
				case COLUMN:
						columnName = userInput;
						columnList = resourceMap.get(COLUMN); 
						break;
				default:
						break;
				}
			} else {
				switch (resource.trim().toLowerCase()) {
				case DATABASE:
						databaseName = userInput;
						break;
				case TABLE:
						tableName = userInput;
						break;
				case UDF:
						udfName = userInput;
						break;
				case COLUMN:
						columnName = userInput;
						break;
				default:
						break;
				}
			}
		}
		
		if (serviceName != null && userInput != null) {
			try {
				
				if(LOG.isDebugEnabled()) {
					LOG.debug("<== HiveResourceMgr.getHiveResources() UserInput: "+ userInput  + " configs: " + configs + " context: "  + context) ;
				}
				
				final HiveClient hiveClient = new HiveConnectionMgr().getHiveConnection(serviceName, configs);
				
				Callable<List<String>> callableObj = null;
				final String finalDbName;
				final String finalColName;
				final String finalTableName;
				
				final List<String> finaldatabaseList = databaseList;
				final List<String> finaltableList 	 = tableList;
				final List<String> finaldfsList		 = udfList;
				final List<String> finalcolumnList   = columnList;
				
				if (hiveClient != null && databaseName != null
						&& !databaseName.isEmpty()) {
					if (tableName != null && !tableName.isEmpty()) {
						if (columnName != null && !columnName.isEmpty()) {
							columnName += "*";
							finalColName = columnName;
							finalDbName = databaseName;
							finalTableName = tableName;

							callableObj = new Callable<List<String>>() {
								@Override
								public List<String> call() {
									return hiveClient.getColumnList(finalColName,
																	finaldatabaseList,
																	finaltableList,
																	finalcolumnList);
								}
							};
						} else {
							tableName += "*";
							finalTableName = tableName;
							finalDbName = databaseName;
							callableObj = new Callable<List<String>>() {

								@Override
								public List<String> call() {
									return hiveClient.getTableList(finalTableName,
											   					   finaldatabaseList,
											   					   finaltableList);
								}
							
							};
						}
					} else {
						databaseName += "*";
						finalDbName = databaseName;
						callableObj = new Callable<List<String>>() {
							@Override
							public List<String> call() {
								return hiveClient.getDatabaseList(finalDbName,
																  finaldatabaseList);
							}
						};

					}
						
					synchronized (hiveClient) {
						resultList = TimedEventUtil.timedTask(callableObj, 5,
								TimeUnit.SECONDS);
					}
				 }
			  } catch (Exception e) {
				LOG.error("Unable to get hive resources.", e);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== HiveResourceMgr.getHiveResources() databaseName:" + databaseName + " tableName: " + tableName  + " columnName: " + columnName + "Result :" + resultList) ;
		}
		return resultList;
	
	}
	
}
