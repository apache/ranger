package org.apache.ranger.services.knox.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.service.ResourceLookupContext;


public class KnoxResourceMgr {

	public static final Logger LOG = Logger.getLogger(KnoxResourceMgr.class);
	
	private static final String TOPOLOGY	  	 = "topology";
	private static final String SERVICE 	 	 = "service";

	public static HashMap<String, Object> validateConfig(String serviceName, Map<String, String> configs) throws Exception {
		HashMap<String, Object> ret = null;
		if (LOG.isDebugEnabled()) {
		   LOG.debug("==> KnoxResourceMgr.testConnection ServiceName: "+ serviceName + "Configs" + configs ) ;
		}
		try {
			ret = KnoxClient.testConnection(serviceName, configs);
		} catch (Exception e) {
		  LOG.error("<== KnoxResourceMgr.testConnection Error: " + e) ;
		  throw e;
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== KnoxResourceMgr.HdfsResourceMgr Result : "+ ret  ) ;
		}
		return ret;
	 }
	
	public static List<String> getKnoxResources(String serviceName, Map<String, String> configs, ResourceLookupContext context) throws Exception  {
		
		
		String 		 userInput 				  = context.getUserInput();
		String 		 resource				  = context.getResourceName();
		Map<String, List<String>> resourceMap = context.getResources();
		List<String> resultList 			  = null;
		List<String> knoxTopologyList		  = null; 
		List<String> knoxServiceList		  = null;
		String  	 knoxTopologyName		  = null;
		String  	 knoxServiceName		  = null;
		
		if ( userInput != null && resource != null) {
		   if  ( resourceMap != null && !resourceMap.isEmpty() &&
			   ( resourceMap.get(TOPOLOGY) != null || resourceMap.get(SERVICE) != null) ) {
	 		 	switch (resource.trim().toLowerCase()) {
				case TOPOLOGY:
					 knoxTopologyName = userInput;
					 knoxTopologyList = resourceMap.get(TOPOLOGY); 
					 break;
				case SERVICE:
					 knoxServiceName = userInput;
					 knoxServiceList = resourceMap.get(SERVICE); 
					 break;
				default:
					 break;
				}
	 	  } else {
			 switch (resource.trim().toLowerCase()) {
			 case TOPOLOGY:
				 knoxTopologyName = userInput;
				 break;
			case SERVICE:
				 knoxServiceName = userInput;
				 break;
			default:
				 break;
			 }  
		   }
		}
		
		String knoxUrl = configs.get("knox.url");
		String knoxAdminUser = configs.get("username");
		String knoxAdminPassword = configs.get("password");

		if (knoxUrl == null || knoxUrl.isEmpty()) {
			LOG.error("Unable to get knox resources: knoxUrl is empty");
			return resultList;
		} else if (knoxAdminUser == null || knoxAdminUser.isEmpty()) {
			LOG.error("Unable to get knox resources: knoxAdminUser is empty");
			return resultList;
		} else if (knoxAdminPassword == null || knoxAdminPassword.isEmpty()) {
			LOG.error("Unable to get knox resources: knoxAdminPassword is empty");
			return resultList;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== KnoxResourceMgr.getKnoxResources()  knoxUrl: "+ knoxUrl  + " knoxAdminUser: " + knoxAdminUser + " topologyName: "  + knoxTopologyName + " KnoxServiceName: " + knoxServiceName) ;
		}
		
		final KnoxClient knoxClient = new KnoxConnectionMgr().getKnoxClient(knoxUrl, knoxAdminUser, knoxAdminPassword); 
		resultList = KnoxClient.getKnoxResources(knoxClient, knoxTopologyName, knoxServiceName,knoxTopologyList,knoxServiceList);

		return  resultList;
	}
	
	
}
