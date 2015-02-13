package org.apache.ranger.services.storm.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.service.ResourceLookupContext;

public class StormResourceMgr {
	public static final 	Logger 	LOG 		= Logger.getLogger(StormResourceMgr.class);
	private static final 	String  TOPOLOGY	= "topology";
	
	public static HashMap<String, Object> validateConfig(String serviceName, Map<String, String> configs) throws Exception {
		HashMap<String, Object> ret = null;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> StormResourceMgr.validateConfig ServiceName: "+ serviceName + "Configs" + configs ) ;
		}	
		
		try {
			ret = StormClient.testConnection(serviceName, configs);
		} catch (Exception e) {
			LOG.error("<== StormResourceMgr.validateConfig Error: " + e) ;
		  throw e;
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== StormResourceMgr.validateConfig Result : "+ ret  ) ;
		}	
		return ret;
	}
	
    public static List<String> getStormResources(String serviceName, Map<String, String> configs,ResourceLookupContext context) {
        String 		 userInput 				  = context.getUserInput();
		String 		 resource				  = context.getResourceName();
		Map<String, List<String>> resourceMap = context.getResources();
	    List<String> 		resultList        = null;
		List<String> 		StormTopologyList = null;
		String  			StromTopologyName = null;
		
		if ( resourceMap != null && !resourceMap.isEmpty() &&
			resourceMap.get(TOPOLOGY) != null ) {
			StromTopologyName = userInput;
			StormTopologyList = resourceMap.get(TOPOLOGY); 
		} else {
			StromTopologyName = userInput;
		}
		
		
        if (configs == null || configs.isEmpty()) {
                LOG.error("Connection Config is empty");

        } else {
                
                String url 		= configs.get("nimbus.url");
                String username = configs.get("username");
                String password = configs.get("password");
                resultList = getStormResources(url, username, password,StromTopologyName,StormTopologyList) ;
        }
        return resultList ;
    }

    public static List<String> getStormResources(String url, String username, String password,String topologyName, List<String> StormTopologyList) {
        final StormClient stormClient = StormConnectionMgr.getStormClient(url, username, password);
        List<String> topologyList = stormClient.getTopologyList(topologyName,StormTopologyList) ;
        return topologyList;
    }
    
}
