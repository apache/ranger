package org.apache.ranger.services.yarn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.yarn.client.YarnResourceMgr;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RangerServiceYarn extends RangerBaseService {

	private static final Log LOG = LogFactory.getLog(RangerServiceYarn.class);
	
	public RangerServiceYarn() {
		super();
	}
	
	@Override
	public void init(RangerServiceDef serviceDef, RangerService service) {
		super.init(serviceDef, service);
	}

	@Override
	public HashMap<String,Object> validateConfig() throws Exception {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		String 	serviceName  	    = getServiceName();
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceYarn.validateConfig Service: (" + serviceName + " )");
		}
		if ( configs != null) {
			try  {
				ret = YarnResourceMgr.validateConfig(serviceName, configs);
			} catch (Exception e) {
				LOG.error("<== RangerServiceYarn.validateConfig Error:" + e);
				throw e;
			}
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceYarn.validateConfig Response : (" + ret + " )");
		}
		return ret;
	}

	@Override
	public List<String> lookupResource(ResourceLookupContext context) throws Exception {
		
		List<String> ret 		   = new ArrayList<String>();
		String 	serviceName  	   = getServiceName();
		Map<String,String> configs = getConfigs();
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceYarn.lookupResource Context: (" + context + ")");
		}
		if (context != null) {
			try {
				ret  = YarnResourceMgr.getYarnResources(serviceName,configs,context);
			} catch (Exception e) {
			  LOG.error( "<==RangerServiceYarn.lookupResource Error : " + e);
			  throw e;
			}
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceYarn.lookupResource Response: (" + ret + ")");
		}
		return ret;
	}
}

