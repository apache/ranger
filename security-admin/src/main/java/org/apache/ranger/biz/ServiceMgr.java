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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.view.VXMessage;
import org.apache.ranger.view.VXResponse;
import org.springframework.stereotype.Component;

@Component
public class ServiceMgr {

	private static final Log LOG = LogFactory.getLog(ServiceMgr.class);
	
	
	public List<String> lookupResource(String serviceName, ResourceLookupContext context, ServiceStore svcStore) throws Exception {
		List<String> 	  ret = null;
		
		RangerBaseService svc = getRangerServiceByName(serviceName, svcStore);

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.lookupResource for Service: (" + svc + "Context: " + context + ")");
		}

		if ( svc != null) {
			try {
				ret = svc.lookupResource(context);
			} catch ( Exception e) {
				LOG.error("==> ServiceMgr.lookupResource Error:" + e);
				throw e;
			}
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.lookupResource for Response: (" + ret + ")");
		}
		
		return ret;
	}
	
	public VXResponse validateConfig(RangerService service, ServiceStore svcStore) throws Exception {
		
		VXResponse ret 			= new VXResponse();
		RangerBaseService svc 	= getRangerServiceByService(service, svcStore);
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.validateConfig for Service: (" + svc + ")");
		}
		
		if (svc != null) {
			try {
				HashMap<String, Object> responseData = svc.validateConfig();
				ret = generateResponseForTestConn(responseData, "");
			} catch (Exception e) {
				LOG.error("==> ServiceMgr.validateConfig Error:" + e);
				throw e;
			}
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.validateConfig for Response: (" + ret + ")");
		}
		
		return ret;
	}

	
  public RangerBaseService getRangerServiceByName(String serviceName, ServiceStore svcStore) throws Exception{
	    RangerBaseService   svc 	= null;
	   	RangerService     	service = svcStore.getServiceByName(serviceName);
	  	
	  	if ( service != null) {
	  		svc = getRangerServiceByService(service, svcStore);
	  	}	
		return svc;
	}
	
	public RangerBaseService getRangerServiceByService(RangerService service, ServiceStore svcStore) throws Exception{
		
		RangerServiceDef 	serviceDef 	= null;
		RangerBaseService	ret 		= null;
		
		String	serviceType = service.getType();
		
		if (serviceType != null) {
			serviceDef  = svcStore.getServiceDefByName(serviceType);
			if ( serviceDef != null) {	
				ret  = (RangerBaseService) Class.forName(serviceDef.getImplClass()).newInstance();
			}
			
			ret.init(serviceDef, service);	
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceMgr.getRangerServiceByService ServiceType: " + serviceType + "ServiceDef: " + serviceDef + "Service Class: " + serviceDef.getImplClass());
		}		
		
		return ret;
	}
	
	private VXResponse generateResponseForTestConn(
			HashMap<String, Object> responseData, String msg) {
		VXResponse vXResponse = new VXResponse();

		Long objId = (responseData.get("objectId") != null) ? Long
				.parseLong(responseData.get("objectId").toString()) : null;
		boolean connectivityStatus = (responseData.get("connectivityStatus") != null) ? Boolean
				.parseBoolean(responseData.get("connectivityStatus").toString())
				: false;
		int statusCode = (connectivityStatus) ? VXResponse.STATUS_SUCCESS
				: VXResponse.STATUS_ERROR;
		String message = (responseData.get("message") != null) ? responseData
				.get("message").toString() : msg;
		String description = (responseData.get("description") != null) ? responseData
				.get("description").toString() : msg;
		String fieldName = (responseData.get("fieldName") != null) ? responseData
				.get("fieldName").toString() : null;

		VXMessage vXMsg = new VXMessage();
		List<VXMessage> vXMsgList = new ArrayList<VXMessage>();
		vXMsg.setFieldName(fieldName);
		vXMsg.setMessage(message);
		vXMsg.setObjectId(objId);
		vXMsgList.add(vXMsg);

		vXResponse.setMessageList(vXMsgList);
		vXResponse.setMsgDesc(description);
		vXResponse.setStatusCode(statusCode);
		return vXResponse;
	}
}

