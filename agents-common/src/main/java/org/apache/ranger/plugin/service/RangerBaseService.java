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

package org.apache.ranger.plugin.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;


public abstract class RangerBaseService {
	private RangerServiceDef serviceDef;
	private RangerService    service;
	
	protected Map<String, String>   configs;
	protected String 			    serviceName;
	protected String 				serviceType;
	

	public void init(RangerServiceDef serviceDef, RangerService service) {
		this.serviceDef    = serviceDef;
		this.service       = service;
		this.configs	   = service.getConfigs();
		this.serviceName   = service.getName();
		this.serviceType   = service.getType();
	}

	/**
	 * @return the serviceDef
	 */
	public RangerServiceDef getServiceDef() {
		return serviceDef;
	}

	/**
	 * @return the service
	 */
	public RangerService getService() {
		return service;
	}

	public Map<String, String> getConfigs() {
		return configs;
	}

	public void setConfigs(Map<String, String> configs) {
		this.configs = configs;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getServiceType() {
		return serviceType;
	}

	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}
		
	public abstract HashMap<String, Object> validateConfig() throws Exception;
	
	public abstract List<String> lookupResource(ResourceLookupContext context) throws Exception;
	
	
	
	
}
