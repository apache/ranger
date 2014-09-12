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

 package com.xasecure.common;

import com.xasecure.common.*;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;

/**
 * 
 *
 */
@Provider
public class XAJAXBContextResolver implements ContextResolver<JAXBContext> {

    private JAXBContext context;
    private Class<?>[] types = {
	com.xasecure.view.VXAuthSessionList.class,
	com.xasecure.view.VXResponse.class,
	com.xasecure.view.VXStringList.class,
	com.xasecure.view.VXPortalUserList.class,
	com.xasecure.view.VXAssetList.class,
	com.xasecure.view.VXResourceList.class,
	com.xasecure.view.VXCredentialStoreList.class,
	com.xasecure.view.VXGroupList.class,
	com.xasecure.view.VXUserList.class,
	com.xasecure.view.VXGroupUserList.class,
	com.xasecure.view.VXGroupGroupList.class,
	com.xasecure.view.VXPermMapList.class,
	com.xasecure.view.VXAuditMapList.class,
	com.xasecure.view.VXPolicyExportAuditList.class,
	com.xasecure.view.VXAccessAuditList.class
    };

    public XAJAXBContextResolver() throws Exception {
	JSONConfiguration config = JSONConfiguration.natural().build();
	context = new JSONJAXBContext(config, types);
    }

    @Override
    public JAXBContext getContext(Class<?> objectType) {
	// return context;
	for (Class<?> type : types) {
	    if (type.getName().equals(objectType.getName())) {
		return context;
	    }
	}
	return null;
    }
}

