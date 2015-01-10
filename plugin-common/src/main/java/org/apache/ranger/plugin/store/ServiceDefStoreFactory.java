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

package org.apache.ranger.plugin.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.store.file.ServiceDefFileStore;


public class ServiceDefStoreFactory {
	private static final Log LOG = LogFactory.getLog(ServiceDefStoreFactory.class);

	private static ServiceDefStoreFactory sInstance = null;

	private ServiceDefStore serviceDefStore = null;


	public static ServiceDefStoreFactory instance() {
		if(sInstance == null) {
			sInstance = new ServiceDefStoreFactory();
		}

		return sInstance;
	}

	public ServiceDefStore getServiceDefStore() {
		return serviceDefStore;
	}

	private ServiceDefStoreFactory() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefStoreFactory.ServiceDefStoreFactory()");
		}

		init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefStoreFactory.ServiceDefStoreFactory()");
		}
	}

	private void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefStoreFactory.init()");
		}

		serviceDefStore = new ServiceDefFileStore(); // TODO: configurable store implementation

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefStoreFactory.init()");
		}
	}
}
