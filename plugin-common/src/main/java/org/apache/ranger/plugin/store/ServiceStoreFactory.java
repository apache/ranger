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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;


public class ServiceStoreFactory {
	private static final Log LOG = LogFactory.getLog(ServiceStoreFactory.class);

	private static ServiceStoreFactory sInstance = null;

	private Map<String, ServiceStore> serviceStores       = null;
	private ServiceStore              defaultServiceStore = null;


	public static ServiceStoreFactory instance() {
		if(sInstance == null) {
			sInstance = new ServiceStoreFactory();
		}

		return sInstance;
	}

	public ServiceStore getServiceStore() {
		ServiceStore ret = defaultServiceStore;

		if(ret == null) { // if no service store has been created yet, create the default store. TODO: review the impact and update, if necessary
			String defaultServiceStoreClass = RangerConfiguration.getInstance().get("ranger.default.service.store.class", "org.apache.ranger.plugin.store.file.ServiceFileStore");

			ret = getServiceStore(defaultServiceStoreClass);
		}

		return ret;
	}

	public ServiceStore getServiceStore(String storeClassname) {
		ServiceStore ret = serviceStores.get(storeClassname);

		if(ret == null) {
			synchronized(this) {
				ret = serviceStores.get(storeClassname);

				if(ret == null) {
					try {
						@SuppressWarnings("unchecked")
						Class<ServiceStore> storeClass = (Class<ServiceStore>)Class.forName(storeClassname);

						ret = storeClass.newInstance();

						ret.init();

						serviceStores.put(storeClassname, ret);

						if(defaultServiceStore == null) {
							defaultServiceStore = ret;
						}
					} catch(Exception excp) {
						LOG.error("failed to instantiate service store of type " + storeClassname, excp);
					}
				}
			}
		}

		return ret;
	}

	private ServiceStoreFactory() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceStoreFactory.ServiceStoreFactory()");
		}

		init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceStoreFactory.ServiceStoreFactory()");
		}
	}

	private void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceStoreFactory.init()");
		}

		serviceStores = new HashMap<String, ServiceStore>();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceStoreFactory.init()");
		}
	}
}
