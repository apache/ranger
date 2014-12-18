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

package org.apache.ranger.plugin.manager;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.ServiceDefStore;
import org.apache.ranger.plugin.store.file.ServiceDefFileStore;


public class ServiceDefManager {
	private static final Log LOG = LogFactory.getLog(ServiceDefManager.class);

	private ServiceDefStore sdStore = null;

	public ServiceDefManager() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefManager.ServiceDefManager()");
		}

		init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefManager.ServiceDefManager()");
		}
	}

	public RangerServiceDef create(RangerServiceDef serviceDef) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefManager.create(" + serviceDef + ")");
		}

		RangerServiceDef ret = sdStore.create(serviceDef);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefManager.create(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	public RangerServiceDef update(RangerServiceDef serviceDef) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefManager.update(" + serviceDef + ")");
		}

		RangerServiceDef ret = sdStore.update(serviceDef);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefManager.update(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	public void delete(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefManager.delete(" + id + ")");
		}

		sdStore.delete(id);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefManager.delete(" + id + ")");
		}
	}

	public RangerServiceDef get(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefManager.get(" + id + ")");
		}

		RangerServiceDef ret = sdStore.get(id);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefManager.get(" + id + ")");
		}

		return ret;
	}

	public RangerServiceDef getByName(String name) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefManager.getByName(" + name + ")");
		}

		RangerServiceDef ret = sdStore.getByName(name);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefManager.getByName(" + name + "): " + ret);
		}

		return ret;
	}

	public List<RangerServiceDef> getAll() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefManager.getAll()");
		}

		List<RangerServiceDef> ret = sdStore.getAll();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefManager.getAll(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	private void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefManager.init()");
		}

		sdStore = new ServiceDefFileStore(); // TODO: store type should be configurable

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefManager.init()");
		}
	}
}
