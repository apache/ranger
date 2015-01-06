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

package org.apache.ranger.plugin.store.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.ServiceDefStore;


public class ServiceDefFileStore extends BaseFileStore implements ServiceDefStore {
	private static final Log LOG = LogFactory.getLog(ServiceDefFileStore.class);

	private List<RangerServiceDef> serviceDefs      = null;
	private long                   nextServiceDefId = 0;

	static Map<String, Long> legacyServiceTypes = new HashMap<String, Long>();

	static {
		legacyServiceTypes.put("hdfs",  new Long(1));
		legacyServiceTypes.put("hbase", new Long(2));
		legacyServiceTypes.put("hive",  new Long(3));
		legacyServiceTypes.put("knox",  new Long(5));
		legacyServiceTypes.put("storm", new Long(6));
	}

	public ServiceDefFileStore() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.ServiceDefManagerFile()");
		}

		init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.ServiceDefManagerFile()");
		}
	}

	@Override
	public RangerServiceDef create(RangerServiceDef serviceDef) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.create(" + serviceDef + ")");
		}

		RangerServiceDef existing = findServiceDefByName(serviceDef.getName());
		
		if(existing != null) {
			throw new Exception(serviceDef.getName() + ": service-def already exists (id=" + existing.getId() + ")");
		}

		RangerServiceDef ret = null;

		try {
			preCreate(serviceDef);

			serviceDef.setId(nextServiceDefId++);

			Path filePath = new Path(getServiceDefFile(serviceDef.getId()));

			ret = saveToFile(serviceDef, filePath, false);

			addServiceDef(ret);

			postCreate(ret);
		} catch(Exception excp) {
			LOG.warn("ServiceDefFileStore.create(): failed to save service-def '" + serviceDef.getName() + "'", excp);

			throw new Exception("failed to save service-def '" + serviceDef.getName() + "'", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.create(" + serviceDef + ")");
		}

		return ret;
	}

	@Override
	public RangerServiceDef update(RangerServiceDef serviceDef) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.update(" + serviceDef + ")");
		}

		RangerServiceDef existing = findServiceDefById(serviceDef.getId());

		if(existing == null) {
			throw new Exception(serviceDef.getId() + ": service-def does not exist");
		}

		if(isLegacyServiceType(existing)) {
			String msg = existing.getName() + ": is an in-built service-def. Update not allowed";

			LOG.warn(msg);

			throw new Exception(msg);
		}

		String existingName = existing.getName();

		boolean renamed = !StringUtils.equalsIgnoreCase(serviceDef.getName(), existingName);

		// renaming service-def would require updating services that refer to this service-def
		if(renamed) {
			LOG.warn("ServiceDefFileStore.update(): service-def renaming not supported. " + existingName + " ==> " + serviceDef.getName());

			throw new Exception("service-def renaming not supported. " + existingName + " ==> " + serviceDef.getName());
		}

		RangerServiceDef ret = null;

		try {
			existing.updateFrom(serviceDef);

			preUpdate(existing);

			Path filePath = new Path(getServiceDefFile(existing.getId()));

			ret = saveToFile(existing, filePath, true);

			postUpdate(ret);
		} catch(Exception excp) {
			LOG.warn("ServiceDefFileStore.update(): failed to save service-def '" + existing.getName() + "'", excp);

			throw new Exception("failed to save service-def '" + existing.getName() + "'", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.update(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	@Override
	public void delete(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.delete(" + id + ")");
		}

		RangerServiceDef existing = findServiceDefById(id);

		if(existing == null) {
			throw new Exception("service-def does not exist. id=" + id);
		}

		if(isLegacyServiceType(existing)) {
			String msg = existing.getName() + ": is an in-built service-def. Update not allowed";

			LOG.warn(msg);

			throw new Exception(msg);
		}

		// TODO: deleting service-def would require deleting services that refer to this service-def

		try {
			preDelete(existing);

			Path filePath = new Path(getServiceDefFile(id));

			deleteFile(filePath);
			
			removeServiceDef(existing);

			postDelete(existing);
		} catch(Exception excp) {
			throw new Exception("failed to delete service-def. id=" + id + "; name=" + existing.getName(), excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.delete(" + id + ")");
		}
	}

	@Override
	public RangerServiceDef get(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.get(" + id + ")");
		}

		RangerServiceDef ret = findServiceDefById(id);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.get(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerServiceDef getByName(String name) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.getByName(" + name + ")");
		}

		RangerServiceDef ret = findServiceDefByName(name);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.getByName(" + name + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerServiceDef> getAll() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.getAll()");
		}

		List<RangerServiceDef> ret = serviceDefs;

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.getAll(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	protected void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.init()");
		}

		super.init();

		try {
			serviceDefs = new ArrayList<RangerServiceDef>();

			// load definitions for legacy services from embedded resources
			String[] legacyServiceDefResources = {
					"/service-defs/ranger-servicedef-hdfs.json",
					"/service-defs/ranger-servicedef-hive.json",
					"/service-defs/ranger-servicedef-hbase.json",
					"/service-defs/ranger-servicedef-knox.json",
					"/service-defs/ranger-servicedef-storm.json",
			};
			
			for(String resource : legacyServiceDefResources) {
				RangerServiceDef sd = loadFromResource(resource, RangerServiceDef.class);
				
				if(sd != null) {
					serviceDefs.add(sd);
				}
			}
			nextServiceDefId = getMaxId(serviceDefs) + 1;

			// load service definitions from file system
			List<RangerServiceDef> sds = loadFromDir(new Path(getDataDir()), FILE_PREFIX_SERVICE_DEF, RangerServiceDef.class);
			
			if(sds != null) {
				for(RangerServiceDef sd : sds) {
					if(sd != null) {
						if(isLegacyServiceType(sd)) {
							LOG.warn("Found in-built service-def '" + sd.getName() + "'  under " + getDataDir() + ". Ignorning");

							continue;
						}

						RangerServiceDef existingSd = findServiceDefByName(sd.getName());

						if(existingSd != null) {
							removeServiceDef(existingSd);
						}

						existingSd = findServiceDefById(sd.getId());

						if(existingSd != null) {
							removeServiceDef(existingSd);
						}

						serviceDefs.add(sd);
					}
				}
			}
			nextServiceDefId = getMaxId(serviceDefs) + 1;
		} catch(Exception excp) {
			LOG.error("ServiceDefFileStore.init(): failed to read service-defs", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.init()");
		}
	}

	private RangerServiceDef findServiceDefById(long id) {
		RangerServiceDef ret = null;

		for(RangerServiceDef sd : serviceDefs) {
			if(sd != null && sd.getId() != null && sd.getId().longValue() == id) {
				ret = sd;

				break;
			}
		}

		return ret;
	}

	private RangerServiceDef findServiceDefByName(String sdName) {
		RangerServiceDef ret = null;

		for(RangerServiceDef sd : serviceDefs) {
			if(sd != null && StringUtils.equalsIgnoreCase(sd.getName(), sdName)) {
				ret = sd;

				break;
			}
		}

		return ret;
	}

	private void addServiceDef(RangerServiceDef sd) {
		serviceDefs.add(sd);
	}

	private void removeServiceDef(RangerServiceDef sd) {
		serviceDefs.remove(sd);
	}

	private boolean isLegacyServiceType(RangerServiceDef sd) {
		return sd == null ? false : (isLegacyServiceType(sd.getName()) || isLegacyServiceType(sd.getId()));
	}

	private boolean isLegacyServiceType(String name) {
		return name == null ? false : legacyServiceTypes.containsKey(name);
	}

	private boolean isLegacyServiceType(Long id) {
		return id == null ? false : legacyServiceTypes.containsValue(id);
	}
}
