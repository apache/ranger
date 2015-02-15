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

import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerServiceDef;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * This utility class deals with service-def for legacy services hdfs/hbase/hive/knox/storm.
 * If any of service-defs don't exist in the given service store, they will be created in
 * the store using the definitions embedded in ranger-plugins-common.jar.
 * 
 * init() method should be called from ServiceStore implementations to initialize legacy service-defs.
 */
public class LegacyServiceDefsUtil {
	private static final Log LOG = LogFactory.getLog(LegacyServiceDefsUtil.class);


	public static final String LEGACY_SERVICEDEF_HDFS_NAME  = "hdfs";
	public static final String LEGACY_SERVICEDEF_HBASE_NAME = "hbase";
	public static final String LEGACY_SERVICEDEF_HIVE_NAME  = "hive";
	public static final String LEGACY_SERVICEDEF_KNOX_NAME  = "knox";
	public static final String LEGACY_SERVICEDEF_STORM_NAME = "storm";
	public static final String PROPERTY_CREATE_LEGACY_SERVICE_DEFS = "ranger.service.store.create.legacy.service-defs";

	private static LegacyServiceDefsUtil instance = new LegacyServiceDefsUtil();

	private boolean          createLegacyServiceDefs = true;
	private RangerServiceDef hdfsServiceDef  = null;
	private RangerServiceDef hBaseServiceDef = null;
	private RangerServiceDef hiveServiceDef  = null;
	private RangerServiceDef knoxServiceDef  = null;
	private RangerServiceDef stormServiceDef = null;

	private Gson gsonBuilder = null;


	/* private constructor to restrict instantiation of this singleton utility class */
	private LegacyServiceDefsUtil() {
	}

	public static LegacyServiceDefsUtil instance() {
		return instance;
	}

	public void init(ServiceStore store) {
		LOG.info("==> LegacyServiceDefsUtil.init()");

		try {
			createLegacyServiceDefs = RangerConfiguration.getInstance().getBoolean(PROPERTY_CREATE_LEGACY_SERVICE_DEFS, true);

			gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();

			hdfsServiceDef  = getOrCreateServiceDef(store, LEGACY_SERVICEDEF_HDFS_NAME);
			hBaseServiceDef = getOrCreateServiceDef(store, LEGACY_SERVICEDEF_HBASE_NAME);
			hiveServiceDef  = getOrCreateServiceDef(store, LEGACY_SERVICEDEF_HIVE_NAME);
			knoxServiceDef  = getOrCreateServiceDef(store, LEGACY_SERVICEDEF_KNOX_NAME);
			stormServiceDef = getOrCreateServiceDef(store, LEGACY_SERVICEDEF_STORM_NAME);
		} catch(Throwable excp) {
			LOG.fatal("LegacyServiceDefsUtil.init(): failed", excp);
		}


		LOG.info("<== LegacyServiceDefsUtil.init()");
	}

	public long getHdfsServiceDefId() {
		return getId(hdfsServiceDef);
	}

	public long getHBaseServiceDefId() {
		return getId(hBaseServiceDef);
	}

	public long getHiveServiceDefId() {
		return getId(hiveServiceDef);
	}

	public long getKnoxServiceDefId() {
		return getId(knoxServiceDef);
	}

	public long getStormServiceDefId() {
		return getId(stormServiceDef);
	}


	private long getId(RangerServiceDef serviceDef) {
		return serviceDef == null || serviceDef.getId() == null ? -1 : serviceDef.getId().longValue();
	}

	private RangerServiceDef getOrCreateServiceDef(ServiceStore store, String serviceDefName) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> LegacyServiceDefsUtil.getOrCreateServiceDef(" + serviceDefName + ")");
		}

		RangerServiceDef ret = null;

		try {
			ret = store.getServiceDefByName(serviceDefName);
			if(ret == null && createLegacyServiceDefs) {
				ret = loadLegacyServiceDef(serviceDefName);

				LOG.info("creating legacy service-def " + serviceDefName);
				store.createServiceDef(ret);
			}
		} catch(Exception excp) {
			LOG.fatal("LegacyServiceDefsUtil.getOrCreateServiceDef(): failed to load/create serviceType " + serviceDefName, excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== LegacyServiceDefsUtil.getOrCreateServiceDef(" + serviceDefName + "): " + ret);
		}

		return ret;
	}

	private RangerServiceDef loadLegacyServiceDef(String serviceType) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> LegacyServiceDefsUtil.loadLegacyServiceDef(" + serviceType + ")");
		}

		RangerServiceDef ret = null;
	
		String resource = "/service-defs/ranger-servicedef-" + serviceType + ".json";

		InputStream inStream = getClass().getResourceAsStream(resource);

		InputStreamReader reader = new InputStreamReader(inStream);

		ret = gsonBuilder.fromJson(reader, RangerServiceDef.class);

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> LegacyServiceDefsUtil.loadLegacyServiceDef(" + serviceType + ")");
		}

		return ret;
	}
}
