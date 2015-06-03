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
 * This utility class deals with service-defs embedded in ranger-plugins-common
 * library (hdfs/hbase/hive/knox/storm/..). If any of these service-defs
 * don't exist in the given service store, they will be created in the store
 * using the embedded definitions.
 * 
 * init() method should be called from ServiceStore implementations to
 * initialize embedded service-defs.
 */
public class EmbeddedServiceDefsUtil {
	private static final Log LOG = LogFactory.getLog(EmbeddedServiceDefsUtil.class);


	public static final String EMBEDDED_SERVICEDEF_TAG_NAME  = "_tag_";
	public static final String EMBEDDED_SERVICEDEF_HDFS_NAME  = "hdfs";
	public static final String EMBEDDED_SERVICEDEF_HBASE_NAME = "hbase";
	public static final String EMBEDDED_SERVICEDEF_HIVE_NAME  = "hive";
	public static final String EMBEDDED_SERVICEDEF_KMS_NAME   = "kms";
	public static final String EMBEDDED_SERVICEDEF_KNOX_NAME  = "knox";
	public static final String EMBEDDED_SERVICEDEF_STORM_NAME = "storm";
	public static final String EMBEDDED_SERVICEDEF_YARN_NAME  = "yarn";
	public static final String EMBEDDED_SERVICEDEF_KAFKA_NAME = "kafka";
	public static final String EMBEDDED_SERVICEDEF_SOLR_NAME  = "solr";
	public static final String PROPERTY_CREATE_EMBEDDED_SERVICE_DEFS = "ranger.service.store.create.embedded.service-defs";

	public static final String HDFS_IMPL_CLASS_NAME  = "org.apache.ranger.services.hdfs.RangerServiceHdfs";
	public static final String HBASE_IMPL_CLASS_NAME = "org.apache.ranger.services.hbase.RangerServiceHBase";
	public static final String HIVE_IMPL_CLASS_NAME  = "org.apache.ranger.services.hive.RangerServiceHive";
	public static final String KMS_IMPL_CLASS_NAME   = "org.apache.ranger.services.kms.RangerServiceKMS";
	public static final String KNOX_IMPL_CLASS_NAME  = "org.apache.ranger.services.knox.RangerServiceKnox";
	public static final String STORM_IMPL_CLASS_NAME = "org.apache.ranger.services.storm.RangerServiceStorm";
	public static final String YARN_IMPL_CLASS_NAME  = "org.apache.ranger.services.yarn.RangerServiceYarn";
	public static final String KAFKA_IMPL_CLASS_NAME = "org.apache.ranger.services.kafka.RangerServiceKafka";
	public static final String SOLR_IMPL_CLASS_NAME  = "org.apache.ranger.services.solr.RangerServiceSolr";

	private static EmbeddedServiceDefsUtil instance = new EmbeddedServiceDefsUtil();

	private boolean          createEmbeddedServiceDefs = true;
	private RangerServiceDef hdfsServiceDef  = null;
	private RangerServiceDef hBaseServiceDef = null;
	private RangerServiceDef hiveServiceDef  = null;
	private RangerServiceDef kmsServiceDef   = null;
	private RangerServiceDef knoxServiceDef  = null;
	private RangerServiceDef stormServiceDef = null;
	private RangerServiceDef yarnServiceDef  = null;
	private RangerServiceDef kafkaServiceDef = null;
	private RangerServiceDef solrServiceDef  = null;

	private RangerServiceDef tagServiceDef = null;

	private Gson gsonBuilder = null;


	/* private constructor to restrict instantiation of this singleton utility class */
	private EmbeddedServiceDefsUtil() {
	}

	public static EmbeddedServiceDefsUtil instance() {
		return instance;
	}

	public void init(ServiceStore store) {
		LOG.info("==> EmbeddedServiceDefsUtil.init()");

		try {
			createEmbeddedServiceDefs = RangerConfiguration.getInstance().getBoolean(PROPERTY_CREATE_EMBEDDED_SERVICE_DEFS, true);

			gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();

			/*
			 * Maintaining the following service-def create-order is critical for the 
			 * the legacy service-defs (HDFS/HBase/Hive/Knox/Storm) to be assigned IDs
			 * that were used in earlier version (0.4) */
			hdfsServiceDef  = getOrCreateServiceDef(store, EMBEDDED_SERVICEDEF_HDFS_NAME);
			hBaseServiceDef = getOrCreateServiceDef(store, EMBEDDED_SERVICEDEF_HBASE_NAME);
			hiveServiceDef  = getOrCreateServiceDef(store, EMBEDDED_SERVICEDEF_HIVE_NAME);
			kmsServiceDef   = getOrCreateServiceDef(store, EMBEDDED_SERVICEDEF_KMS_NAME);
			knoxServiceDef  = getOrCreateServiceDef(store, EMBEDDED_SERVICEDEF_KNOX_NAME);
			stormServiceDef = getOrCreateServiceDef(store, EMBEDDED_SERVICEDEF_STORM_NAME);
			yarnServiceDef  = getOrCreateServiceDef(store, EMBEDDED_SERVICEDEF_YARN_NAME);
			kafkaServiceDef = getOrCreateServiceDef(store, EMBEDDED_SERVICEDEF_KAFKA_NAME);
			solrServiceDef  = getOrCreateServiceDef(store, EMBEDDED_SERVICEDEF_SOLR_NAME);

			tagServiceDef = getOrCreateServiceDef(store, EMBEDDED_SERVICEDEF_TAG_NAME);

			// Ensure that tag service def is updated with access types of all service defs
			store.updateTagServiceDefForAccessTypes();
		} catch(Throwable excp) {
			LOG.fatal("EmbeddedServiceDefsUtil.init(): failed", excp);
		}

		LOG.info("<== EmbeddedServiceDefsUtil.init()");
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

	public long getKmsServiceDefId() {
		return getId(kmsServiceDef);
	}

	public long getKnoxServiceDefId() {
		return getId(knoxServiceDef);
	}

	public long getStormServiceDefId() {
		return getId(stormServiceDef);
	}

	public long getYarnServiceDefId() {
		return getId(yarnServiceDef);
	}
	
	public long getKafkaServiceDefId() {
		return getId(kafkaServiceDef);
	}

	public long getSolrServiceDefId() {
		return getId(solrServiceDef);
	}

	public long getTagServiceDefId() { return getId(tagServiceDef); }

	private long getId(RangerServiceDef serviceDef) {
		return serviceDef == null || serviceDef.getId() == null ? -1 : serviceDef.getId().longValue();
	}

	private RangerServiceDef getOrCreateServiceDef(ServiceStore store, String serviceDefName) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> EmbeddedServiceDefsUtil.getOrCreateServiceDef(" + serviceDefName + ")");
		}

		RangerServiceDef ret = null;

		try {
			ret = store.getServiceDefByName(serviceDefName);
			if(ret == null && createEmbeddedServiceDefs) {
				ret = loadEmbeddedServiceDef(serviceDefName);

				LOG.info("creating embedded service-def " + serviceDefName);
				ret = store.createServiceDef(ret);
				LOG.info("created embedded service-def " + serviceDefName);
			}
		} catch(Exception excp) {
			LOG.fatal("EmbeddedServiceDefsUtil.getOrCreateServiceDef(): failed to load/create serviceType " + serviceDefName, excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== EmbeddedServiceDefsUtil.getOrCreateServiceDef(" + serviceDefName + "): " + ret);
		}

		return ret;
	}

	private RangerServiceDef loadEmbeddedServiceDef(String serviceType) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> EmbeddedServiceDefsUtil.loadEmbeddedServiceDef(" + serviceType + ")");
		}

		RangerServiceDef ret = null;
	
		String resource = "/service-defs/ranger-servicedef-" + serviceType + ".json";

		InputStream inStream = getClass().getResourceAsStream(resource);

		InputStreamReader reader = new InputStreamReader(inStream);

		ret = gsonBuilder.fromJson(reader, RangerServiceDef.class);

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> EmbeddedServiceDefsUtil.loadEmbeddedServiceDef(" + serviceType + ")");
		}

		return ret;
	}
}
