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

package org.apache.ranger.authorization.hive.authorizer;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;

import java.lang.reflect.Constructor;

public class RangerHiveMetastoreAuthorizer extends MetaStorePreEventListener {
	private static final Log LOG  = LogFactory.getLog(RangerHiveMetastoreAuthorizer.class);

	private static final String   RANGER_PLUGIN_TYPE                      = "hive";
	private static final String   RANGER_HIVE_METASTORE_AUTHORIZER_IMPL_CLASSNAME   =
            "org.apache.ranger.authorization.hive.authorizer.RangerHiveMetastoreAuthorizer";

	private MetaStorePreEventListener rangerHiveMetastoreAuthorizerImpl   = null;
	private static RangerPluginClassLoader rangerPluginClassLoader  = null;
	
	public RangerHiveMetastoreAuthorizer(Configuration conf) {
        super(conf);
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHiveMetastoreAuthorizer.RangerHiveMetastoreAuthorizer()");
		}

		this.init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHiveMetastoreAuthorizer.RangerHiveMetastoreAuthorizer()");
		}
	}

	public void init(){
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHiveMetastoreAuthorizer.init()");
		}

		try {
			
			rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());
			
			@SuppressWarnings("unchecked")
			Class<MetaStorePreEventListener> cls = (Class<MetaStorePreEventListener>) Class.forName(RANGER_HIVE_METASTORE_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

			activatePluginClassLoader();
            Constructor ctor = cls.getDeclaredConstructor(Configuration.class);
			rangerHiveMetastoreAuthorizerImpl = (MetaStorePreEventListener) ctor.newInstance(getConf());
		} catch (Exception e) {
			// check what need to be done
			LOG.error("Error Enabling RangerHdfsPluing", e);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuthorizer.init()");
		}
	}

	@Override
    public void onEvent(org.apache.hadoop.hive.metastore.events.PreEventContext preEventContext)
            throws org.apache.hadoop.hive.metastore.api.MetaException,
            org.apache.hadoop.hive.metastore.api.NoSuchObjectException,
            org.apache.hadoop.hive.metastore.api.InvalidOperationException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHiveMetaStoreAuthorizer.onEvent(" + preEventContext.getEventType().name() + ")");
		}

		try {
			activatePluginClassLoader();

			rangerHiveMetastoreAuthorizerImpl.onEvent(preEventContext);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHiveMetastoreAuthorizer.onEvent(" + preEventContext.getEventType().name() + "): Success");
		}
	}

	private void activatePluginClassLoader() {
		if(rangerPluginClassLoader != null) {
			rangerPluginClassLoader.activate();
		}
	}

	private void deactivatePluginClassLoader() {
		if(rangerPluginClassLoader != null) {
			rangerPluginClassLoader.deactivate();
		}
	}
}

