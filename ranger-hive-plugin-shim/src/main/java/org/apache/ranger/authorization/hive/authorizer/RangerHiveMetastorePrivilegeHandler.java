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
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;

import java.lang.reflect.Constructor;

public class RangerHiveMetastorePrivilegeHandler extends MetaStoreEventListener {
	private static final Log LOG  = LogFactory.getLog(RangerHiveMetastorePrivilegeHandler.class);

	private static final String   RANGER_PLUGIN_TYPE                      = "hive";
	private static final String   RANGER_HIVE_METASTORE_AUTHORIZER_IMPL_CLASSNAME   =
            "org.apache.ranger.authorization.hive.authorizer.RangerHiveMetastorePrivilegeHandler";

	private MetaStoreEventListener rangerHiveMetastorePrivilegeHandlerImpl   = null;
	private static RangerPluginClassLoader rangerPluginClassLoader  = null;
	
	public RangerHiveMetastorePrivilegeHandler(Configuration conf) {
        super(conf);
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHiveMetastorePrivilegeHandler.RangerHiveMetastorePrivilegeHandler()");
		}

		this.init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHiveMetastorePrivilegeHandler.RangerHiveMetastorePrivilegeHandler()");
		}
	}

	public void init(){
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHiveMetastorePrivilegHandler.init()");
		}

		try {
			
			rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());
			
			@SuppressWarnings("unchecked")
			Class<MetaStoreEventListener> cls = (Class<MetaStoreEventListener>) Class.forName(RANGER_HIVE_METASTORE_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

			activatePluginClassLoader();
            Constructor ctor = cls.getDeclaredConstructor(Configuration.class);
			rangerHiveMetastorePrivilegeHandlerImpl = (MetaStoreEventListener) ctor.newInstance(getConf());
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
    public void onConfigChange(ConfigChangeEvent cce) throws MetaException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHiveMetaStoreAuthorizer.onConfigChange(" + cce.toString() + ")");
		}

		try {
			activatePluginClassLoader();

			rangerHiveMetastorePrivilegeHandlerImpl.onConfigChange(cce);
		} finally {
			deactivatePluginClassLoader();
		}
	}

    @Override
    public void onCreateTable(CreateTableEvent cte) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onCreateTable(" + cte.toString() + ")");
        }

        try {
            activatePluginClassLoader();

            rangerHiveMetastorePrivilegeHandlerImpl.onCreateTable(cte);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void onDropTable(DropTableEvent dte) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onDropTable(" + dte.toString() + ")");
        }

        try {
            activatePluginClassLoader();
            rangerHiveMetastorePrivilegeHandlerImpl.onDropTable(dte);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void onAlterTable(AlterTableEvent ate) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onAlterTable(" + ate.toString() + ")");
        }

        try {
            activatePluginClassLoader();

            rangerHiveMetastorePrivilegeHandlerImpl.onAlterTable(ate);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void onAddPartition(AddPartitionEvent ape) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onAddPartition(" + ape.toString() + ")");
        }

        try {
            activatePluginClassLoader();

            rangerHiveMetastorePrivilegeHandlerImpl.onAddPartition(ape);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void onDropPartition(DropPartitionEvent dpe) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onDropPartition(" + dpe.toString() + ")");
        }

        try {
            activatePluginClassLoader();

            rangerHiveMetastorePrivilegeHandlerImpl.onDropPartition(dpe);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void onAlterPartition(AlterPartitionEvent ape) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onAlterPartition(" + ape.toString() + ")");
        }

        try {
            activatePluginClassLoader();

            rangerHiveMetastorePrivilegeHandlerImpl.onAlterPartition(ape);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void onCreateDatabase(CreateDatabaseEvent cde) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onCreateDatabase(" + cde.toString() + ")");
        }

        try {
            activatePluginClassLoader();

            rangerHiveMetastorePrivilegeHandlerImpl.onCreateDatabase(cde);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void onDropDatabase(DropDatabaseEvent dde) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onDropDatabase(" + dde.toString() + ")");
        }

        try {
            activatePluginClassLoader();

            rangerHiveMetastorePrivilegeHandlerImpl.onDropDatabase(dde);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void onLoadPartitionDone(LoadPartitionDoneEvent lpde) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onLoadPartitionDone(" + lpde.toString() + ")");
        }

        try {
            activatePluginClassLoader();

            rangerHiveMetastorePrivilegeHandlerImpl.onLoadPartitionDone(lpde);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void onAddIndex(AddIndexEvent aie) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onAddIndex(" + aie.toString() + ")");
        }

        try {
            activatePluginClassLoader();

            rangerHiveMetastorePrivilegeHandlerImpl.onAddIndex(aie);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void onDropIndex(DropIndexEvent die) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onDropIndex(" + die.toString() + ")");
        }

        try {
            activatePluginClassLoader();

            rangerHiveMetastorePrivilegeHandlerImpl.onDropIndex(die);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void onAlterIndex(AlterIndexEvent aie) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onAlterIndex(" + aie.toString() + ")");
        }

        try {
            activatePluginClassLoader();

            rangerHiveMetastorePrivilegeHandlerImpl.onAlterIndex(aie);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void onInsert(InsertEvent ie) throws MetaException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerHiveMetaStoreAuthorizer.onInsert(" + ie.toString() + ")");
        }

        try {
            activatePluginClassLoader();

            rangerHiveMetastorePrivilegeHandlerImpl.onInsert(ie);
        } finally {
            deactivatePluginClassLoader();
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