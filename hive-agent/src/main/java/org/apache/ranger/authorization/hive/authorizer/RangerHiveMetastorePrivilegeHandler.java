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
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.util.AlterRequest;
import org.apache.ranger.plugin.util.GrantRevokeRequest;

import java.util.*;

/**
 * This hook is invoked post privilege checks and only handles privilege side effects
 * as result of a primary Hive operation, such as management of "automatic privileges"
 * as result of SQL DDL operations. Therefore there is no need to do security auditing
 * here since it has been allowed by the check on the primary operation.
 */
public class RangerHiveMetastorePrivilegeHandler extends MetaStoreEventListener {
	private static final Log LOG = LogFactory.getLog(RangerHiveMetastorePrivilegeHandler.class) ;

	private static volatile RangerHivePlugin hivePlugin = null ;

    private HiveAuthzSessionContext.CLIENT_TYPE ctype = null;

	public RangerHiveMetastorePrivilegeHandler(Configuration config) {
		super(config);

		LOG.debug("RangerHiveMetastorePrivilegeHandler.RangerHiveMetastorePrivilegeHandler()");
	}

    private Pair<SessionState, UserGroupInformation> init() throws MetaException {
        RangerHivePlugin plugin = hivePlugin;
        SessionState ss;
        UserGroupInformation ugi;
        ss = SessionState.get();
        if (ss != null) {
            if (ss.isHiveServerQuery()) {
                ctype = HiveAuthzSessionContext.CLIENT_TYPE.HIVESERVER2;
            } else {
                ctype = HiveAuthzSessionContext.CLIENT_TYPE.HIVECLI;
            }
        }

        try {
            ugi = Utils.getUGI();
        } catch (Exception ex) {
            throw new MetaException(ex.getMessage());
        }

        if (plugin == null) {
            synchronized (RangerHiveMetastoreAuthorizer.class) {
                plugin = hivePlugin;

                if (plugin == null) {
                    String appType = "";
                    if (ctype != null) {
                        appType = ctype.toString();
                    }

                    plugin = new RangerHivePlugin(appType);
                    plugin.init();
                    hivePlugin = plugin;
                }
            }
        }
        return new Pair(ss, ugi);
    }

    @Override
    public void onConfigChange(ConfigChangeEvent cce) throws MetaException {
        init();
        synchronized (RangerHiveMetastorePrivilegeHandler.class) {
            Configuration config = getConf();
            config.set(cce.getKey(), cce.getOldValue());
            setConf(config);
        }
    }

    private GrantRevokeRequest createGrantRevokeRequest(String dbName) throws MetaException {
        return createGrantRevokeRequest(dbName, null, null);
    }

    private GrantRevokeRequest createGrantRevokeRequest(String dbName, String tableName)
        throws MetaException {
        return createGrantRevokeRequest(dbName, tableName, null);
    }

    /**
     *
     * @param dbName database name
     * @param tableName table name; null means for DB only
     * @param indexName index name
     * @return created GrantRevokeRequest
     */
    private GrantRevokeRequest createGrantRevokeRequest(String dbName, String tableName,
                                                        String indexName)
            throws MetaException {
        Pair<SessionState, UserGroupInformation> p = init();
        UserGroupInformation ugi = p.second;
        SessionState ss= p.first;
        GrantRevokeRequest request = new GrantRevokeRequest();
        request.setGrantor(ugi.getShortUserName());
        request.setDelegateAdmin(false);
        request.setEnableAudit(true);
        request.setReplaceExistingPermissions(false);
        Map<String, String> mapResource = new HashMap();
        mapResource.put(RangerHiveResource.KEY_DATABASE, dbName);
        if (tableName != null)
            mapResource.put(RangerHiveResource.KEY_TABLE, tableName);
        if (indexName != null)
            mapResource.put(RangerHiveResource.KEY_INDEX, indexName);
        else
            mapResource.put(RangerHiveResource.KEY_COLUMN, "*");
        request.setResource(mapResource);
        if (ss != null) {
            request.setClientIPAddress(ss.getUserIpAddress());
            request.setSessionId(ss.getSessionId());
            request.setRequestData(ss.getCmd());
            request.getUsers().add(ss.getUserName() == null ?
                    ss.getAuthenticator().getUserName() : ss.getUserName());
        }
        if (ctype != null)
            request.setClientType(ctype.toString());
        // G access to the owner right now;
        // Later Table.getPrivileges could be consulted to honor Hive's CreateTableAutomaticGrant
        request.getAccessTypes().add(HiveAccessType.ALL.name());
        return request;
    }

    /**
     *
     * @param oldDbName old database name
     * @param oldTableName old table name; null means for DB only
     * @param oldIndexName old index name
     * @param newDbName old database name
     * @param newTableName new table name; null means for DB only
     * @param newIndexName new index name
     * @return created AlterRequest
     */
    private AlterRequest getHiveAlterRequest(String oldDbName, String oldTableName,
                                             String oldIndexName,
                                             String newDbName, String newTableName,
                                             String newIndexName)
            throws MetaException {
        Pair<SessionState, UserGroupInformation> p =  init();
        UserGroupInformation ugi = p.second;
        SessionState ss= p.first;
        AlterRequest request = new AlterRequest();
        request.setGrantor(ugi.getShortUserName());
        request.setDelegateAdmin(false);
        request.setEnableAudit(true);
        request.setReplaceExistingPermissions(false);
        Map<String, String> oldMapResource = new HashMap();
        oldMapResource.put(RangerHiveResource.KEY_DATABASE, oldDbName);
        if (oldTableName != null)
            oldMapResource.put(RangerHiveResource.KEY_TABLE, oldTableName);
        if (oldIndexName != null)
            oldMapResource.put(RangerHiveResource.KEY_INDEX, oldIndexName);
        else
            oldMapResource.put(RangerHiveResource.KEY_COLUMN, "*");
        request.setOldResource(oldMapResource);
        Map<String, String> newMapResource = new HashMap();
        newMapResource.put(RangerHiveResource.KEY_DATABASE, newDbName);
        if (newTableName != null)
            newMapResource.put(RangerHiveResource.KEY_TABLE, newTableName);
        if (newIndexName != null)
            newMapResource.put(RangerHiveResource.KEY_INDEX, newIndexName);
        else
            newMapResource.put(RangerHiveResource.KEY_COLUMN, "*");
        request.setNewResource(newMapResource);
        if (ss != null) {
            request.setClientIPAddress(ss.getUserIpAddress());
            request.setSessionId(ss.getSessionId());
            request.setRequestData(ss.getCmd());
        }
        if (ctype != null)
            request.setClientType(ctype.toString());
        return request;
    }

    @Override
    public void onCreateTable(CreateTableEvent cte) throws MetaException {
        Table table = cte.getTable();
        if (table.getTableType().equals(TableType.INDEX_TABLE.toString())) return;
        GrantRevokeRequest request1 = createGrantRevokeRequest(table.getDbName(),
                table.getTableName(), null);
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();
        try {
            hivePlugin.grantAccess(request1, auditHandler);
            try {
                // For index privileges
                GrantRevokeRequest request2 = createGrantRevokeRequest(table.getDbName(),
                        table.getTableName(), "*");
                hivePlugin.grantAccess(request2, auditHandler);
            } catch (Exception e) {
                hivePlugin.removeAccess(request1, auditHandler);
            }
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        } finally {
            auditHandler.flushAudit();
        }
    }

    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
        Table table = tableEvent.getTable();
        GrantRevokeRequest request1 = createGrantRevokeRequest(table.getDbName(),
                table.getTableName(), null);
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();
        try {
            hivePlugin.removeAccess(request1, auditHandler);
            try {
                // For index privileges
                GrantRevokeRequest request2 = createGrantRevokeRequest(table.getDbName(),
                        table.getTableName(), "*");
                hivePlugin.removeAccess(request2, auditHandler);
            } catch (Exception e) {
                hivePlugin.grantAccess(request1, auditHandler);
            }
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        } finally {
            auditHandler.flushAudit();
        }
    }

    public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
        Table newTable = tableEvent.getNewTable();
        String newDbName = newTable.getDbName();
        String newTableName = newTable.getTableName();
        Table oldTable = tableEvent.getOldTable();
        String oldDbName = oldTable.getDbName();
        String oldTableName = oldTable.getTableName();
        // Now this is a two-step process:
        // revoke the old privileges followed by granting the new ones
        AlterRequest request1 = getHiveAlterRequest(oldDbName, oldTableName, null,
                newDbName, newTableName, null);
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();
        try {
            hivePlugin.alterAccess(request1, auditHandler);
            AlterRequest request2 = getHiveAlterRequest(oldDbName, oldTableName, "*",
                        newDbName, newTableName, "*");
            hivePlugin.alterAccess(request2, auditHandler);
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        } finally {
            auditHandler.flushAudit();
        }
    }

    public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
    }

    public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    }

    public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
    }

    public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
        GrantRevokeRequest request = createGrantRevokeRequest(dbEvent.getDatabase().getName(),
                null, null);
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();
        try {
            hivePlugin.grantAccess(request, auditHandler);
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        } finally {
            auditHandler.flushAudit();
        }
    }

    public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
        List<String> resourceLocations = null;
        GrantRevokeRequest request = createGrantRevokeRequest(dbEvent.getDatabase().getName(),
                null, null);
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();
        try {
            hivePlugin.removeAccess(request, auditHandler);
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        } finally {
            auditHandler.flushAudit();
        }
    }

    public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException {
        // no-op
    }

    public void onAddIndex(AddIndexEvent indexEvent) throws MetaException {
        Index index = indexEvent.getIndex();
        GrantRevokeRequest request = createGrantRevokeRequest(index.getDbName(),
                index.getOrigTableName(), index.getIndexName());
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();
        try {
            hivePlugin.grantAccess(request, auditHandler);
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        } finally {
            auditHandler.flushAudit();
        }
    }

    public void onDropIndex(DropIndexEvent indexEvent) throws MetaException {
        Index index = indexEvent.getIndex();
        if (index != null) {
            GrantRevokeRequest request = createGrantRevokeRequest(index.getDbName(),
                    index.getOrigTableName(), index.getIndexName());
            RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();
            try {
                hivePlugin.removeAccess(request, auditHandler);
            } catch (Exception e) {
                throw new MetaException(e.getMessage());
            } finally {
                auditHandler.flushAudit();
            }
        }
    }

    public void onAlterIndex(AlterIndexEvent indexEvent) throws MetaException {
        Index oldIndex = indexEvent.getOldIndex();
        Index newIndex = indexEvent.getNewIndex();
        AlterRequest request = getHiveAlterRequest(oldIndex.getDbName(), oldIndex.getOrigTableName(),
                oldIndex.getIndexName(),
                newIndex.getDbName(), newIndex.getOrigTableName(),
                newIndex.getIndexName());
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();
        try {
            hivePlugin.alterAccess(request, auditHandler);
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        } finally {
            auditHandler.flushAudit();
        }
    }

    public void onInsert(InsertEvent insertEvent) throws MetaException {
        GrantRevokeRequest request = createGrantRevokeRequest(insertEvent.getDb(),
                insertEvent.getTable(), null);
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();
        try {
            hivePlugin.grantAccess(request, auditHandler);
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        } finally {
            auditHandler.flushAudit();
        }
    }
}

class Pair<T1, T2> {
    public Pair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }
    public T1 first;
    public T2 second;
}
