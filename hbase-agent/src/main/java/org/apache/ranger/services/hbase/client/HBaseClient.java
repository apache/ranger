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

package org.apache.ranger.services.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

public class HBaseClient extends BaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);

    private static final String ERROR_MSG = " You can still save the repository and start creating policies, but you would not be able to use autocomplete for resource names. Check ranger_admin.log for more info.";

    private final Configuration conf;

    public HBaseClient(String serivceName, Map<String, String> connectionProp) {
        super(serivceName, addDefaultHBaseProp(connectionProp));

        conf = HBaseConfiguration.create();

        Set<String> rangerInternalPropertyKeys = getConfigHolder().getRangerInternalPropertyKeys();

        for (Map.Entry<String, String> entry : connectionProperties.entrySet()) {
            String key   = entry.getKey();
            String value = entry.getValue();

            if (!rangerInternalPropertyKeys.contains(key)) {
                conf.set(key, value);
            }
        }
    }

    public static Map<String, Object> connectionTest(String dataSource, Map<String, String> configs) throws Exception {
        Map<String, Object> responseData       = new HashMap<>();
        boolean             connectivityStatus = false;
        HBaseClient         connectionObj      = new HBaseClient(dataSource, configs);

        if (connectionObj != null) {
            try {
                connectivityStatus = connectionObj.getHBaseStatus();
            } catch (HadoopException e) {
                LOG.error("<== HBaseClient.testConnection(): Unable to retrieve any databases using given parameters", e);

                throw e;
            }
        }

        if (connectivityStatus) {
            String successMsg = "ConnectionTest Successful";

            generateResponseDataMap(connectivityStatus, successMsg, successMsg, null, null, responseData);
        } else {
            String failureMsg = "Unable to retrieve any databases using given parameters.";

            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg + ERROR_MSG, null, null, responseData);
        }

        return responseData;
    }

    public boolean getHBaseStatus() throws HadoopException {
        boolean hbaseStatus = false;
        Subject subj        = getLoginSubject();

        if (subj != null) {
            try {
                hbaseStatus = Subject.doAs(subj, new PrivilegedAction<Boolean>() {
                    @Override
                    public Boolean run() {
                        boolean hbaseStatus1 = false;

                        try {
                            LOG.info("getHBaseStatus: creating default Hbase configuration");
                            LOG.info("getHBaseStatus: setting config values from client");

                            setClientConfigValues(conf);

                            LOG.info("getHBaseStatus: checking HbaseAvailability with the new config");

                            HBaseAdmin.available(conf);

                            LOG.info("getHBaseStatus: no exception: HbaseAvailability true");

                            hbaseStatus1 = true;
                        } catch (ZooKeeperConnectionException zce) {
                            String          msgDesc      = "getHBaseStatus: Unable to connect to `ZooKeeper` using given config parameters.";
                            HadoopException hdpException = new HadoopException(msgDesc, zce);

                            hdpException.generateResponseDataMap(false, getMessage(zce), msgDesc + ERROR_MSG, null, null);

                            LOG.error(msgDesc + zce);

                            throw hdpException;
                        } catch (MasterNotRunningException mnre) {
                            String          msgDesc      = "getHBaseStatus: Looks like `Master` is not running, so couldn't check that running HBase is available or not, Please try again later.";
                            HadoopException hdpException = new HadoopException(msgDesc, mnre);

                            hdpException.generateResponseDataMap(false, getMessage(mnre), msgDesc + ERROR_MSG, null, null);

                            LOG.error(msgDesc + mnre);

                            throw hdpException;
                        } catch (IOException io) {
                            String          msgDesc      = "getHBaseStatus: Unable to check availability of Hbase environment [" + getConfigHolder().getDatasourceName() + "].";
                            HadoopException hdpException = new HadoopException(msgDesc, io);

                            hdpException.generateResponseDataMap(false, getMessage(io), msgDesc + ERROR_MSG, null, null);

                            LOG.error(msgDesc + io);

                            throw hdpException;
                        } catch (Throwable e) {
                            String          msgDesc      = "getHBaseStatus: Unable to check availability of Hbase environment [" + getConfigHolder().getDatasourceName() + "].";
                            HadoopException hdpException = new HadoopException(msgDesc, e);

                            hdpException.generateResponseDataMap(false, getMessage(e), msgDesc + ERROR_MSG, null, null);

                            LOG.error(msgDesc + e);

                            throw hdpException;
                        }

                        return hbaseStatus1;
                    }
                });
            } catch (SecurityException se) {
                String          msgDesc      = "getHBaseStatus: Unable to connect to HBase Server instance ";
                HadoopException hdpException = new HadoopException(msgDesc, se);

                hdpException.generateResponseDataMap(false, getMessage(se), msgDesc + ERROR_MSG, null, null);

                LOG.error(msgDesc + se);

                throw hdpException;
            }
        } else {
            LOG.error("getHBaseStatus: secure login not done, subject is null");
        }

        return hbaseStatus;
    }

    public List<String> getTableList(final String tableNameMatching, final List<String> existingTableList) throws HadoopException {
        LOG.debug("==> HbaseClient.getTableList()  tableNameMatching {} ExisitingTableList {}", tableNameMatching, existingTableList);

        List<String> ret  = null;
        Subject      subj = getLoginSubject();

        if (subj != null) {
            ret = Subject.doAs(subj, new PrivilegedAction<List<String>>() {
                @Override
                public List<String> run() {
                    List<String> tableList = new ArrayList<>();
                    Admin        admin     = null;

                    try {
                        LOG.info("getTableList: setting config values from client");

                        setClientConfigValues(conf);

                        LOG.info("getTableList: checking HbaseAvailability with the new config");

                        try (Connection conn = ConnectionFactory.createConnection(conf)) {
                            LOG.info("getTableList: no exception: HbaseAvailability true");

                            admin = conn.getAdmin();

                            List<TableDescriptor> htds = admin.listTableDescriptors(Pattern.compile(tableNameMatching));

                            if (htds != null) {
                                for (TableDescriptor htd : htds) {
                                    String tableName = htd.getTableName().getNameAsString();

                                    if (existingTableList != null && existingTableList.contains(tableName)) {
                                        continue;
                                    } else {
                                        tableList.add(htd.getTableName().getNameAsString());
                                    }
                                }
                            } else {
                                LOG.error("getTableList: null HTableDescription received from HBaseAdmin.listTables");
                            }
                        }
                    } catch (ZooKeeperConnectionException zce) {
                        String          msgDesc      = "getTableList: Unable to connect to `ZooKeeper` using given config parameters.";
                        HadoopException hdpException = new HadoopException(msgDesc, zce);

                        hdpException.generateResponseDataMap(false, getMessage(zce), msgDesc + ERROR_MSG, null, null);

                        LOG.error(msgDesc + zce);

                        throw hdpException;
                    } catch (MasterNotRunningException mnre) {
                        String          msgDesc      = "getTableList: Looks like `Master` is not running, so couldn't check that running HBase is available or not, Please try again later.";
                        HadoopException hdpException = new HadoopException(msgDesc, mnre);

                        hdpException.generateResponseDataMap(false, getMessage(mnre), msgDesc + ERROR_MSG, null, null);

                        LOG.error(msgDesc + mnre);

                        throw hdpException;
                    } catch (IOException io) {
                        String          msgDesc      = "getTableList: Unable to get HBase table List for [repository:" + getConfigHolder().getDatasourceName() + ",table-match:" + tableNameMatching + "].";
                        HadoopException hdpException = new HadoopException(msgDesc, io);

                        hdpException.generateResponseDataMap(false, getMessage(io), msgDesc + ERROR_MSG, null, null);

                        LOG.error(msgDesc + io);

                        throw hdpException;
                    } catch (Throwable e) {
                        String          msgDesc      = "getTableList : Unable to get HBase table List for [repository:" + getConfigHolder().getDatasourceName() + ",table-match:" + tableNameMatching + "].";
                        HadoopException hdpException = new HadoopException(msgDesc, e);

                        hdpException.generateResponseDataMap(false, getMessage(e), msgDesc + ERROR_MSG, null, null);

                        LOG.error(msgDesc + e);

                        throw hdpException;
                    } finally {
                        if (admin != null) {
                            try {
                                admin.close();
                            } catch (IOException e) {
                                LOG.error("Unable to close HBase connection [{}]", getConfigHolder().getDatasourceName(), e);
                            }
                        }
                    }

                    return tableList;
                }
            });
        }

        LOG.debug("<== HbaseClient.getTableList() {}", ret);

        return ret;
    }

    public List<String> getColumnFamilyList(final String columnFamilyMatching, final List<String> tableList, final List<String> existingColumnFamilies) {
        LOG.debug("==> HbaseClient.getColumnFamilyList()  columnFamilyMatching {} ExisitingTableList {}existingColumnFamilies {}", columnFamilyMatching, tableList, existingColumnFamilies);

        List<String> ret  = null;
        Subject      subj = getLoginSubject();

        if (subj != null) {
            try {
                ret = Subject.doAs(subj, new PrivilegedAction<List<String>>() {
                    String tblName;

                    @Override
                    public List<String> run() {
                        List<String> colfList = new ArrayList<>();
                        Admin        admin    = null;

                        try {
                            LOG.info("getColumnFamilyList: setting config values from client");

                            setClientConfigValues(conf);

                            LOG.info("getColumnFamilyList: checking HbaseAvailability with the new config");

                            try (Connection conn = ConnectionFactory.createConnection(conf)) {
                                LOG.info("getColumnFamilyList: no exception: HbaseAvailability true");

                                admin = conn.getAdmin();

                                if (tableList != null) {
                                    for (String tableName : tableList) {
                                        tblName = tableName;

                                        TableDescriptor htd = admin.getDescriptor(TableName.valueOf(tableName));
                                        if (htd != null) {
                                            for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
                                                String colf = hcd.getNameAsString();

                                                if (colf.matches(columnFamilyMatching)) {
                                                    if (existingColumnFamilies != null && existingColumnFamilies.contains(colf)) {
                                                        continue;
                                                    } else {
                                                        colfList.add(colf);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (ZooKeeperConnectionException zce) {
                            String          msgDesc      = "getColumnFamilyList: Unable to connect to `ZooKeeper` using given config parameters.";
                            HadoopException hdpException = new HadoopException(msgDesc, zce);

                            hdpException.generateResponseDataMap(false, getMessage(zce), msgDesc + ERROR_MSG, null, null);

                            LOG.error(msgDesc + zce);

                            throw hdpException;
                        } catch (MasterNotRunningException mnre) {
                            String          msgDesc      = "getColumnFamilyList: Looks like `Master` is not running, so couldn't check that running HBase is available or not, Please try again later.";
                            HadoopException hdpException = new HadoopException(msgDesc, mnre);

                            hdpException.generateResponseDataMap(false, getMessage(mnre), msgDesc + ERROR_MSG, null, null);

                            LOG.error(msgDesc + mnre);

                            throw hdpException;
                        } catch (IOException io) {
                            String          msgDesc      = "getColumnFamilyList: Unable to get HBase ColumnFamilyList for [repository:" + getConfigHolder().getDatasourceName() + ",table:" + tblName + ", table-match:" + columnFamilyMatching + "] ";
                            HadoopException hdpException = new HadoopException(msgDesc, io);

                            hdpException.generateResponseDataMap(false, getMessage(io), msgDesc + ERROR_MSG, null, null);

                            LOG.error(msgDesc + io);

                            throw hdpException;
                        } catch (SecurityException se) {
                            String          msgDesc      = "getColumnFamilyList: Unable to get HBase ColumnFamilyList for [repository:" + getConfigHolder().getDatasourceName() + ",table:" + tblName + ", table-match:" + columnFamilyMatching + "] ";
                            HadoopException hdpException = new HadoopException(msgDesc, se);

                            hdpException.generateResponseDataMap(false, getMessage(se), msgDesc + ERROR_MSG, null, null);

                            LOG.error(msgDesc + se);

                            throw hdpException;
                        } catch (Throwable e) {
                            String          msgDesc      = "getColumnFamilyList: Unable to get HBase ColumnFamilyList for [repository:" + getConfigHolder().getDatasourceName() + ",table:" + tblName + ", table-match:" + columnFamilyMatching + "] ";
                            HadoopException hdpException = new HadoopException(msgDesc, e);

                            hdpException.generateResponseDataMap(false, getMessage(e), msgDesc + ERROR_MSG, null, null);

                            LOG.error(msgDesc + e);

                            throw hdpException;
                        } finally {
                            if (admin != null) {
                                try {
                                    admin.close();
                                } catch (IOException e) {
                                    LOG.error("Unable to close HBase connection [{}]", getConfigHolder().getDatasourceName(), e);
                                }
                            }
                        }

                        return colfList;
                    }
                });
            } catch (SecurityException se) {
                String          msgDesc      = "getColumnFamilyList: Unable to connect to HBase Server instance ";
                HadoopException hdpException = new HadoopException(msgDesc, se);

                hdpException.generateResponseDataMap(false, getMessage(se), msgDesc + ERROR_MSG, null, null);

                LOG.error(msgDesc + se);

                throw hdpException;
            }
        }

        LOG.debug("<== HbaseClient.getColumnFamilyList() {}", ret);

        return ret;
    }

    //TODO: temporary solution - to be added to the UI for HBase
    private static Map<String, String> addDefaultHBaseProp(Map<String, String> connectionProp) {
        if (connectionProp != null) {
            String param         = "zookeeper.znode.parent";
            String unsecuredPath = "/hbase-unsecure";
            String authParam     = "hadoop.security.authorization";
            String ret           = connectionProp.get(param);

            LOG.info("HBase connection has [{}] with value [{}]", param, ret);

            if (ret == null) {
                ret = connectionProp.get(authParam);

                LOG.info("HBase connection has [{}] with value [{}]", authParam, ret);

                if (ret != null && ret.trim().equalsIgnoreCase("false")) {
                    LOG.info("HBase connection is resetting [{}] with value [{}]", param, unsecuredPath);

                    connectionProp.put(param, unsecuredPath);
                }
            }
        }

        return connectionProp;
    }

    private void setClientConfigValues(Configuration conf) {
        if (this.connectionProperties == null) {
            return;
        }

        for (Entry<String, String> e : this.connectionProperties.entrySet()) {
            String v = conf.get(e.getKey());

            if (v != null && !v.equalsIgnoreCase(e.getValue())) {
                conf.set(e.getKey(), e.getValue());
            }
        }
    }
}
