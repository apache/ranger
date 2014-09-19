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

 package com.xasecure.hbase.client;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import javax.security.auth.Subject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.UserGroupInformation;

import com.xasecure.hadoop.client.config.BaseClient;

public class HBaseClient extends BaseClient {

	private static final Log LOG = LogFactory.getLog(HBaseClient.class) ;

	private Subject subj = null ;

	public HBaseClient(String dataSource) {
		super(dataSource) ;
		initHBase() ;
	}

	public HBaseClient(String dataSource,HashMap<String,String> connectionProp) {
		super(dataSource, addDefaultHBaseProp(connectionProp)) ;
		initHBase() ;
	}
	
	//TODO: temporary solution - to be added to the UI for HBase 
	private static HashMap<String,String> addDefaultHBaseProp(HashMap<String,String> connectionProp) {
		if (connectionProp != null) {
			String param = "zookeeper.znode.parent" ;
			String unsecuredPath = "/hbase-unsecure" ;
			String authParam = "hadoop.security.authorization" ;
			
			String ret = connectionProp.get(param) ;
			LOG.info("HBase connection has [" + param + "] with value [" + ret + "]");
			if (ret == null) {
				ret = connectionProp.get(authParam) ;
				LOG.info("HBase connection has [" + authParam + "] with value [" + ret + "]");
				if (ret != null && ret.trim().equalsIgnoreCase("false")) {
					LOG.info("HBase connection is resetting [" + param + "] with value [" + unsecuredPath + "]");
					connectionProp.put(param, unsecuredPath) ;
				}
			}
		}
		return connectionProp;
	}

	public void initHBase() {
		try {
			if (UserGroupInformation.isSecurityEnabled()) {
				LOG.info("initHBase:security enabled");
				if (getConfigHolder().getKeyTabFile() == null) {
					    LOG.info("initHBase: using username/password");
						subj = SecureClientLogin.loginUserWithPassword(getConfigHolder().getUserName(), getConfigHolder().getPassword()) ;
				}
				else {
				    LOG.info("initHBase: using username/keytab");
					subj = SecureClientLogin.loginUserFromKeytab(getConfigHolder().getUserName() , getConfigHolder().getKeyTabFile()) ;
				}
			}
			else {
			    LOG.info("initHBase: security not enabled, using username");
				subj = SecureClientLogin.login(getConfigHolder().getUserName()) ;
			}
		} catch (IOException e) {
			LOG.error("Unable to perform secure login to Hbase environment [" + getConfigHolder().getDatasourceName() + "]", e);
		}
	}
	
	public boolean getHBaseStatus() {
		boolean hbaseStatus = false;
		
		if (subj != null) {
			ClassLoader prevCl = Thread.currentThread().getContextClassLoader() ;
			try {
				Thread.currentThread().setContextClassLoader(getConfigHolder().getClassLoader());
	
				hbaseStatus = Subject.doAs(subj, new PrivilegedAction<Boolean>() {
					@Override
					public Boolean run() {
						Boolean hbaseStatus1 = false;
						try {
						    LOG.info("getHBaseStatus: creating default Hbase configuration");
							Configuration conf = HBaseConfiguration.create() ;					
							LOG.info("getHBaseStatus: setting config values from client");
							setClientConfigValues(conf);						
						    LOG.info("getHBaseStatus: checking HbaseAvailability with the new config");
							HBaseAdmin.checkHBaseAvailable(conf);					
						    LOG.info("getHBaseStatus: no exception: HbaseAvailability true");
							hbaseStatus1 = true;
						} catch (Throwable e) {
							LOG.error("getHBaseStatus: Unable to check availability of Hbase environment [" + getConfigHolder().getDatasourceName() + "]", e);
							hbaseStatus1 = false;
						}
						return hbaseStatus1;
					}
				}) ;
			} finally {
				Thread.currentThread().setContextClassLoader(prevCl);
			}
		} else {
			LOG.error("getHBaseStatus: secure login not done, subject is null");
		}
		
		return hbaseStatus;
	}
	
	private void setClientConfigValues(Configuration conf) {
		if (this.connectionProperties == null) return;
		Iterator<Entry<String, String>> i =  this.connectionProperties.entrySet().iterator();
		while (i.hasNext()) {
			Entry<String, String> e = i.next();
			String v = conf.get(e.getKey());
			if (v != null && !v.equalsIgnoreCase(e.getValue())) {
				conf.set(e.getKey(), e.getValue());
			}
		}		
	}

	public List<String> getTableList(final String tableNameMatching) {
		List<String> ret = null ;
		
		if (subj != null) {
			ClassLoader prevCl = Thread.currentThread().getContextClassLoader() ;
			try {
				Thread.currentThread().setContextClassLoader(getConfigHolder().getClassLoader());
	
				ret = Subject.doAs(subj, new PrivilegedAction<List<String>>() {
		
					@Override
					public List<String> run() {
						
						List<String> tableList = new ArrayList<String>() ;
						HBaseAdmin admin = null ;
						try {
							
							Configuration conf = HBaseConfiguration.create() ;
							admin = new HBaseAdmin(conf) ;
							for (HTableDescriptor htd : admin.listTables(tableNameMatching)) {
								tableList.add(htd.getNameAsString()) ;
							}
						}
						catch(Throwable t) {
							LOG.error("Unable to get HBase table List for [repository:" + getConfigHolder().getDatasourceName() + ",table-match:" + tableNameMatching + "]", t);
						}
						finally {
							if (admin != null) {
								try {
									admin.close() ;
								} catch (IOException e) {
									LOG.error("Unable to close HBase connection [" + getConfigHolder().getDatasourceName() + "]", e);
								}
							}
						}
						return tableList ;
					}
					
				}) ;
			}
			finally {
				Thread.currentThread().setContextClassLoader(prevCl);
			}
		}
		return ret ;
	}
	
	
	public List<String> getColumnFamilyList(final String tableName, final String columnFamilyMatching) {
		List<String> ret = null ;		
		if (subj != null) {
			ClassLoader prevCl = Thread.currentThread().getContextClassLoader() ;
			try {
				Thread.currentThread().setContextClassLoader(getConfigHolder().getClassLoader());
				
				ret = Subject.doAs(subj, new PrivilegedAction<List<String>>() {
		
					@Override
					public List<String> run() {
						
						List<String> colfList = new ArrayList<String>() ;
						HBaseAdmin admin = null ;
						try {
							Configuration conf = HBaseConfiguration.create();
							admin = new HBaseAdmin(conf) ;
							HTableDescriptor htd = admin.getTableDescriptor(tableName.getBytes()) ;
							if (htd != null) {
								for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
									String colf = hcd.getNameAsString() ;
									if (colf.matches(columnFamilyMatching)) {
										if (!colfList.contains(colf)) {
											colfList.add(colf) ;
										}
									}
								}
							}
						}
						catch(Throwable t) {
							LOG.error("Unable to get HBase table List for [repository:" + getConfigHolder().getDatasourceName() + ",table:" + tableName + ", table-match:" + columnFamilyMatching + "]", t);
						}
						finally {
							if (admin != null) {
								try {
									admin.close() ;
								} catch (IOException e) {
									LOG.error("Unable to close HBase connection [" + getConfigHolder().getDatasourceName() + "]", e);
								}
							}
						}
						return colfList ;
					}
					
				}) ;
			}
			finally {
				Thread.currentThread().setContextClassLoader(prevCl);
			}
		}
		return ret ;
	}
}
