package com.xasecure.hbase.client;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
				if (getConfigHolder().getKeyTabFile() == null) {
						subj = SecureClientLogin.loginUserWithPassword(getConfigHolder().getUserName(), getConfigHolder().getPassword()) ;
				}
				else {
					subj = SecureClientLogin.loginUserFromKeytab(getConfigHolder().getUserName() , getConfigHolder().getKeyTabFile()) ;
				}
			}
			else {
				subj = SecureClientLogin.login(getConfigHolder().getUserName()) ;
			}
		} catch (IOException e) {
			LOG.error("Unable to perform secure login to Hive environment [" + getConfigHolder().getDatasourceName() + "]", e);
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
