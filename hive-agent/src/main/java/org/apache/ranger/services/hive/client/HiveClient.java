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

 package org.apache.ranger.services.hive.client;

import java.io.Closeable;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.security.auth.Subject;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;

public class HiveClient extends BaseClient implements Closeable {

	private static final Log LOG = LogFactory.getLog(HiveClient.class) ;
	
	Connection con = null ;
	boolean isKerberosAuth=false;
	

	public HiveClient(String serviceName) {
		super(serviceName, null) ;
		initHive() ;
	}
	
	public HiveClient(String serviceName,Map<String,String> connectionProp) {
		super(serviceName,connectionProp) ;
		initHive() ;
	}
	
	public void initHive() {
		isKerberosAuth = getConfigHolder().isKerberosAuthentication();
		if (isKerberosAuth) {
			LOG.info("Secured Mode: JDBC Connection done with preAuthenticated Subject");
			Subject.doAs(getLoginSubject(), new PrivilegedAction<Object>() {
				public Object run() {
					initConnection();
					return null;
				}
			}) ;				
		}
		else {
			LOG.info("Since Password is NOT provided, Trying to use UnSecure client with username and password");
			final String userName = getConfigHolder().getUserName() ;
			final String password = getConfigHolder().getPassword() ;
			Subject.doAs(getLoginSubject(), new PrivilegedAction<Object>() {
				public Object run() {
					initConnection(userName,password);
					return null;
				}
			}) ;	
		}
	}
	
	public List<String> getDatabaseList(String databaseMatching, final List<String> databaseList){
	 	final String 	   dbMatching = databaseMatching;
	 	final List<String> dbList	  = databaseList;
		List<String> dblist = Subject.doAs(getLoginSubject(), new PrivilegedAction<List<String>>() {
			public List<String>  run() {
				return getDBList(dbMatching,dbList);
			}
		}) ;
		return dblist;
	}
		
	private List<String> getDBList(String databaseMatching, List<String>dbList) {
		List<String> ret = new ArrayList<String>() ;
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		if (con != null) {
			Statement stat =  null ;
			ResultSet rs = null ;
			String sql = "show databases" ;
			if (databaseMatching != null && ! databaseMatching.isEmpty()) {
				sql = sql + " like \"" + databaseMatching  + "\"" ;
			}
			try {
				if(LOG.isDebugEnabled()) {
					LOG.debug("<== HiveClient getDBList databaseMatching : " + databaseMatching + " ExcludedbList :" + dbList) ;
				}
				stat =  con.createStatement()  ;
				rs = stat.executeQuery(sql) ;
				while (rs.next()) {
					String dbName = rs.getString(1);
					if ( dbList != null && dbList.contains(dbName)) {
						continue;
					}
					ret.add(rs.getString(1)) ;
				}
			} catch (SQLTimeoutException sqlt) {
				String msgDesc = "Time Out, Unable to execute SQL [" + sql
						+ "].";
				HadoopException hdpException = new HadoopException(msgDesc,
						sqlt);
				hdpException.generateResponseDataMap(false, getMessage(sqlt),
						msgDesc + errMsg, null, null);
				if(LOG.isDebugEnabled()) {
					LOG.debug("<== HiveClient.getDBList() Error : " + sqlt) ;
				}
				throw hdpException;
			} catch (SQLException sqle) {
				String msgDesc = "Unable to execute SQL [" + sql + "].";
				HadoopException hdpException = new HadoopException(msgDesc,
						sqle);
				hdpException.generateResponseDataMap(false, getMessage(sqle),
						msgDesc + errMsg, null, null);
				if(LOG.isDebugEnabled()) {
					LOG.debug("<== HiveClient.getDBList() Error : " + sqle) ;
				}
				throw hdpException;
			} finally {
				close(rs) ;
				close(stat) ;
			}
			
		}
		return ret ;
	}
	
	public List<String> getTableList(String tableNameMatching, List<String> databaseList, List<String> tblNameList){
		final String 	   tblNameMatching = tableNameMatching;
		final List<String> dbList  	 	   = databaseList;
		final List<String> tblList   	   = tblNameList;
		
		List<String> tableList = Subject.doAs(getLoginSubject(), new PrivilegedAction<List<String>>() {
			public List<String>  run() {
				return getTblList(tblNameMatching,dbList,tblList);
			}
		}) ;
		return tableList;
	}

	public List<String> getTblList(String tableNameMatching, List<String> dbList, List<String> tblList) {
		List<String> ret = new ArrayList<String>() ;
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		if (con != null) {
			Statement stat =  null ;
			ResultSet rs = null ;
			
			String sql = null ;
			
			try {
				
				if(LOG.isDebugEnabled()) {
					LOG.debug("<== HiveClient getTblList tableNameMatching: " + tableNameMatching + " dbList :" + dbList + " tblList: " + tblList) ;
				}
				if (dbList != null && !dbList.isEmpty()) {
					for ( String db: dbList) {
						sql = "use " + db;
						
						try {
							stat = con.createStatement() ;
							stat.execute(sql) ;
						}
						finally {
							close(stat) ;
                            stat = null;
						}
						
						sql = "show tables " ;
						if (tableNameMatching != null && ! tableNameMatching.isEmpty()) {
							sql = sql + " like \"" + tableNameMatching  + "\"" ;
						}
                        try {
                            stat = con.createStatement();
                            rs = stat.executeQuery(sql);
                            while (rs.next()) {
                                String tblName = rs.getString(1);
                                if (tblList != null && tblList.contains(tblName)) {
                                    continue;
                                }
                                ret.add(tblName);
                            }
                        } finally {
                            close(rs);
                            close(stat);
                            rs = null;
                            stat = null;
                        }
					 }
				}
			} catch (SQLTimeoutException sqlt) {
				String msgDesc = "Time Out, Unable to execute SQL [" + sql
						+ "].";
				HadoopException hdpException = new HadoopException(msgDesc,
						sqlt);
				hdpException.generateResponseDataMap(false, getMessage(sqlt),
						msgDesc + errMsg, null, null);
				if(LOG.isDebugEnabled()) {
					LOG.debug("<== HiveClient.getTblList() Error : " + sqlt) ;
				}
				throw hdpException;
			} catch (SQLException sqle) {
				String msgDesc = "Unable to execute SQL [" + sql + "].";
				HadoopException hdpException = new HadoopException(msgDesc,
						sqle);
				hdpException.generateResponseDataMap(false, getMessage(sqle),
						msgDesc + errMsg, null, null);
				if(LOG.isDebugEnabled()) {
					LOG.debug("<== HiveClient.getTblList() Error : " + sqle) ;
				}
				throw hdpException;
			}
			
		}
		return ret ;
	}

	public List<String> getViewList(String database, String viewNameMatching) {
		List<String> ret = null ;
		return ret ;
	}

	public List<String> getUDFList(String database, String udfMatching) {
		List<String> ret = null ;
		return ret ;
	}
	
	public List<String> getColumnList(String columnNameMatching, List<String> dbList, List<String> tblList, List<String> colList) {
		final String clmNameMatching    = columnNameMatching;
		final List<String> databaseList = dbList;
		final List<String> tableList    = tblList;
		final List<String> clmList 	= colList;
		List<String> columnList = Subject.doAs(getLoginSubject(), new PrivilegedAction<List<String>>() {
			public List<String>  run() {
					return getClmList(clmNameMatching,databaseList,tableList,clmList);
				}
			}) ;
		return columnList;
	}
	
	public List<String> getClmList(String columnNameMatching,List<String> dbList, List<String> tblList, List<String> colList) {
		List<String> ret = new ArrayList<String>() ;
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		if (con != null) {
			
			String columnNameMatchingRegEx = null ;
			
			if (columnNameMatching != null && ! columnNameMatching.isEmpty()) {
				columnNameMatchingRegEx = columnNameMatching ;
			}
			
			Statement stat =  null ;
			ResultSet rs = null ;
			
			String sql = null ;
			
			if(LOG.isDebugEnabled()) {
				LOG.debug("<== HiveClient.getClmList() columnNameMatching: " + columnNameMatching + " dbList :" + dbList +  " tblList: " + tblList + " colList: " + colList) ;
			}
			
			if (dbList != null && !dbList.isEmpty() && 
				tblList != null && !tblList.isEmpty()) {
				for (String db: dbList) {
					for(String tbl:tblList) { 
						try {
							sql = "use " + db;
							
							try {
								stat = con.createStatement() ;
								stat.execute(sql) ;
							}
							finally {
								close(stat) ;
							}
							
							sql = "describe  " + tbl ;
							stat =  con.createStatement()  ;
							rs = stat.executeQuery(sql) ;
							while (rs.next()) {
								String columnName = rs.getString(1) ;
								if (colList != null && colList.contains(columnName)) {
									continue;
								}
								if (columnNameMatchingRegEx == null) {
									ret.add(columnName) ;
								}
								else if (FilenameUtils.wildcardMatch(columnName,columnNameMatchingRegEx)) {
									ret.add(columnName) ;
								}
							  }
			
							} catch (SQLTimeoutException sqlt) {
								String msgDesc = "Time Out, Unable to execute SQL [" + sql
										+ "].";
								HadoopException hdpException = new HadoopException(msgDesc,
										sqlt);
								hdpException.generateResponseDataMap(false, getMessage(sqlt),
										msgDesc + errMsg, null, null);
								if(LOG.isDebugEnabled()) {
									LOG.debug("<== HiveClient.getClmList() Error : " + sqlt) ;
								}
								throw hdpException;
							} catch (SQLException sqle) {
								String msgDesc = "Unable to execute SQL [" + sql + "].";
								HadoopException hdpException = new HadoopException(msgDesc,
										sqle);
								hdpException.generateResponseDataMap(false, getMessage(sqle),
										msgDesc + errMsg, null, null);
								if(LOG.isDebugEnabled()) {
									LOG.debug("<== HiveClient.getClmList() Error : " + sqle) ;
								}
								throw hdpException;
							} finally {
								close(rs) ;
								close(stat) ;
							}
					}
				}
			}
		}
		return ret ;
	}
	
	
	public void close() {
		Subject.doAs(getLoginSubject(), new PrivilegedAction<Void>(){
			public Void run() {
				close(con) ;
				return null;
			}
		});
	}
	
	private void close(Statement aStat) {
		try {
			if (aStat != null) {
				aStat.close();
			}
		} catch (SQLException e) {
			LOG.error("Unable to close SQL statement", e);
		}
	}

	private void close(ResultSet aResultSet) {
		try {
			if (aResultSet != null) {
				aResultSet.close();
			}
		} catch (SQLException e) {
			LOG.error("Unable to close ResultSet", e);
		}
	}

	private void close(Connection aCon) {
		try {
			if (aCon != null) {
				aCon.close();
			}
		} catch (SQLException e) {
			LOG.error("Unable to close SQL Connection", e);
		}
	}

	private void initConnection() {
		initConnection(null,null) ;
	}

	
	private void initConnection(String userName, String password) {
	
		Properties prop = getConfigHolder().getRangerSection() ;
		String driverClassName = prop.getProperty("jdbc.driverClassName") ;
		String url =  prop.getProperty("jdbc.url") ;	
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
	
		if (driverClassName != null) {
			try {
				Driver driver = (Driver)Class.forName(driverClassName).newInstance() ;
				DriverManager.registerDriver(driver);
			} catch (SQLException e) {
				String msgDesc = "initConnection: Caught SQLException while registering "
						+ "Hive driver, so Unable to connect to Hive Thrift Server instance.";
				HadoopException hdpException = new HadoopException(msgDesc, e);
				hdpException.generateResponseDataMap(false, getMessage(e),
						msgDesc + errMsg, null, null);
				throw hdpException;
			} catch (IllegalAccessException ilae) {
				String msgDesc = "initConnection: Class or its nullary constructor might not accessible."
						+ "So unable to initiate connection to hive thrift server instance.";
				HadoopException hdpException = new HadoopException(msgDesc, ilae);
				hdpException.generateResponseDataMap(false, getMessage(ilae),
						msgDesc + errMsg, null, null);
				throw hdpException;
			} catch (InstantiationException ie) {
				String msgDesc = "initConnection: Class may not have its nullary constructor or "
						+ "may be the instantiation fails for some other reason."
						+ "So unable to initiate connection to hive thrift server instance.";
				HadoopException hdpException = new HadoopException(msgDesc, ie);
				hdpException.generateResponseDataMap(false, getMessage(ie),
						msgDesc + errMsg, null, null);
				throw hdpException;
				
			} catch (ExceptionInInitializerError eie) {
				String msgDesc = "initConnection: Got ExceptionInInitializerError, "
						+ "The initialization provoked by this method fails."
						+ "So unable to initiate connection to hive thrift server instance.";
				HadoopException hdpException = new HadoopException(msgDesc, eie);
				hdpException.generateResponseDataMap(false, getMessage(eie),
						msgDesc + errMsg, null, null);
				throw hdpException;
			} catch (SecurityException se) {
				String msgDesc = "initConnection: unable to initiate connection to hive thrift server instance,"
						+ " The caller's class loader is not the same as or an ancestor "
						+ "of the class loader for the current class and invocation of "
						+ "s.checkPackageAccess() denies access to the package of this class.";
				HadoopException hdpException = new HadoopException(msgDesc, se);
				hdpException.generateResponseDataMap(false, getMessage(se),
						msgDesc + errMsg, null, null);
				throw hdpException;
			} catch (Throwable t) {
				String msgDesc = "initConnection: Unable to connect to Hive Thrift Server instance, "
						+ "please provide valid value of field : {jdbc.driverClassName}.";
				HadoopException hdpException = new HadoopException(msgDesc, t);
				hdpException.generateResponseDataMap(false, getMessage(t),
						msgDesc + errMsg, null, "jdbc.driverClassName");
				throw hdpException;
			}
		}
		
		try {
			
			if (userName == null && password == null) {
				con = DriverManager.getConnection(url) ;
			}
			else {			
				con = DriverManager.getConnection(url, userName, password) ;
			}
		
		} catch (SQLException e) {
			String msgDesc = "Unable to connect to Hive Thrift Server instance.";
			HadoopException hdpException = new HadoopException(msgDesc, e);
			hdpException.generateResponseDataMap(false, getMessage(e), msgDesc
					+ errMsg, null, null);
			throw hdpException;
		} catch (SecurityException se) {
			String msgDesc = "Unable to connect to Hive Thrift Server instance.";
			HadoopException hdpException = new HadoopException(msgDesc, se);
			hdpException.generateResponseDataMap(false, getMessage(se), msgDesc
					+ errMsg, null, null);
			throw hdpException;
		} catch ( Throwable t) {
			String msgDesc = "Unable to connect to Hive Thrift Server instance";
			HadoopException hdpException = new HadoopException(msgDesc, t);
			hdpException.generateResponseDataMap(false, getMessage(t),
					msgDesc + errMsg, null, url);
		     throw hdpException;
		}
	}

	
	public static void main(String[] args) {
		
		HiveClient hc = null ;
		
		if (args.length == 0) {
			System.err.println("USAGE: java " + HiveClient.class.getName() + " dataSourceName <databaseName> <tableName> <columnName>") ;
			System.exit(1) ;
		}
		
		try {
			hc = new HiveClient(args[0]) ;
			
			if (args.length == 2) {
				List<String> dbList = hc.getDatabaseList(args[1],null) ;
				if (dbList.size() == 0) {
					System.out.println("No database found with db filter [" + args[1] + "]") ;
				}
				else {
					for (String str : dbList ) {
						System.out.println("database: " + str ) ;
					}
				}
			}
			else if (args.length == 3) {
				List<String> tableList = hc.getTableList(args[2],null,null) ;
				if (tableList.size() == 0) {
					System.out.println("No tables found under database[" + args[1] + "] with table filter [" + args[2] + "]") ;
				}
				else {
					for(String str : tableList) {
						System.out.println("Table: " + str) ;
					}
				}
			}
			else if (args.length == 4) {
				List<String> columnList = hc.getColumnList(args[3],null,null,null) ;
				if (columnList.size() == 0) {
					System.out.println("No columns found for db:" + args[1] + ", table: [" + args[2] + "], with column filter [" + args[3] + "]") ;
				}
				else {
					for (String str : columnList ) {
						System.out.println("Column: " + str) ;
					}
				}
			}
			
		}
		finally {
			if (hc != null) {
				hc.close();
			}
		}	
	}

	public static HashMap<String, Object> testConnection(String serviceName,
			Map<String, String> connectionProperties) {

		HashMap<String, Object> responseData = new HashMap<String, Object>();
		boolean connectivityStatus = false;
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";

		HiveClient connectionObj = new HiveClient(serviceName,
				(HashMap<String, String>) connectionProperties);
		if (connectionObj != null) {
		
			List<String> testResult = connectionObj.getDatabaseList("*",null);
			if (testResult != null && testResult.size() != 0) {
				connectivityStatus = true;
			}
		}
		if (connectivityStatus) {
			String successMsg = "TestConnection Successful";
			generateResponseDataMap(connectivityStatus, successMsg, successMsg,
					null, null, responseData);
		} else {
			String failureMsg = "Unable to retrieve any databases using given parameters.";
			generateResponseDataMap(connectivityStatus, failureMsg, failureMsg + errMsg,
					null, null, responseData);
		}
		
		connectionObj.close();
		return responseData;
	}
	
}
