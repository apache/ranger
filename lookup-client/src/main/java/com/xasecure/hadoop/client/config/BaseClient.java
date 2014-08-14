package com.xasecure.hadoop.client.config;

import java.io.IOException;
import java.util.HashMap;

import javax.security.auth.Subject;

import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.UserGroupInformation;

import com.xasecure.hadoop.client.exceptions.HadoopException;

public abstract class BaseClient {
	
	private String dataSource ;
	private Subject loginSubject ;
	private HadoopConfigHolder configHolder;
	
	protected HashMap<String,String> connectionProperties ;
	
	public BaseClient(String dataSource) {
		this.dataSource = dataSource ;
		init() ;
		login() ;
	}
	
	public BaseClient(String dataSource, HashMap<String,String> connectionProperties) {
		this.dataSource = dataSource ;
		this.connectionProperties = connectionProperties ;
		init() ;
		login() ;
	}
	
	
	private void init() {
		if (connectionProperties == null) {
			configHolder = HadoopConfigHolder.getInstance(dataSource) ;
		}
		else {
			configHolder = HadoopConfigHolder.getInstance(dataSource,connectionProperties) ;
		}
	}
	
	
	protected void login() {
		ClassLoader prevCl = Thread.currentThread().getContextClassLoader() ;
		try {
			Thread.currentThread().setContextClassLoader(configHolder.getClassLoader());
			String userName = configHolder.getUserName() ;
			if (userName == null) {
				throw new HadoopException("Unable to find login username for hadoop environment, [" + dataSource + "]") ;
			}
			String keyTabFile = configHolder.getKeyTabFile() ;
			if (keyTabFile != null) {
				if ( UserGroupInformation.isSecurityEnabled() ) {
					loginSubject = SecureClientLogin.loginUserFromKeytab(userName, keyTabFile) ;
				}
				else {
					loginSubject = SecureClientLogin.login(userName) ;
				}
			}
			else {
				String password = configHolder.getPassword() ;
				if ( UserGroupInformation.isSecurityEnabled() ) {
					loginSubject = SecureClientLogin.loginUserWithPassword(userName, password) ;
				}
				else {
					loginSubject = SecureClientLogin.login(userName) ;
				}
			}
		}
		catch(IOException ioe) {
			throw new HadoopException("Unable to login to Hadoop environment [" + dataSource + "]", ioe) ;
		}
		finally {
			Thread.currentThread().setContextClassLoader(prevCl);
		}
	}
	
	public String getDataSource() {
		return dataSource ;
	}

	protected Subject getLoginSubject() {
		return loginSubject;
	}

	protected HadoopConfigHolder getConfigHolder() {
		return configHolder;
	}
	
	

}
