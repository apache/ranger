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

package org.apache.ranger.audit.destination;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import org.apache.ranger.audit.dao.DaoManager;
import org.apache.ranger.audit.entity.AuthzAuditEventDbObj;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.provider.MiscUtil;

public class DBAuditDestination extends AuditDestination {

	private static final Log logger = LogFactory
			.getLog(DBAuditDestination.class);

	public static final String PROP_DB_JDBC_DRIVER = "jdbc.driver";
	public static final String PROP_DB_JDBC_URL = "jdbc.url";
	public static final String PROP_DB_USER = "user";
	public static final String PROP_DB_PASSWORD = "password";
	public static final String PROP_DB_PASSWORD_ALIAS = "password.alias";

	private EntityManagerFactory entityManagerFactory;
	private DaoManager daoManager;

	private String jdbcDriver = null;
	private String jdbcURL = null;
	private String dbUser = null;
	private String dbPasswordAlias = "auditDBCred";

	public DBAuditDestination() {
		logger.info("DBAuditDestination() called");
	}

	@Override
	public void init(Properties props, String propPrefix) {
		logger.info("init() called");
		super.init(props, propPrefix);
		// Initial connect
		connect();

		// initialize the database related classes
		AuthzAuditEventDbObj.init(props);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.apache.ranger.audit.provider.AuditHandler#logger(java.util.Collection
	 * )
	 */
	@Override
	public boolean log(Collection<AuditEventBase> events) {
		boolean retValue = false;
		logStatusIfRequired();
		addTotalCount(events.size());
		
		if (beginTransaction()) {
			boolean isFailed = false;
			for (AuditEventBase event : events) {
				try {
					event.persist(daoManager);
				} catch (Throwable t) {
					logger.error("Error persisting data. event=" + event, t);
					isFailed = true;
					break;
				}
			}
			if (isFailed) {
				retValue = false;
				rollbackTransaction();
			} else {
				retValue = commitTransaction();
			}
		}
		
		if (retValue) {
			addSuccessCount(events.size());
		} else {
			addDeferredCount(events.size());
		}
		return retValue;
	}

	@Override
	public void stop() {
		cleanUp();
		super.stop();
	}

	// Local methods
	protected void connect() {
		if (isDbConnected()) {
			return;
		}
		try {
			jdbcDriver = MiscUtil.getStringProperty(props, propPrefix + "."
					+ PROP_DB_JDBC_DRIVER);
			jdbcURL = MiscUtil.getStringProperty(props, propPrefix + "."
					+ PROP_DB_JDBC_URL);
			dbUser = MiscUtil.getStringProperty(props, propPrefix + "."
					+ PROP_DB_USER);
			String dbPasswordFromProp = MiscUtil.getStringProperty(props,
					propPrefix + "." + PROP_DB_PASSWORD);
			String tmpAlias = MiscUtil.getStringProperty(props, propPrefix
					+ "." + PROP_DB_PASSWORD_ALIAS);
			dbPasswordAlias = tmpAlias != null ? tmpAlias : dbPasswordAlias;
			String credFile = MiscUtil.getStringProperty(props,
					AUDIT_DB_CREDENTIAL_PROVIDER_FILE);

			if (jdbcDriver == null || jdbcDriver.isEmpty()) {
				logger.fatal("JDBC driver not provided. Set property name "
						+ propPrefix + "." + PROP_DB_JDBC_DRIVER);
				return;
			}
			if (jdbcURL == null || jdbcURL.isEmpty()) {
				logger.fatal("JDBC URL not provided. Set property name "
						+ propPrefix + "." + PROP_DB_JDBC_URL);
				return;
			}
			if (dbUser == null || dbUser.isEmpty()) {
				logger.fatal("DB user not provided. Set property name "
						+ propPrefix + "." + PROP_DB_USER);
				return;
			}
			String dbPassword = MiscUtil.getCredentialString(credFile,
					dbPasswordAlias);

			if (dbPassword == null || dbPassword.isEmpty()) {
				// If password is not in credential store, let's try password
				// from property
				dbPassword = dbPasswordFromProp;
			}

			if (dbPassword == null || dbPassword.isEmpty()) {
				logger.warn("DB password not provided. Will assume it is empty and continue");
			}
			logger.info("JDBC Driver=" + jdbcDriver + ", JDBC URL=" + jdbcURL
					+ ", dbUser=" + dbUser + ", passwordAlias="
					+ dbPasswordAlias + ", credFile=" + credFile
					+ ", usingPassword=" + (dbPassword == null ? "no" : "yes"));

			Map<String, String> dbProperties = new HashMap<String, String>();
			dbProperties.put("javax.persistence.jdbc.driver", jdbcDriver);
			dbProperties.put("javax.persistence.jdbc.url", jdbcURL);
			dbProperties.put("javax.persistence.jdbc.user", dbUser);
			if (dbPassword != null) {
				dbProperties.put("javax.persistence.jdbc.password", dbPassword);
			}

			entityManagerFactory = Persistence.createEntityManagerFactory(
					"xa_server", dbProperties);

			logger.info("entityManagerFactory=" + entityManagerFactory);

			daoManager = new DaoManager();
			daoManager.setEntityManagerFactory(entityManagerFactory);

			// this forces the connection to be made to DB
			if (daoManager.getEntityManager() == null) {
				logger.error("Error connecting audit database. EntityManager is null. dbURL="
						+ jdbcURL + ", dbUser=" + dbUser);
			} else {
				logger.info("Connected to audit database. dbURL=" + jdbcURL
						+ ", dbUser=" + dbUser);
			}

		} catch (Throwable t) {
			logger.error("Error connecting audit database. dbURL=" + jdbcURL
					+ ", dbUser=" + dbUser, t);
		}
	}

	private synchronized void cleanUp() {
		logger.info("DBAuditDestination: cleanUp()");

		try {
			if (entityManagerFactory != null && entityManagerFactory.isOpen()) {
				entityManagerFactory.close();
			}
		} catch (Exception excp) {
			logger.error("DBAuditDestination.cleanUp(): failed", excp);
		} finally {
			entityManagerFactory = null;
			daoManager = null;
		}
		logStatus();
	}

	private EntityManager getEntityManager() {
		DaoManager daoMgr = daoManager;

		if (daoMgr != null) {
			try {
				return daoMgr.getEntityManager();
			} catch (Exception excp) {
				logger.error("DBAuditDestination.getEntityManager(): failed",
						excp);

				cleanUp();
			}
		}

		return null;
	}

	private boolean isDbConnected() {
		EntityManager em = getEntityManager();
		return em != null && em.isOpen();
	}

	private void clearEntityManager() {
		try {
			EntityManager em = getEntityManager();

			if (em != null) {
				em.clear();
			}
		} catch (Exception excp) {
			logger.warn("DBAuditDestination.clearEntityManager(): failed", excp);
		}
	}

	private EntityTransaction getTransaction() {
		if (!isDbConnected()) {
			connect();
		}

		EntityManager em = getEntityManager();

		return em != null ? em.getTransaction() : null;
	}

	private boolean beginTransaction() {
		EntityTransaction trx = getTransaction();

		if (trx != null && !trx.isActive()) {
			trx.begin();
		}

		if (trx == null) {
			logger.warn("DBAuditDestination.beginTransaction(): trx is null");
		}

		return trx != null;
	}

	private boolean commitTransaction() {
		boolean ret = false;
		EntityTransaction trx = null;

		try {
			trx = getTransaction();

			if (trx != null && trx.isActive()) {
				trx.commit();
				ret = true;
			} else {
				throw new Exception("trx is null or not active");
			}
		} catch (Throwable excp) {
			logger.error("DBAuditDestination.commitTransaction(): failed", excp);

			cleanUp(); // so that next insert will try to init()
		} finally {
			clearEntityManager();
		}

		return ret;
	}

	private boolean rollbackTransaction() {
		boolean ret = false;
		EntityTransaction trx = null;

		try {
			trx = getTransaction();

			if (trx != null && trx.isActive()) {
				trx.rollback();
				ret = true;
			} else {
				throw new Exception("trx is null or not active");
			}
		} catch (Throwable excp) {
			logger.error("DBAuditDestination.rollbackTransaction(): failed",
					excp);

			cleanUp(); // so that next insert will try to init()
		} finally {
			clearEntityManager();
		}

		return ret;
	}

}
