/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.provider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.dao.DaoManager;
import org.apache.ranger.audit.destination.AuditDestination;
import org.apache.ranger.audit.entity.AuthzAuditEventDbObj;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.authorization.hadoop.utils.RangerCredentialProvider;


/*
 * NOTE:
 * - Instances of this class are not thread-safe.
 */
public class DbAuditProvider extends AuditDestination {

	private static final Log LOG = LogFactory.getLog(DbAuditProvider.class);

	public static final String AUDIT_DB_IS_ASYNC_PROP           = "xasecure.audit.db.is.async";
	public static final String AUDIT_DB_MAX_QUEUE_SIZE_PROP     = "xasecure.audit.db.async.max.queue.size";
	public static final String AUDIT_DB_MAX_FLUSH_INTERVAL_PROP = "xasecure.audit.db.async.max.flush.interval.ms";

	private static final String AUDIT_DB_BATCH_SIZE_PROP            = "xasecure.audit.db.batch.size";
	private static final String AUDIT_DB_RETRY_MIN_INTERVAL_PROP    = "xasecure.audit.db.config.retry.min.interval.ms";
	private static final String AUDIT_JPA_CONFIG_PROP_PREFIX        = "xasecure.audit.jpa.";
	private static final String AUDIT_DB_CREDENTIAL_PROVIDER_FILE   = "xasecure.audit.credential.provider.file";
	private static final String AUDIT_DB_CREDENTIAL_PROVIDER_ALIAS	= "auditDBCred";
	private static final String AUDIT_JPA_JDBC_PASSWORD  			= "javax.persistence.jdbc.password";

	private EntityManagerFactory entityManagerFactory;
	private DaoManager          daoManager;
	
	private int                 mCommitBatchSize      = 1;
	private int                 mDbRetryMinIntervalMs = 60 * 1000;
	private ArrayList<AuditEventBase> mUncommitted    = new ArrayList<AuditEventBase>();
	private Map<String, String> mDbProperties         = null;
	private long                mLastDbFailedTime     = 0;

	public DbAuditProvider() {
		LOG.info("DbAuditProvider: creating..");
	}

	@Override
	public void init(Properties props) {
		LOG.info("DbAuditProvider.init()");

		super.init(props);

		mDbProperties         = MiscUtil.getPropertiesWithPrefix(props, AUDIT_JPA_CONFIG_PROP_PREFIX);
		mCommitBatchSize      = MiscUtil.getIntProperty(props, AUDIT_DB_BATCH_SIZE_PROP, 1000);
		mDbRetryMinIntervalMs = MiscUtil.getIntProperty(props, AUDIT_DB_RETRY_MIN_INTERVAL_PROP, 15 * 1000);

		boolean isAsync = MiscUtil.getBooleanProperty(props, AUDIT_DB_IS_ASYNC_PROP, false);

		if(! isAsync) {
			mCommitBatchSize = 1; // Batching not supported in sync mode
		}

		String jdbcPassword = getCredentialString(MiscUtil.getStringProperty(props, AUDIT_DB_CREDENTIAL_PROVIDER_FILE), AUDIT_DB_CREDENTIAL_PROVIDER_ALIAS);

		if(jdbcPassword != null && !jdbcPassword.isEmpty()) {
			mDbProperties.put(AUDIT_JPA_JDBC_PASSWORD, jdbcPassword);
		}

		// initialize the database related classes
		AuthzAuditEventDbObj.init(props);
	}

	@Override
	public boolean log(AuditEventBase event) {
		LOG.debug("DbAuditProvider.log()");

		boolean isSuccess = false;

		try {
			if(preCreate()) {
				DaoManager daoMgr = daoManager;

				if(daoMgr != null) {
					event.persist(daoMgr);
	
					isSuccess = postCreate(event);
				}
			}
		} catch(Exception excp) {
			logDbError("DbAuditProvider.log(): failed", excp);
		} finally {
			if(! isSuccess) {
				logFailedEvent(event);
			}
		}
		LOG.debug("<== DbAuditProvider.log()");
		return isSuccess;
	}

	@Override
	public boolean log(Collection<AuditEventBase> events) {
		boolean ret = true;
		for (AuditEventBase event : events) {
			ret = log(event);
			if(!ret) {
				break;
			}
		}
		return ret;
	}

	@Override
	public boolean logJSON(String event) {
		AuditEventBase eventObj = MiscUtil.fromJson(event,
				AuthzAuditEvent.class);
		return log(eventObj);
	}

	@Override
	public boolean logJSON(Collection<String> events) {
		boolean ret = true;
		for (String event : events) {
			ret = logJSON(event);
			if( !ret ) {
				break;
			}
		}
		return ret;
	}

	@Override
	public void start() {
		LOG.info("DbAuditProvider.start()");

		init();
	}

	@Override
	public void stop() {
		LOG.info("DbAuditProvider.stop()");

		cleanUp();
	}

	@Override
	public void flush() {
		if(mUncommitted.size() > 0) {
			boolean isSuccess = commitTransaction();

			if(! isSuccess) {
				for(AuditEventBase evt : mUncommitted) {
					logFailedEvent(evt);
				}
			}

			mUncommitted.clear();
		}
	}

	private synchronized boolean init() {
		long now = System.currentTimeMillis();

		if((now - mLastDbFailedTime) < mDbRetryMinIntervalMs) {
			return false;
		}

		LOG.info("DbAuditProvider: init()");
		LOG.info("java.library.path:"+System.getProperty("java.library.path"));
		try {
			entityManagerFactory = Persistence.createEntityManagerFactory("xa_server", mDbProperties);

	   	    daoManager = new DaoManager();
	   	    daoManager.setEntityManagerFactory(entityManagerFactory);

	   	    daoManager.getEntityManager(); // this forces the connection to be made to DB
		} catch(Exception excp) {
			logDbError("DbAuditProvider: DB initalization failed", excp);

			cleanUp();

			return false;
		}

		return true;
	}
	
	private synchronized void cleanUp() {
		LOG.info("DbAuditProvider: cleanUp()");

		try {
			if(entityManagerFactory != null && entityManagerFactory.isOpen()) {
				entityManagerFactory.close();
			}
		} catch(Exception excp) {
			LOG.error("DbAuditProvider.cleanUp(): failed", excp);
		} finally {
			entityManagerFactory = null;
			daoManager    = null;
		}
	}
	
	private boolean isDbConnected() {
		EntityManager em = getEntityManager();
		
		return em != null && em.isOpen();
	}
	
	private EntityManager getEntityManager() {
		DaoManager daoMgr = daoManager;

		if(daoMgr != null) {
			try {
				return daoMgr.getEntityManager();
			} catch(Exception excp) {
				logDbError("DbAuditProvider.getEntityManager(): failed", excp);

				cleanUp();
			}
		}

		return null;
	}
	
	private void clearEntityManager() {
		try {
			EntityManager em = getEntityManager();
			
			if(em != null) {
				em.clear();
			}
		} catch(Exception excp) {
			LOG.warn("DbAuditProvider.clearEntityManager(): failed", excp);
		}
	}
	
	private EntityTransaction getTransaction() {
		EntityManager em = getEntityManager();

		return em != null ? em.getTransaction() : null;
	}
	
	private boolean isInTransaction() {
		EntityTransaction trx = getTransaction();

		return trx != null && trx.isActive();
	}

	private boolean beginTransaction() {
		EntityTransaction trx = getTransaction();
		
		if(trx != null && !trx.isActive()) {
			trx.begin();
		}

		if(trx == null) {
			LOG.warn("DbAuditProvider.beginTransaction(): trx is null");
		}
		
		return trx != null;
	}

	private boolean commitTransaction() {
		boolean           ret = false;
		EntityTransaction trx = null;

		try {
			trx = getTransaction();

			if(trx != null && trx.isActive()) {
				trx.commit();

				ret =true;
			} else {
				throw new Exception("trx is null or not active");
			}
		} catch(Exception excp) {
			logDbError("DbAuditProvider.commitTransaction(): failed", excp);

			cleanUp(); // so that next insert will try to init()
		} finally {
			clearEntityManager();
		}

		return ret;
	}
	
	private boolean preCreate() {
		boolean ret = true;

		if(!isDbConnected()) {
			ret = init();
		}

		if(ret) {
			if(! isInTransaction()) {
				ret = beginTransaction();
			}
		}
		
		return ret;
	}
	
	private boolean postCreate(AuditEventBase event) {
		boolean ret = true;

		if(mCommitBatchSize <= 1) {
			ret = commitTransaction();
		} else {
			mUncommitted.add(event);

			if((mUncommitted.size() % mCommitBatchSize) == 0) {
				ret = commitTransaction();

				if(! ret) {
					for(AuditEventBase evt : mUncommitted) {
						if(evt != event) {
							logFailedEvent(evt);
						}
					}
				}

				mUncommitted.clear();
			}
 		}
 		return ret;
	}

	private void logDbError(String msg, Exception excp) {
		long now = System.currentTimeMillis();

		if((now - mLastDbFailedTime) > mDbRetryMinIntervalMs) {
			mLastDbFailedTime = now;
 		}

		LOG.warn(msg, excp);
	}

	private String getCredentialString(String url,String alias) {
		if(url != null && alias != null) {
			return RangerCredentialProvider.getInstance().getCredentialString(url,alias);
		}
		return null;
	}
}
