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

package com.xasecure.audit.provider;

import java.util.ArrayList;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.audit.dao.DaoManager;
import com.xasecure.audit.dao.XAHBaseAuditEventDao;
import com.xasecure.audit.dao.XAHdfsAuditEventDao;
import com.xasecure.audit.dao.XAHiveAuditEventDao;
import com.xasecure.audit.dao.XAKnoxAuditEventDao;
import com.xasecure.audit.entity.XXHBaseAuditEvent;
import com.xasecure.audit.entity.XXHdfsAuditEvent;
import com.xasecure.audit.entity.XXHiveAuditEvent;
import com.xasecure.audit.entity.XXKnoxAuditEvent;
import com.xasecure.audit.model.AuditEventBase;
import com.xasecure.audit.model.HBaseAuditEvent;
import com.xasecure.audit.model.HdfsAuditEvent;
import com.xasecure.audit.model.HiveAuditEvent;
import com.xasecure.audit.model.KnoxAuditEvent;


/*
 * NOTE:
 * - Instances of this class are not thread-safe.
 */
public class DbAuditProvider implements AuditProvider {

	private static final Log LOG = LogFactory.getLog(DbAuditProvider.class);

	private EntityManagerFactory entityManagerFactory;
	private DaoManager          daoManager;
	private XAHBaseAuditEventDao hbaseDao;
	private XAHdfsAuditEventDao hdfsDao;
	private XAHiveAuditEventDao hiveDao;
	private XAKnoxAuditEventDao knoxDao;
	
	private int                 mCommitBatchSize  = 1;
	private long                mLastCommitTime   = 0;
	private ArrayList<AuditEventBase> mUncommitted = new ArrayList<AuditEventBase>();
	private Map<String, String> mDbProperties;

	public DbAuditProvider(Map<String, String> properties, int dbBatchSize) {
		LOG.info("DbAuditProvider: creating..");
		
		mDbProperties    = properties;
		mCommitBatchSize = dbBatchSize < 1 ? 1 : dbBatchSize;
	}

	@Override
	public void log(HBaseAuditEvent event) {
		LOG.debug("DbAuditProvider.log(HBaseAuditEvent)");

		if(preCreate(event)) {
			hbaseDao.create(new XXHBaseAuditEvent(event));
			postCreate(event);
		}
	}

	@Override
	public void log(HdfsAuditEvent event) {
		LOG.debug("DbAuditProvider.log(HdfsAuditEvent)");

		if(preCreate(event)) {
			hdfsDao.create(new XXHdfsAuditEvent(event));
			postCreate(event);
		}
	}

	@Override
	public void log(HiveAuditEvent event) {
		LOG.debug("DbAuditProvider.log(HiveAuditEvent)");
		
		if(preCreate(event)) {
			hiveDao.create(new XXHiveAuditEvent(event));
			postCreate(event);
		}
	}
	
	@Override
	public void log(KnoxAuditEvent event) {
		LOG.debug("DbAuditProvider.log(KnoxAuditEvent)");
		
		if(preCreate(event)) {
			knoxDao.create(new XXKnoxAuditEvent(event));
			postCreate(event);
		}
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
    public void waitToComplete() {
		LOG.info("DbAuditProvider.waitToComplete()");
	}

	@Override
	public boolean isFlushPending() {
		return mUncommitted.size() > 0;
	}
	
	@Override
	public long getLastFlushTime() {
		return mLastCommitTime;
	}

	@Override
	public void flush() {
		if(mUncommitted.size() > 0) {
			commitTransaction();
		}
	}

	private boolean init() {
		LOG.info("DbAuditProvider: init()");

		try {
			entityManagerFactory = Persistence.createEntityManagerFactory("xa_server", mDbProperties);
		} catch(Exception excp) {
			LOG.error("DbAuditProvider: DB initalization failed", excp);

			entityManagerFactory = null;

			return false;
		}

   	    daoManager = new DaoManager();
   	    daoManager.setEntityManagerFactory(entityManagerFactory);

		hbaseDao = daoManager.getXAHBaseAuditEvent();
		hdfsDao = daoManager.getXAHdfsAuditEvent();
		hiveDao = daoManager.getXAHiveAuditEvent();
		knoxDao = daoManager.getXAKnoxAuditEvent();

		return true;
	}
	
	private void cleanUp() {
		LOG.info("DbAuditProvider: cleanUp()");

		try {
			clearEntityManager();

			if(entityManagerFactory != null && entityManagerFactory.isOpen()) {
				entityManagerFactory.close();
			}
		} finally {
			entityManagerFactory = null;
			daoManager    = null;
			hbaseDao      = null;
			hdfsDao       = null;
			hiveDao       = null;
		}
	}
	
	private boolean isDbConnected() {
		EntityManager em = getEntityManager();
		
		return em != null && em.isOpen();
	}
	
	private EntityManager getEntityManager() {
		return daoManager != null ? daoManager.getEntityManager() : null;
	}
	
	private void clearEntityManager() {
		EntityManager em = getEntityManager();
		
		if(em == null) {
			LOG.info("clearEntityManager(): em is null");
		} else {
			em.clear();
		}
	}
	
	private EntityTransaction getTransaction() {
		EntityManager em = getEntityManager();

		if(em == null)
			LOG.info("getTransaction(): em is null");

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
			LOG.error("beginTransaction(): trx is null");
		}
		
		return trx != null;
	}

	private void commitTransaction() {
		EntityTransaction trx = getTransaction();

		try {
			if(trx != null && trx.isActive()) {
				trx.commit();
			} else {
				if(trx == null) {
					LOG.error("commitTransaction(): trx is null. Clearing " + mUncommitted.size() + " uncommitted logs");
				}
				else {
					LOG.error("commitTransaction(): trx is not active. Clearing " + mUncommitted.size() + " uncommitted logs");
				}

				cleanUp(); // so that next insert will try to init()
			}
		} catch(Exception excp) {
			LOG.error("commitTransaction(): error while committing " + mUncommitted.size() + " log(s)", excp);
			for(AuditEventBase event : mUncommitted) {
				LOG.error("failed to log event { " + event.toString() + " }");
			}

			cleanUp(); // so that next insert will try to init()
		} finally {
			mLastCommitTime = System.currentTimeMillis();
			mUncommitted.clear();

			clearEntityManager();
		}
	}
	
	private boolean preCreate(AuditEventBase event) {
		boolean ret = true;

		if(!isDbConnected()) {
			LOG.error("DbAuditProvider: not connected to DB. Retrying..");

			ret = init();
		}

		if(ret) {
			if(! isInTransaction()) {
				ret = beginTransaction();
			}
		}
		
		if(!ret) {
			LOG.error("failed to log event { " + event.toString() + " }");
		}
		
		return ret;
	}
	
	private void postCreate(AuditEventBase event) {
		mUncommitted.add(event);

		if((mCommitBatchSize == 1) || ((mUncommitted.size() % mCommitBatchSize) == 0)) {
			flush();
		}
	}
}
