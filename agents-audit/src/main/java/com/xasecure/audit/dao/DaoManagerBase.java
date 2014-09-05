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

package com.xasecure.audit.dao;

import javax.persistence.EntityManager;

import org.apache.log4j.Logger;


public abstract class DaoManagerBase {
	final static Logger logger = Logger.getLogger(DaoManagerBase.class);

	abstract public EntityManager getEntityManager();

	private XAHBaseAuditEventDao mHBaseDao = null;
	private XAHdfsAuditEventDao  mHdfsDao  = null;
	private XAHiveAuditEventDao  mHiveDao  = null;
	private XAKnoxAuditEventDao  mKnoxDao  = null;
	private XAStormAuditEventDao mStormDao = null;

    public DaoManagerBase() {
	}

	public XAHBaseAuditEventDao getXAHBaseAuditEventDao() {
		if(mHBaseDao == null) {
			mHBaseDao = new XAHBaseAuditEventDao(this);
		}

		return mHBaseDao;
	}

	public XAHdfsAuditEventDao getXAHdfsAuditEventDao() {
		if(mHdfsDao == null) {
			mHdfsDao = new XAHdfsAuditEventDao(this);
		}

		return mHdfsDao;
	}

	public XAHiveAuditEventDao getXAHiveAuditEventDao() {
		if(mHiveDao == null) {
			mHiveDao = new XAHiveAuditEventDao(this);
		}

		return mHiveDao;
	}

	public XAKnoxAuditEventDao getXAKnoxAuditEventDao() {
		if(mKnoxDao == null) {
			mKnoxDao = new XAKnoxAuditEventDao(this);
		}

		return mKnoxDao;
	}

	public XAStormAuditEventDao getXAStormAuditEventDao() {
		if(mStormDao == null) {
			mStormDao = new XAStormAuditEventDao(this);
		}

		return mStormDao;
	}
}

