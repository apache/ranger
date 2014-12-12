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

package org.apache.ranger.audit.dao;

import javax.persistence.EntityManager;

import org.apache.log4j.Logger;


public abstract class DaoManagerBase {
	final static Logger logger = Logger.getLogger(DaoManagerBase.class);

	abstract public EntityManager getEntityManager();

	private RangerHBaseAuditEventDao mHBaseDao = null;
	private RangerHdfsAuditEventDao  mHdfsDao  = null;
	private RangerHiveAuditEventDao  mHiveDao  = null;
	private RangerKnoxAuditEventDao  mKnoxDao  = null;
	private RangerStormAuditEventDao mStormDao = null;

    public DaoManagerBase() {
	}

	public RangerHBaseAuditEventDao getXAHBaseAuditEventDao() {
		if(mHBaseDao == null) {
			mHBaseDao = new RangerHBaseAuditEventDao(this);
		}

		return mHBaseDao;
	}

	public RangerHdfsAuditEventDao getXAHdfsAuditEventDao() {
		if(mHdfsDao == null) {
			mHdfsDao = new RangerHdfsAuditEventDao(this);
		}

		return mHdfsDao;
	}

	public RangerHiveAuditEventDao getXAHiveAuditEventDao() {
		if(mHiveDao == null) {
			mHiveDao = new RangerHiveAuditEventDao(this);
		}

		return mHiveDao;
	}

	public RangerKnoxAuditEventDao getXAKnoxAuditEventDao() {
		if(mKnoxDao == null) {
			mKnoxDao = new RangerKnoxAuditEventDao(this);
		}

		return mKnoxDao;
	}

	public RangerStormAuditEventDao getXAStormAuditEventDao() {
		if(mStormDao == null) {
			mStormDao = new RangerStormAuditEventDao(this);
		}

		return mStormDao;
	}
}

