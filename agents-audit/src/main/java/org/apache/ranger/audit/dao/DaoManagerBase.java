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
	protected final static Logger logger = Logger.getLogger(DaoManagerBase.class);

	abstract public EntityManager getEntityManager();

	private AuthzAuditEventDao mAuthzAuditDao = null;

    public DaoManagerBase() {
	}

	public AuthzAuditEventDao getAuthzAuditEventDao() {
		if(mAuthzAuditDao == null) {
			mAuthzAuditDao = new AuthzAuditEventDao(this);
		}

		return mAuthzAuditDao;
	}
}

