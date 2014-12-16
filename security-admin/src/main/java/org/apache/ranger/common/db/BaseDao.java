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

 package org.apache.ranger.common.db;



import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.Query;
import javax.persistence.TypedQuery;

import org.apache.log4j.Logger;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.RangerDaoManagerBase;

public abstract class BaseDao<T> {
	static final Logger logger = Logger.getLogger(BaseDao.class);

	protected RangerDaoManager daoManager;

	EntityManager em;

	protected Class<T> tClass;

	public BaseDao(RangerDaoManagerBase daoManager) {
		this.daoManager = (RangerDaoManager) daoManager;
		this.init(daoManager.getEntityManager());
	}

	public BaseDao(RangerDaoManagerBase daoManager, String persistenceContextUnit) {
		this.daoManager = (RangerDaoManager) daoManager;

		EntityManager em = this.daoManager.getEntityManager(persistenceContextUnit);

		this.init(em);
	}

	@SuppressWarnings("unchecked")
	private void init(EntityManager em) {
		this.em = em;

		ParameterizedType genericSuperclass = (ParameterizedType) getClass()
				.getGenericSuperclass();

		Type type = genericSuperclass.getActualTypeArguments()[0];

		if (type instanceof ParameterizedType) {
			this.tClass = (Class<T>) ((ParameterizedType) type).getRawType();
		} else {
			this.tClass = (Class<T>) type;
		}
	}

	public EntityManager getEntityManager() {
		return this.em;
	}

	public T create(T obj) {
		T ret = null;

		em.persist(obj);
		em.flush();

		ret = obj;
		return ret;
	}

	public T update(T obj) {
		em.merge(obj);
		em.flush();
		return obj;
	}

	public boolean remove(Long id) {
		return remove(getById(id));
	}

	public boolean remove(T obj) {
		if (obj == null) {
			return true;
		}

		em.remove(obj);
		em.flush();

		return true;
	}

	public T getById(Long id) {
		if (id == null) {
			return null;
		}
		T ret = null;
		try {
			ret = em.find(tClass, id);
		} catch (NoResultException e) {
			return null;
		}
		return ret;
	}

	public List<T> findByNamedQuery(String namedQuery, String paramName,
			Object refId) {
		List<T> ret = new ArrayList<T>();

		if (namedQuery == null) {
			return ret;
		}
		try {
			TypedQuery<T> qry = em.createNamedQuery(namedQuery, tClass);
			qry.setParameter(paramName, refId);
			ret = qry.getResultList();
		} catch (NoResultException e) {
			// ignore
		}
		return ret;
	}

	public List<T> findByParentId(Long parentId) {
		String namedQuery = tClass.getSimpleName() + ".findByParentId";
		return findByNamedQuery(namedQuery, "parentId", parentId);
	}

	
	public List<T> executeQueryInSecurityContext(Class<T> clazz, Query query) {
		return executeQueryInSecurityContext(clazz, query, true);
	}

	@SuppressWarnings("unchecked")
	public List<T> executeQueryInSecurityContext(Class<T> clazz, Query query,
			boolean userPrefFilter) {
		// boolean filterEnabled = false;
		List<T> rtrnList = null;
		try {
			// filterEnabled = enableVisiblityFilters(clazz, userPrefFilter);

			rtrnList = query.getResultList();
		} finally {
			// if (filterEnabled) {
			// disableVisiblityFilters(clazz);
			// }

		}

		return rtrnList;
	}

	/**
	 * @param clazz
	 * @param query
	 * @param b
	 * @return
	 */
	private Long executeCountQueryInSecurityContext(Class<T> clazz,
			Query query, boolean userPrefFilter) {
		// boolean filterEnabled = false;
		Long rtrnObj = null;
		try {
			// filterEnabled = enableVisiblityFilters(clazz, userPrefFilter);
			rtrnObj = (Long) query.getSingleResult();
		} finally {
			// if (filterEnabled) {
			// disableVisiblityFilters(clazz);
			// }
		}

		return rtrnObj;
	}

	public Long executeCountQueryInSecurityContext(Class<T> clazz, Query query) {
		return executeCountQueryInSecurityContext(clazz, query, true);
	}
	
	public List<T> getAll() {
		List<T> ret = null;
		TypedQuery<T> qry = em.createQuery(
				"SELECT t FROM " + tClass.getSimpleName() + " t", tClass);
		ret = qry.getResultList();
		return ret;
	}

	public Long getAllCount() {
		Long ret = null;
		TypedQuery<Long> qry = em.createQuery(
				"SELECT count(t) FROM " + tClass.getSimpleName() + " t",
				Long.class);
		ret = qry.getSingleResult();
		return ret;
	}

}
