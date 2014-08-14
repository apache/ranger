package com.xasecure.common.db;

/*
 * Copyright (c) 2014 XASecure.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure.
 */


import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.Query;
import javax.persistence.TypedQuery;

import org.apache.log4j.Logger;

import com.xasecure.common.ContextUtil;
import com.xasecure.db.XADaoManager;
import com.xasecure.db.XADaoManagerBase;
import com.xasecure.entity.XXDBBase;

public abstract class BaseDao<T> {
	static final Logger logger = Logger.getLogger(BaseDao.class);

	protected XADaoManager daoManager;

	EntityManager em;

	protected Class<T> tClass;

	public BaseDao(XADaoManagerBase daoManager) {
		this.daoManager = (XADaoManager) daoManager;
		this.init(daoManager.getEntityManager());
	}

	public BaseDao(XADaoManagerBase daoManager, String persistenceContextUnit) {
		this.daoManager = (XADaoManager) daoManager;

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

}
