package org.apache.ranger.db;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXServiceDef;

public class XXServiceDefDao extends BaseDao<XXServiceDef> {
	/**
	 * Default Constructor
	 */
	public XXServiceDefDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXServiceDef findByName(String name) {
		if (name == null) {
			return null;
		}
		try {
			XXServiceDef xServiceDef = getEntityManager()
					.createNamedQuery("XXServiceDef.findByName", tClass)
					.setParameter("name", name).getSingleResult();
			return xServiceDef;
		} catch (NoResultException e) {
			return null;
		}
	}

}