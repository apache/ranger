package org.apache.ranger.db;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXService;

/**
 */

public class XXServiceDao extends BaseDao<XXService> {
	/**
	 * Default Constructor
	 */
	public XXServiceDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXService findByName(String name) {
		if (name == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXService.findByName", tClass)
					.setParameter("name", name).getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

	public Long getMaxIdOfXXService() {
		try {
			return (Long) getEntityManager().createNamedQuery("XXService.getMaxIdOfXXService").getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

}