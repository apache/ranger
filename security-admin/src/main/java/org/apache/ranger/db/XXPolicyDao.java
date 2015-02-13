package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicy;

/**
 */

public class XXPolicyDao extends BaseDao<XXPolicy> {
	/**
	 * Default Constructor
	 */
	public XXPolicyDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXPolicy findByName(String polName) {
		if (polName == null) {
			return null;
		}
		try {
			XXPolicy xPol = getEntityManager()
					.createNamedQuery("XXPolicy.findByName", tClass)
					.setParameter("polName", polName).getSingleResult();
			return xPol;
		} catch (NoResultException e) {
			return null;
		}
	}

	public List<XXPolicy> findByServiceId(Long serviceId) {
		if (serviceId == null) {
			return new ArrayList<XXPolicy>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicy.findByServiceId", tClass)
					.setParameter("serviceId", serviceId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicy>();
		}
	}

}