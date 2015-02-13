package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyItem;

/**
 */

public class XXPolicyItemDao extends BaseDao<XXPolicyItem> {
	/**
	 * Default Constructor
	 */
	public XXPolicyItemDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public List<XXPolicyItem> findByPolicyId(Long policyId) {
		if (policyId == null) {
			return new ArrayList<XXPolicyItem>();
		}
		try {
			List<XXPolicyItem> returnList = getEntityManager()
					.createNamedQuery("XXPolicyItem.findByPolicyId", tClass)
					.setParameter("policyId", policyId).getResultList();
			if (returnList == null) {
				return new ArrayList<XXPolicyItem>();
			}
			return returnList;
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItem>();
		}
	}

}