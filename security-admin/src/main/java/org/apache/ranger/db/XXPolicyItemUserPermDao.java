package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyItemUserPerm;

public class XXPolicyItemUserPermDao extends BaseDao<XXPolicyItemUserPerm> {

	public XXPolicyItemUserPermDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public List<XXPolicyItemUserPerm> findByPolicyItemId(Long polItemId) {
		if(polItemId == null) {
			return new ArrayList<XXPolicyItemUserPerm>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyItemUserPerm.findByPolicyItemId", tClass)
					.setParameter("polItemId", polItemId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemUserPerm>();
		}
	}

}
