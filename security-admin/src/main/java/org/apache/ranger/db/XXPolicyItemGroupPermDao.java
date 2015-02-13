package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyItemGroupPerm;

public class XXPolicyItemGroupPermDao extends BaseDao<XXPolicyItemGroupPerm> {

	public XXPolicyItemGroupPermDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public List<XXPolicyItemGroupPerm> findByPolicyItemId(Long polItemId) {
		if(polItemId == null) {
			return new ArrayList<XXPolicyItemGroupPerm>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyItemGroupPerm.findByPolicyItemId", tClass)
					.setParameter("polItemId", polItemId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemGroupPerm>();
		}
	}

}
