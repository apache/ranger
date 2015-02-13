package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyItemAccess;

public class XXPolicyItemAccessDao extends BaseDao<XXPolicyItemAccess> {

	public XXPolicyItemAccessDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}
	
	public List<XXPolicyItemAccess> findByPolicyItemId(Long polItemId) {
		if(polItemId == null) {
			return new ArrayList<XXPolicyItemAccess>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyItemAccess.findByPolicyItemId", tClass)
					.setParameter("polItemId", polItemId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemAccess>();
		}
	}

}
