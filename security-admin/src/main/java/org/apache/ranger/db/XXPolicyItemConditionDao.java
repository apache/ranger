package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyItemCondition;

public class XXPolicyItemConditionDao extends BaseDao<XXPolicyItemCondition> {

	public XXPolicyItemConditionDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}
	
	public List<XXPolicyItemCondition> findByPolicyItemId(Long polItemId) {
		if(polItemId == null) {
			return new ArrayList<XXPolicyItemCondition>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyItemCondition.findByPolicyItemId", tClass)
					.setParameter("polItemId", polItemId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemCondition>();
		}
	}

	public List<XXPolicyItemCondition> findByPolicyItemAndDefId(Long polItemId,
			Long polCondDefId) {
		if(polItemId == null || polCondDefId == null) {
			return new ArrayList<XXPolicyItemCondition>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyItemCondition.findByPolicyItemAndDefId", tClass)
					.setParameter("polItemId", polItemId)
					.setParameter("polCondDefId", polCondDefId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemCondition>();
		}
	}

}
