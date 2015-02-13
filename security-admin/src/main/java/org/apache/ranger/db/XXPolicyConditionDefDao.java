package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyConditionDef;

public class XXPolicyConditionDefDao extends BaseDao<XXPolicyConditionDef> {

	public XXPolicyConditionDefDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public List<XXPolicyConditionDef> findByServiceDefId(Long serviceDefId) {
		if (serviceDefId == null) {
			return new ArrayList<XXPolicyConditionDef>();
		}
		try {
			List<XXPolicyConditionDef> retList = getEntityManager()
					.createNamedQuery("XXPolicyConditionDef.findByServiceDefId", tClass)
					.setParameter("serviceDefId", serviceDefId).getResultList();
			return retList;
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyConditionDef>();
		}
	}

	public List<XXPolicyConditionDef> findByPolicyItemId(Long polItemId) {
		if(polItemId == null) {
			return new ArrayList<XXPolicyConditionDef>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyConditionDef.findByPolicyItemId", tClass)
					.setParameter("polItemId", polItemId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyConditionDef>();
		}
	}
	
	public XXPolicyConditionDef findByPolicyItemIdAndName(Long polItemId, String name) {
		if(polItemId == null || name == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyConditionDef.findByPolicyItemIdAndName", tClass)
					.setParameter("polItemId", polItemId)
					.setParameter("name", name).getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

	
}
