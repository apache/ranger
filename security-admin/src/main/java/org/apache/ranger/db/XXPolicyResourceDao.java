package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyResource;

public class XXPolicyResourceDao extends BaseDao<XXPolicyResource> {

	public XXPolicyResourceDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}
	
	public XXPolicyResource findByResDefIdAndPolicyId(Long resDefId, Long polId) {
		if(resDefId == null || polId == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyResource.findByResDefIdAndPolicyId", tClass)
					.setParameter("resDefId", resDefId).setParameter("polId", polId)
					.getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

	public List<XXPolicyResource> findByPolicyId(Long policyId) {
		if(policyId == null) {
			return new ArrayList<XXPolicyResource>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyResource.findByPolicyId", tClass)
					.setParameter("policyId", policyId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyResource>();
		}
	}

}
