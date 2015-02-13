package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyResourceMap;

public class XXPolicyResourceMapDao extends BaseDao<XXPolicyResourceMap> {

	public XXPolicyResourceMapDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}
	
	public List<XXPolicyResourceMap> findByPolicyResId(Long polResId) {
		if(polResId == null) {
			return new ArrayList<XXPolicyResourceMap>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyResourceMap.findByPolicyResId", tClass)
					.setParameter("polResId", polResId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyResourceMap>();
		}
	}

}
