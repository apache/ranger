package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXEnumDef;
import org.apache.ranger.entity.XXPolicyConditionDef;

public class XXEnumDefDao extends BaseDao<XXEnumDef> {

	public XXEnumDefDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public List<XXEnumDef> findByServiceDefId(Long serviceDefId) {
		if (serviceDefId == null) {
			return new ArrayList<XXEnumDef>();
		}
		try {
			List<XXEnumDef> retList = getEntityManager()
					.createNamedQuery("XXEnumDef.findByServiceDefId", tClass)
					.setParameter("serviceDefId", serviceDefId).getResultList();
			return retList;
		} catch (NoResultException e) {
			return new ArrayList<XXEnumDef>();
		}
	}
	
}
