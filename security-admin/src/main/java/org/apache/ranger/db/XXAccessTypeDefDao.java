package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXAccessTypeDef;

public class XXAccessTypeDefDao extends BaseDao<XXAccessTypeDef> {

	public XXAccessTypeDefDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}
	
	public List<XXAccessTypeDef> findByServiceDefId(Long serviceDefId) {
		if (serviceDefId == null) {
			return new ArrayList<XXAccessTypeDef>();
		}
		try {
			List<XXAccessTypeDef> retList = getEntityManager()
					.createNamedQuery("XXAccessTypeDef.findByServiceDefId", tClass)
					.setParameter("serviceDefId", serviceDefId).getResultList();
			return retList;
		} catch (NoResultException e) {
			return new ArrayList<XXAccessTypeDef>();
		}
	}

	public XXAccessTypeDef findByNameAndServiceId(String name, Long serviceId) {
		if(name == null || serviceId == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXAccessTypeDef.findByNameAndServiceId", tClass)
					.setParameter("name", name).setParameter("serviceId", serviceId)
					.getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

}
