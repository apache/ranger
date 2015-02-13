package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXServiceConfigMap;

public class XXServiceConfigMapDao extends BaseDao<XXServiceConfigMap> {

	public XXServiceConfigMapDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public List<XXServiceConfigMap> findByServiceId(Long serviceId) {
		if (serviceId == null) {
			return new ArrayList<XXServiceConfigMap>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXServiceConfigMap.findByServiceId", tClass)
					.setParameter("serviceId", serviceId)
					.getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXServiceConfigMap>();
		}
	}

	public XXServiceConfigMap findByServiceAndConfigKey(Long serviceId,
			String configKey) {
		if(serviceId == null || configKey == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXServiceConfigMap.findByServiceAndConfigKey", tClass)
					.setParameter("serviceId", serviceId)
					.setParameter("configKey", configKey).getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

}
