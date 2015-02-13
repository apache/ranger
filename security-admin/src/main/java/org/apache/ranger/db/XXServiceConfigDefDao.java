package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXServiceConfigDef;

public class XXServiceConfigDefDao extends BaseDao<XXServiceConfigDef> {
	
	public XXServiceConfigDefDao(RangerDaoManagerBase daoMgr) {
		super(daoMgr);
	}

	public List<XXServiceConfigDef> findByServiceDefId(Long serviceDefId) {
		if (serviceDefId == null) {
			return new ArrayList<XXServiceConfigDef>();
		}
		try {
			List<XXServiceConfigDef> retList = getEntityManager()
					.createNamedQuery("XXServiceConfigDef.findByServiceDefId", tClass)
					.setParameter("serviceDefId", serviceDefId).getResultList();
			return retList;
		} catch (NoResultException e) {
			return new ArrayList<XXServiceConfigDef>();
		}
	}
	
	public List<XXServiceConfigDef> findByServiceDefName(String serviceDef) {
		if (serviceDef == null) {
			return new ArrayList<XXServiceConfigDef>();
		}
		try {
			List<XXServiceConfigDef> retList = getEntityManager()
					.createNamedQuery("XXServiceConfigDef.findByServiceDefName", tClass)
					.setParameter("serviceDef", serviceDef).getResultList();
			return retList;
		} catch (NoResultException e) {
			return new ArrayList<XXServiceConfigDef>();
		}
	}
	
}
