package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXContextEnricherDef;

public class XXContextEnricherDefDao extends BaseDao<XXContextEnricherDef> {

	public XXContextEnricherDefDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public List<XXContextEnricherDef> findByServiceDefId(Long serviceDefId) {
		if (serviceDefId == null) {
			return new ArrayList<XXContextEnricherDef>();
		}
		try {
			List<XXContextEnricherDef> retList = getEntityManager()
					.createNamedQuery("XXContextEnricherDef.findByServiceDefId", tClass)
					.setParameter("serviceDefId", serviceDefId).getResultList();
			return retList;
		} catch (NoResultException e) {
			return new ArrayList<XXContextEnricherDef>();
		}
	}

	public XXContextEnricherDef findByServiceDefIdAndName(Long serviceDefId, String name) {
		if (serviceDefId == null) {
			return null;
		}
		try {
			XXContextEnricherDef retList = getEntityManager()
					.createNamedQuery("XXContextEnricherDef.findByServiceDefIdAndName", tClass)
					.setParameter("serviceDefId", serviceDefId)
					.setParameter("name", name).getSingleResult();
			return retList;
		} catch (NoResultException e) {
			return null;
		}
	}
}
