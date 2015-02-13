package org.apache.ranger.db;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXDataHist;

public class XXDataHistDao extends BaseDao<XXDataHist> {

	public XXDataHistDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXDataHist findLatestByObjectClassTypeAndObjectId(Integer classType, Long objectId) {
		if(classType == null || objectId == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXDataHist.findLatestByObjectClassTypeAndObjectId", tClass)
					.setParameter("classType", classType)
					.setParameter("objectId", objectId)
					.setMaxResults(1).getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

}
