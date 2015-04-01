package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;
import javax.persistence.Query;

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
	
	public XXDataHist findObjByEventTimeClassTypeAndId(String eventTime, int classType, Long objId) {
		if (eventTime == null || objId == null) {
			return null;
		}
		try {
			String queryStr = "select obj.* from x_data_hist obj where obj.obj_class_type = "+classType
					+ " and obj.obj_id = "+objId + " and obj.create_time <= '" + eventTime + "' ORDER BY obj.id DESC";
			Query jpaQuery = getEntityManager().createNativeQuery(queryStr, tClass).setMaxResults(1);
			
			return (XXDataHist) jpaQuery.getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public List<Integer> getVersionListOfObject(Long objId, int classType) {
		if (objId == null) {
			return new ArrayList<Integer>();
		}
		try {
			return getEntityManager().createNamedQuery("XXDataHist.getVersionListOfObject")
					.setParameter("objId", objId).setParameter("classType", classType).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<Integer>();
		}
	}

	public XXDataHist findObjectByVersionNumber(Long objId, int classType, int versionNo) {
		if (objId == null) {
			return null;
		}
		try {
			return getEntityManager().createNamedQuery("XXDataHist.findObjectByVersionNumber", tClass)
					.setParameter("objId", objId).setParameter("classType", classType)
					.setParameter("version", versionNo).getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

}
