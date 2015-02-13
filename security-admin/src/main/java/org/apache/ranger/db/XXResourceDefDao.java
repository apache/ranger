package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXResourceDef;

public class XXResourceDefDao extends BaseDao<XXResourceDef> {

	public XXResourceDefDao(RangerDaoManagerBase daoMgr) {
		super(daoMgr);
	}
	
	public XXResourceDef findByNameAndServiceDefId(String name, Long defId) {
		if(name == null || defId == null) {
			return null;
		}
		try {
			return getEntityManager().createNamedQuery(
					"XXResourceDef.findByNameAndDefId", tClass)
					.setParameter("name", name).setParameter("defId", defId)
					.getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

	public List<XXResourceDef> findByServiceDefId(Long serviceDefId) {
		if (serviceDefId == null) {
			return new ArrayList<XXResourceDef>();
		}
		try {
			List<XXResourceDef> retList = getEntityManager()
					.createNamedQuery("XXResourceDef.findByServiceDefId", tClass)
					.setParameter("serviceDefId", serviceDefId).getResultList();
			return retList;
		} catch (NoResultException e) {
			return new ArrayList<XXResourceDef>();
		}
	}
	
	public List<XXResourceDef> findByPolicyId(Long policyId) {
		if(policyId == null) {
			return new ArrayList<XXResourceDef>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXResourceDef.findByPolicyId", tClass)
					.setParameter("policyId", policyId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXResourceDef>();
		}
	}

	public XXResourceDef findByNameAndPolicyId(String name, Long policyId) {
		if(policyId == null || name == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXResourceDef.findByNameAndPolicyId", tClass)
					.setParameter("policyId", policyId)
					.setParameter("name", name).getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

}
