package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXEnumElementDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;

public class XXEnumElementDefDao extends BaseDao<XXEnumElementDef> {

	public XXEnumElementDefDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public List<XXEnumElementDef> findByEnumDefId(Long enumDefId) {
		if(enumDefId == null) {
			return new ArrayList<XXEnumElementDef>();
		}
		try {
			List<XXEnumElementDef> returnList = getEntityManager()
					.createNamedQuery("XXEnumElementDef.findByEnumDefId", tClass)
					.setParameter("enumDefId", enumDefId).getResultList();
			return returnList;
		} catch (NoResultException e) {
			return new ArrayList<XXEnumElementDef>();
		}
	}

}
