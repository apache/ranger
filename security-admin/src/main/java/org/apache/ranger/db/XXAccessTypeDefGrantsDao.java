package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXAccessTypeDefGrants;

public class XXAccessTypeDefGrantsDao extends BaseDao<XXAccessTypeDefGrants> {

	public XXAccessTypeDefGrantsDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	@SuppressWarnings("unchecked")
	public List<String> findImpliedGrantsByATDId(Long atdId) {
		if(atdId == null) {
			return new ArrayList<String>();
		}
		try {
			List<String> returnList = getEntityManager()
					.createNamedQuery("XXAccessTypeDefGrants.findImpliedGrantsByATDId")
					.setParameter("atdId", atdId).getResultList();
			
			return returnList;
		} catch (NoResultException e) {
			return new ArrayList<String>();
		}
	}

}
