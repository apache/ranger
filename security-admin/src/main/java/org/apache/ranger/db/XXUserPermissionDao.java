package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.log4j.Logger;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXGroupUser;
import org.apache.ranger.entity.XXUserPermission;

public class XXUserPermissionDao extends BaseDao<XXUserPermission>{

	static final Logger logger = Logger.getLogger(XXUserPermissionDao.class);

	public XXUserPermissionDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public List<XXUserPermission> findByModuleId(Long moduleId,boolean isUpdate) {
		if (moduleId != null) {
			try {

				if(isUpdate)
				{
					return getEntityManager()
							.createNamedQuery("XXUserPermissionUpdates.findByModuleId", XXUserPermission.class)
							.setParameter("moduleId", moduleId)
							.getResultList();
				}
				return getEntityManager()
						.createNamedQuery("XXUserPermission.findByModuleId", XXUserPermission.class)
						.setParameter("moduleId", moduleId)
						.setParameter("isAllowed",RangerCommonEnums.IS_ALLOWED)
						.getResultList();
			} catch (NoResultException e) {
				logger.debug(e.getMessage());
			}
		} else {
			logger.debug("ResourceUserId not provided.");
			return new ArrayList<XXUserPermission>();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public List<XXUserPermission> findByUserPermissionIdAndIsAllowed(Long userId) {
		if (userId != null) {
			try {
				return getEntityManager()
						.createNamedQuery("XXUserPermission.findByUserPermissionIdAndIsAllowed")
						.setParameter("userId", userId)
						.setParameter("isAllowed",RangerCommonEnums.IS_ALLOWED)
						.getResultList();
			} catch (NoResultException e) {
				logger.debug(e.getMessage());
			}
		} else {
			logger.debug("ResourceUserId not provided.");
			return new ArrayList<XXUserPermission>();
		}
		return null;
	}


	public List<XXUserPermission> findByUserPermissionId(Long userId) {
		if (userId != null) {
			try {
				return getEntityManager()
						.createNamedQuery("XXUserPermission.findByUserPermissionId", XXUserPermission.class)
						.setParameter("userId", userId)
						.getResultList();
			} catch (NoResultException e) {
				logger.debug(e.getMessage());
			}
		} else {
			logger.debug("ResourceUserId not provided.");
			return new ArrayList<XXUserPermission>();
		}
		return null;
	}

	public List<XXUserPermission> findByModuleIdAndUserId(Long userId,Long moduleId) {
		if (userId != null) {
			try {
				return getEntityManager()
						.createNamedQuery("XXUserPermission.findByModuleIdAndUserId", XXUserPermission.class)
						.setParameter("userId", userId)
						.setParameter("moduleId", moduleId)
						.getResultList();
			} catch (NoResultException e) {
				logger.debug(e.getMessage());
			}
		} else {
			logger.debug("ResourceUserId not provided.");
			return new ArrayList<XXUserPermission>();
		}
		return null;
	}
}
