package com.xasecure.security.handler;

/**
 *
 */

import java.io.Serializable;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.xasecure.biz.XABizUtil;
import com.xasecure.common.ContextUtil;
import com.xasecure.common.PropertiesUtil;
import com.xasecure.common.UserSessionBase;
import com.xasecure.db.XADaoManager;
import com.xasecure.entity.XXDBBase;

@Component
public class XADomainObjectSecurityHandler {

	public static Logger logger = Logger
			.getLogger(XADomainObjectSecurityHandler.class);

	@Autowired
	public XADaoManager daoManager;

	@Autowired
	XABizUtil msBizUtil;

	boolean checkParentObject = false;

	public XADomainObjectSecurityHandler() {
		checkParentObject = PropertiesUtil.getBooleanProperty(
				"xa.db.access.filter.check.parentobject", checkParentObject);
	}

	/**
	 * @return the daoManager
	 */
	public XADaoManager getDaoManager() {
		return daoManager;
	}

	public <T extends XXDBBase> boolean hasAccess(T targetDomainObject,
			Permission.permissionType permission) {
		//TODO: Need to review this method and reimplement it properly
		return true;
	}

	public boolean hasAccess(String targetType, Serializable targetId,
			Permission.permissionType permission) {
		try {
			Class<?> clazz = Class.forName(targetType);
			Class<? extends XXDBBase> gjClazz = clazz.asSubclass(XXDBBase.class);
			return hasAccess(gjClazz, targetId, permission);

		} catch (ClassNotFoundException cfe) {
			logger.error("class not found:" + targetType, cfe);
		} catch (Exception e) {
			logger.error("Excepion targetType:" + targetType + " targetId:"
					+ targetId, e);
		}

		return false;
	}

	public boolean hasAccess(Class<? extends XXDBBase> targetClass,
			Serializable targetId, Permission.permissionType permission) {
		try {
			Class<? extends XXDBBase> gjClazz = targetClass
					.asSubclass(XXDBBase.class);
			XXDBBase base = getDaoManager().getEntityManager().find(gjClazz,
					targetId);
			return hasAccess(base, permission);

		} catch (Exception e) {
			logger.error("Excepion targetType:" + targetClass + " targetId:"
					+ targetId, e);
		}

		return false;
	}

	public boolean hasModeratorPermission() {
		UserSessionBase sess = ContextUtil.getCurrentUserSession();
		if (sess != null && sess.isUserAdmin()) {
			return true;
		}
		return false;
	}

}
