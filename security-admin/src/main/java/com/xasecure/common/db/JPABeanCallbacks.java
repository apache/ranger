package com.xasecure.common.db;

import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;

import com.xasecure.common.DateUtil;
import com.xasecure.common.UserSessionBase;
import com.xasecure.entity.XXDBBase;

import org.apache.log4j.Logger;

import com.xasecure.security.context.XAContextHolder;
import com.xasecure.security.context.XASecurityContext;

public class JPABeanCallbacks {
	static final Logger logger = Logger.getLogger(JPABeanCallbacks.class);

	@PrePersist
	void onPrePersist(Object o) {
		try {
			if (o != null && o instanceof XXDBBase) {
				XXDBBase entity = (XXDBBase) o;

				entity.setUpdateTime(DateUtil.getUTCDate());

				XASecurityContext context = XAContextHolder
						.getSecurityContext();
				if (context != null) {
					UserSessionBase userSession = context.getUserSession();
					if (userSession != null) {
						entity.setAddedByUserId(userSession.getUserId());
						entity.setUpdatedByUserId(userSession
								.getUserId());
					}
				} else {
					if (logger.isDebugEnabled()) {
						logger.debug(
								"Security context not found for this request. obj="
										+ o, new Throwable());
					}
				}
			}
		} catch (Throwable t) {
			logger.error(t);
		}

	}

	// @PostPersist
	// void onPostPersist(Object o) {
	// if (o != null && o instanceof MBase) {
	// MBase entity = (MBase) o;
	// if (logger.isDebugEnabled()) {
	// logger.debug("DBChange.create:class=" + o.getClass().getName()
	// + entity.getId());
	// }
	//
	// }
	// }

	// @PostLoad void onPostLoad(Object o) {}

	@PreUpdate
	void onPreUpdate(Object o) {
		try {
			if (o != null && o instanceof XXDBBase) {
				XXDBBase entity = (XXDBBase) o;
				entity.setUpdateTime(DateUtil.getUTCDate());
			}
		} catch (Throwable t) {
			logger.error(t);
		}

	}

	// @PostUpdate
	// void onPostUpdate(Object o) {
	// }

	// @PreRemove void onPreRemove(Object o) {}

	// @PostRemove
	// void onPostRemove(Object o) {
	// }

}