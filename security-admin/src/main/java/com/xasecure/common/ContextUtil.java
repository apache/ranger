package com.xasecure.common;

import com.xasecure.security.context.XAContextHolder;
import com.xasecure.security.context.XASecurityContext;

public class ContextUtil {

	/**
	 * Singleton class
	 */
	private ContextUtil() {
	}

	public static Long getCurrentUserId() {
		XASecurityContext context = XAContextHolder.getSecurityContext();
		if (context != null) {
			UserSessionBase userSession = context.getUserSession();
			if (userSession != null) {
				return userSession.getUserId();
			}
		}
		return null;
	}

	public static String getCurrentUserPublicName() {
		XASecurityContext context = XAContextHolder.getSecurityContext();
		if (context != null) {
			UserSessionBase userSession = context.getUserSession();
			if (userSession != null) {
				return userSession.getXXPortalUser().getPublicScreenName();
				// return userSession.getGjUser().getPublicScreenName();
			}
		}
		return null;
	}

	public static UserSessionBase getCurrentUserSession() {
		UserSessionBase userSession = null;
		XASecurityContext context = XAContextHolder.getSecurityContext();
		if (context != null) {
			userSession = context.getUserSession();
		}
		return userSession;
	}

	public static RequestContext getCurrentRequestContext() {
		XASecurityContext context = XAContextHolder.getSecurityContext();
		if (context != null) {
			return context.getRequestContext();
		}
		return null;
	}

	public static String getCurrentUserLoginId() {
		XASecurityContext context = XAContextHolder.getSecurityContext();
		if (context != null) {
			UserSessionBase userSession = context.getUserSession();
			if (userSession != null) {
				return userSession.getLoginId();
			}
		}
		return null;
	}

}
