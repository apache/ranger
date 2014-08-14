package com.xasecure.biz;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.xasecure.common.XACommonEnums;
import com.xasecure.common.XAConstants;
import com.xasecure.common.DateUtil;
import com.xasecure.common.HTTPUtil;
import com.xasecure.common.MessageEnums;
import com.xasecure.common.RESTErrorUtil;
import com.xasecure.common.SearchCriteria;
import com.xasecure.common.StringUtil;
import com.xasecure.common.UserSessionBase;
import com.xasecure.db.XADaoManager;
import com.xasecure.entity.XXAuthSession;
import com.xasecure.entity.XXPortalUser;
import com.xasecure.entity.XXPortalUserRole;
import com.xasecure.service.AuthSessionService;
import com.xasecure.util.RestUtil;
import com.xasecure.view.VXAuthSession;
import com.xasecure.view.VXAuthSessionList;
import com.xasecure.view.VXLong;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.xasecure.security.context.XAContextHolder;
import com.xasecure.security.context.XASecurityContext;

@Component
@Transactional
public class SessionMgr {

	static final Logger logger = Logger.getLogger(SessionMgr.class);

	@Autowired
	RESTErrorUtil restErrorUtil;
	
	@Autowired
	XADaoManager daoManager;

	@Autowired
	AuthSessionService authSessionService;

	@Autowired
	HTTPUtil httpUtil;

	@Autowired
	StringUtil stringUtil;
	
	public SessionMgr() {
		logger.debug("SessionManager created");
	}

	public UserSessionBase processSuccessLogin(int authType, String userAgent) {
		return processSuccessLogin(authType, userAgent, null);
	}

	public UserSessionBase processSuccessLogin(int authType, String userAgent,
			HttpServletRequest httpRequest) {
		boolean newSessionCreation = true;
		UserSessionBase userSession = null;

		XASecurityContext context = XAContextHolder.getSecurityContext();
		if (context != null) {
			userSession = context.getUserSession();
		}

		Authentication authentication = SecurityContextHolder.getContext()
				.getAuthentication();
		WebAuthenticationDetails details = (WebAuthenticationDetails) authentication
				.getDetails();

		String currentLoginId = authentication.getName();
		if (userSession != null) {
			if (validateUserSession(userSession, currentLoginId)) {
				newSessionCreation = false;
			}
		}

		if (newSessionCreation) {
			// Need to build the UserSession
			XXPortalUser gjUser = daoManager.getXXPortalUser().findByLoginId(currentLoginId);
			if (gjUser == null) {
				logger.error(
						"Error getting user for loginId=" + currentLoginId,
						new Exception());
				return null;
			}

			XXAuthSession gjAuthSession = new XXAuthSession();
			gjAuthSession.setLoginId(currentLoginId);
			gjAuthSession.setUserId(gjUser.getId());
			gjAuthSession.setAuthTime(DateUtil.getUTCDate());
			gjAuthSession.setAuthStatus(XXAuthSession.AUTH_STATUS_SUCCESS);
			gjAuthSession.setAuthType(authType);
			if (details != null) {
				gjAuthSession.setExtSessionId(details.getSessionId());
				gjAuthSession.setRequestIP(details.getRemoteAddress());
			}

			if (userAgent != null) {
				gjAuthSession.setRequestUserAgent(userAgent);
			}
			gjAuthSession.setDeviceType(httpUtil.getDeviceType(userAgent));
			gjAuthSession = storeAuthSession(gjAuthSession);

			userSession = new UserSessionBase();
			userSession.setXXPortalUser(gjUser);
			userSession.setXXAuthSession(gjAuthSession);
			resetUserSessionForProfiles(userSession);

			if (details != null) {
				logger.info("Login Success: loginId=" + currentLoginId
						+ ", sessionId=" + gjAuthSession.getId()
						+ ", sessionId=" + details.getSessionId()
						+ ", requestId=" + details.getRemoteAddress());
			} else {
				logger.info("Login Success: loginId=" + currentLoginId
						+ ", sessionId=" + gjAuthSession.getId()
						+ ", details is null");
			}

		}

		return userSession;
	}

	public void resetUserSessionForProfiles(UserSessionBase userSession) {
		if (userSession == null) {
			// Nothing to reset
			return;
		}

		// Let's get the Current User Again
		String currentLoginId = userSession.getLoginId();

		XXPortalUser gjUser = daoManager.getXXPortalUser().findByLoginId(currentLoginId);
		userSession.setXXPortalUser(gjUser);

		setUserRoles(userSession);

	}

	private void setUserRoles(UserSessionBase userSession) {

		List<String> strRoleList = new ArrayList<String>();
		List<XXPortalUserRole> roleList = daoManager.getXXPortalUserRole().findByUserId(
				userSession.getUserId());
		for (XXPortalUserRole gjUserRole : roleList) {
			String userRole = gjUserRole.getUserRole();

			strRoleList.add(userRole);
			if (userRole.equals(XAConstants.ROLE_SYS_ADMIN)) {
				userSession.setUserAdmin(true);
			}
		}
		userSession.setUserRoleList(strRoleList);
	}

	public XXAuthSession processFailureLogin(int authStatus, int authType,
			String loginId, String remoteAddr, String sessionId) {
		XXAuthSession gjAuthSession = new XXAuthSession();
		gjAuthSession.setLoginId(loginId);
		gjAuthSession.setUserId(null);
		gjAuthSession.setAuthTime(DateUtil.getUTCDate());
		gjAuthSession.setAuthStatus(authStatus);
		gjAuthSession.setAuthType(authType);
		gjAuthSession.setDeviceType(XACommonEnums.DEVICE_UNKNOWN);
		gjAuthSession.setExtSessionId(sessionId);
		gjAuthSession.setRequestIP(remoteAddr);
		gjAuthSession.setRequestUserAgent(null);

		gjAuthSession = storeAuthSession(gjAuthSession);
		return gjAuthSession;
	}

	protected boolean validateUserSession(UserSessionBase userSession,
			String currentLoginId) {
		if (currentLoginId
				.equalsIgnoreCase(userSession.getXXPortalUser().getLoginId())) {
			return true;
		} else {
			logger.info(
					"loginId doesn't match loginId from HTTPSession. Will create new session. loginId="
							+ currentLoginId + ", userSession=" + userSession,
					new Exception());
			return false;
		}
	}

	@Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
	protected XXAuthSession storeAuthSession(XXAuthSession gjAuthSession) {
		// daoManager.getEntityManager().getTransaction().begin();
		XXAuthSession dbMAuthSession = daoManager.getXXAuthSession().create(
				gjAuthSession);
		// daoManager.getEntityManager().getTransaction().commit();
		return dbMAuthSession;
	}

	// non-WEB processing
	public UserSessionBase processStandaloneSuccessLogin(int authType,
			String ipAddress) {
		Authentication authentication = SecurityContextHolder.getContext()
				.getAuthentication();

		String currentLoginId = authentication.getName();

		// Need to build the UserSession
		XXPortalUser gjUser = daoManager.getXXPortalUser().findByLoginId(currentLoginId);
		if (gjUser == null) {
			logger.error("Error getting user for loginId=" + currentLoginId,
					new Exception());
			return null;
		}

		XXAuthSession gjAuthSession = new XXAuthSession();
		gjAuthSession.setLoginId(currentLoginId);
		gjAuthSession.setUserId(gjUser.getId());
		gjAuthSession.setAuthTime(DateUtil.getUTCDate());
		gjAuthSession.setAuthStatus(XXAuthSession.AUTH_STATUS_SUCCESS);
		gjAuthSession.setAuthType(authType);
		gjAuthSession.setDeviceType(XACommonEnums.DEVICE_UNKNOWN);
		gjAuthSession.setExtSessionId(null);
		gjAuthSession.setRequestIP(ipAddress);
		gjAuthSession.setRequestUserAgent(null);

		gjAuthSession = storeAuthSession(gjAuthSession);

		UserSessionBase userSession = new UserSessionBase();
		userSession.setXXPortalUser(gjUser);
		userSession.setXXAuthSession(gjAuthSession);

		// create context with user-session and set in thread-local
		XASecurityContext context = new XASecurityContext();
		context.setUserSession(userSession);
		XAContextHolder.setSecurityContext(context);

		resetUserSessionForProfiles(userSession);

		return userSession;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXAuthSessionList searchAuthSessions(SearchCriteria searchCriteria) {

		if (searchCriteria != null && searchCriteria.getParamList() != null
				&& searchCriteria.getParamList().size() > 0) {	
			
			int clientTimeOffsetInMinute=RestUtil.getClientTimeOffset();
			java.util.Date temp = null;
			DateUtil dateUtil = new DateUtil();
			if (searchCriteria.getParamList().containsKey("startDate")) {
				temp = (java.util.Date) searchCriteria.getParamList().get(
						"startDate");
				temp = dateUtil.getDateFromGivenDate(temp, 0, 0, 0, 0);
				temp = dateUtil.addTimeOffset(temp, clientTimeOffsetInMinute);
				searchCriteria.getParamList().put("startDate", temp);
			}
			if (searchCriteria.getParamList().containsKey("endDate")) {
				temp = (java.util.Date) searchCriteria.getParamList().get(
						"endDate");
				temp = dateUtil.getDateFromGivenDate(temp, 0, 23, 59, 59);
				temp = dateUtil.addTimeOffset(temp, clientTimeOffsetInMinute);
				searchCriteria.getParamList().put("endDate", temp);
			}
		}
		
		return authSessionService.search(searchCriteria);
	}

	public VXLong countAuthSessions(SearchCriteria searchCriteria) {
		return authSessionService.getSearchCount(searchCriteria,
				AuthSessionService.AUTH_SESSION_SEARCH_FLDS);
	}

	public VXAuthSession getAuthSession(Long id) {
		return authSessionService.readResource(id);
	}

	public VXAuthSession getAuthSessionBySessionId(String authSessionId) {
		if(stringUtil.isEmpty(authSessionId)){
			throw restErrorUtil.createRESTException("Please provide the auth session id.", 
					MessageEnums.INVALID_INPUT_DATA);
		}
		
		XXAuthSession xXAuthSession = daoManager.getXXAuthSession()
				.getAuthSessionBySessionId(authSessionId);
		
		if(xXAuthSession==null){
			throw restErrorUtil.createRESTException("Please provide a valid "
					+ "session id.", MessageEnums.INVALID_INPUT_DATA);
		}
		
		VXAuthSession vXAuthSession = authSessionService.populateViewBean(xXAuthSession);
		return vXAuthSession;
	}
	
	public boolean isValidXAUser(String loginId) {
		XXPortalUser pUser = daoManager.getXXPortalUser().findByLoginId(loginId);
		if (pUser == null) {
			logger.error("Error getting user for loginId=" + loginId);
			return false;
		} else {
			logger.info(loginId+" is a valid user");
			return true;
		}
		
	}

}
