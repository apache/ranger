/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.biz;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.HTTPUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.RangerSuperUserConfig;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXAuthSession;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXPortalUserRole;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.apache.ranger.security.listener.RangerHttpSessionListener;
import org.apache.ranger.security.web.filter.RangerSecurityContextFormationFilter;
import org.apache.ranger.service.AuthSessionService;
import org.apache.ranger.util.RestUtil;
import org.apache.ranger.view.VXAuthSession;
import org.apache.ranger.view.VXAuthSessionList;
import org.apache.ranger.view.VXLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

@Component
@Transactional
public class SessionMgr {
    static final Logger logger = LoggerFactory.getLogger(SessionMgr.class);

    public static final String PROP_SESSION_LIMIT_CONCURRENCY      = "ranger.session.limit.concurrency";
    public static final String SESSION_ATTR_CONCURRENT_EXPIRED     = "RANGER_CONCURRENT_SESSION_EXPIRED";
    public static final String SESSION_ATTR_CONCURRENT_EXPIRED_SSO = "RANGER_CONCURRENT_SESSION_EXPIRED_SSO";
    public static final String SESSION_ATTR_DOWNLOAD_ONLY          = "RANGER_SESSION_DOWNLOAD_ONLY";
    public static final String SESSION_ATTR_NON_UI                 = "RANGER_SESSION_NON_UI";
    private static final String DEFAULT_BROWSER_USER_AGENTS        = "Mozilla,Opera,Chrome";
    private static final ConcurrentHashMap<String, Object> CONCURRENT_SESSION_LOCKS = new ConcurrentHashMap<>();

    private static final Long SESSION_UPDATE_INTERVAL_IN_MILLIS = 30 * DateUtils.MILLIS_PER_MINUTE;

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    RangerDaoManager daoManager;

    @Autowired
    RangerBizUtil bizUtil;

    @Autowired
    XUserMgr xUserMgr;

    @Autowired
    AuthSessionService authSessionService;

    @Autowired
    HTTPUtil httpUtil;

    @Autowired
    StringUtil stringUtil;

    public SessionMgr() {
        logger.debug("SessionManager created");
    }

    public UserSessionBase processSuccessLogin(int authType, String userAgent, HttpServletRequest httpRequest) {
        boolean               newSessionCreation = true;
        UserSessionBase       userSession        = null;
        RangerSecurityContext context            = RangerContextHolder.getSecurityContext();

        if (context != null) {
            userSession = context.getUserSession();
        }

        Authentication           authentication = SecurityContextHolder.getContext().getAuthentication();
        WebAuthenticationDetails details        = (WebAuthenticationDetails) authentication.getDetails();
        String                   currentLoginId = authentication.getName();

        if (userSession != null) {
            if (validateUserSession(userSession, currentLoginId)) {
                newSessionCreation = false;
            }
        }

        if (newSessionCreation) {
            getSSOSpnegoAuthCheckForAPI(currentLoginId, httpRequest);

            // Need to build the UserSession
            XXPortalUser gjUser = daoManager.getXXPortalUser().findByLoginId(currentLoginId);

            if (gjUser == null) {
                logger.error("Error getting user for loginId={}", currentLoginId, new Exception());

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

            HttpSession session = httpRequest.getSession();

            if (session != null) {
                if (session.getAttribute("auditLoginId") == null) {
                    synchronized (session) {
                        if (session.getAttribute("auditLoginId") == null) {
                            boolean isDownloadLogEnabled = PropertiesUtil.getBooleanProperty("ranger.downloadpolicy.session.log.enabled", false);

                            if (isDownloadLogEnabled) {
                                gjAuthSession = storeAuthSession(gjAuthSession);

                                session.setAttribute("auditLoginId", gjAuthSession.getId());
                            } else if (!StringUtils.isEmpty(httpRequest.getRequestURI()) && !isPluginOrSecureDownloadRequest(httpRequest.getRequestURI())) {
                                gjAuthSession = storeAuthSession(gjAuthSession);

                                session.setAttribute("auditLoginId", gjAuthSession.getId());
                            } else if (StringUtils.isEmpty(httpRequest.getRequestURI())) {
                                gjAuthSession = storeAuthSession(gjAuthSession);

                                session.setAttribute("auditLoginId", gjAuthSession.getId());
                            } else { //NOPMD
                                //do not log the details for download policy and tag
                            }
                        }
                    }
                }
            }

            userSession = new UserSessionBase();

            userSession.setXXPortalUser(gjUser);
            userSession.setXXAuthSession(gjAuthSession);

            if (httpRequest.getAttribute("spnegoEnabled") != null && (boolean) httpRequest.getAttribute("spnegoEnabled")) {
                userSession.setSpnegoEnabled(true);
            }

            boolean ssoEnabled;

            if (authType == XXAuthSession.AUTH_TYPE_TRUSTED_PROXY) {
                ssoEnabled = true;
            } else {
                Object ssoEnabledObj = httpRequest.getAttribute("ssoEnabled");

                ssoEnabled = ssoEnabledObj != null ? Boolean.parseBoolean(String.valueOf(ssoEnabledObj)) : PropertiesUtil.getBooleanProperty("ranger.sso.enabled", false);
            }

            logger.debug("session id = {} ssoenabled = {}", userSession.getLoginId(), ssoEnabled);

            userSession.setSSOEnabled(ssoEnabled);

            resetUserSessionForProfiles(userSession);
            resetUserModulePermission(userSession);

            if (logger.isDebugEnabled()) {
                Calendar cal = Calendar.getInstance();

                if (details != null) {
                    logger.debug("Login Success: loginId={}, sessionId={}, sessionId={}, requestId={}, epoch={}", currentLoginId, gjAuthSession.getId(), details.getSessionId(), details.getRemoteAddress(), cal.getTimeInMillis());
                } else {
                    logger.debug("Login Success: loginId={}, sessionId={}, details is null, epoch={}", currentLoginId, gjAuthSession.getId(), cal.getTimeInMillis());
                }
            }

            if (session != null) {
                if (isPluginOrSecureDownloadRequest(httpRequest.getRequestURI())) {
                    try {
                        session.setAttribute(SESSION_ATTR_DOWNLOAD_ONLY, Boolean.TRUE);
                    } catch (IllegalStateException e) {
                        logger.debug("Could not mark download-only session", e);
                    }
                } else if (!isBrowserUserAgent(resolveUserAgent(userAgent, httpRequest))) {
                    try {
                        session.setAttribute(SESSION_ATTR_NON_UI, Boolean.TRUE);
                    } catch (IllegalStateException e) {
                        logger.debug("Could not mark non-UI session", e);
                    }
                } else {
                    enforceConcurrentSessionLimit(currentLoginId, session);
                }
            }
        }

        return userSession;
    }

    public void resetUserModulePermission(UserSessionBase userSession) {
        XXUser xUser = daoManager.getXXUser().findByUserName(userSession.getLoginId());

        if (xUser != null) {
            List<String> permissionList;

            if (userSession.isUserAdmin()) {
                permissionList = daoManager.getXXModuleDef().getAllModuleNames();
            } else {
                permissionList = daoManager.getXXModuleDef().findAccessibleModulesByUserId(userSession.getUserId(), xUser.getId());
            }

            CopyOnWriteArraySet<String>          userPermissions      = new CopyOnWriteArraySet<>(permissionList);
            UserSessionBase.RangerUserPermission rangerUserPermission = userSession.getRangerUserPermission();

            if (rangerUserPermission == null) {
                rangerUserPermission = new UserSessionBase.RangerUserPermission();
            }

            rangerUserPermission.setUserPermissions(userPermissions);
            rangerUserPermission.setLastUpdatedTime(Calendar.getInstance().getTimeInMillis());
            userSession.setRangerUserPermission(rangerUserPermission);

            logger.debug("UserSession Updated to set new Permissions to User: {}", userSession.getLoginId());
        } else {
            logger.error("No XUser found with username: {}So Permission is not set for the user", userSession.getLoginId());
        }
    }

    public void resetUserSessionForProfiles(UserSessionBase userSession) {
        if (userSession == null) {
            // Nothing to reset
            return;
        }

        // Let's get the Current User Again
        String       currentLoginId = userSession.getLoginId();
        XXPortalUser gjUser         = daoManager.getXXPortalUser().findByLoginId(currentLoginId);

        userSession.setXXPortalUser(gjUser);

        setUserRoles(userSession);
    }

    public XXAuthSession processFailureLogin(int authStatus, int authType, String loginId, String remoteAddr, String sessionId, String userAgent) {
        XXAuthSession gjAuthSession = new XXAuthSession();

        gjAuthSession.setLoginId(loginId);
        gjAuthSession.setUserId(null);
        gjAuthSession.setAuthTime(DateUtil.getUTCDate());
        gjAuthSession.setAuthStatus(authStatus);
        gjAuthSession.setAuthType(authType);
        gjAuthSession.setDeviceType(RangerCommonEnums.DEVICE_UNKNOWN);
        gjAuthSession.setExtSessionId(sessionId);
        gjAuthSession.setRequestIP(remoteAddr);
        gjAuthSession.setRequestUserAgent(userAgent);

        gjAuthSession = storeAuthSession(gjAuthSession);

        return gjAuthSession;
    }

    // non-WEB processing
    public UserSessionBase processStandaloneSuccessLogin(int authType, String ipAddress) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String         currentLoginId = authentication.getName();
        XXPortalUser   gjUser         = daoManager.getXXPortalUser().findByLoginId(currentLoginId); // Need to build the UserSession

        if (gjUser == null) {
            logger.error("Error getting user for loginId={}", currentLoginId, new Exception());

            return null;
        }

        XXAuthSession gjAuthSession = new XXAuthSession();

        gjAuthSession.setLoginId(currentLoginId);
        gjAuthSession.setUserId(gjUser.getId());
        gjAuthSession.setAuthTime(DateUtil.getUTCDate());
        gjAuthSession.setAuthStatus(XXAuthSession.AUTH_STATUS_SUCCESS);
        gjAuthSession.setAuthType(authType);
        gjAuthSession.setDeviceType(RangerCommonEnums.DEVICE_UNKNOWN);
        gjAuthSession.setExtSessionId(null);
        gjAuthSession.setRequestIP(ipAddress);
        gjAuthSession.setRequestUserAgent(null);

        gjAuthSession = storeAuthSession(gjAuthSession);

        UserSessionBase userSession = new UserSessionBase();

        userSession.setXXPortalUser(gjUser);
        userSession.setXXAuthSession(gjAuthSession);

        // create context with user-session and set in thread-local
        RangerSecurityContext context = new RangerSecurityContext();

        context.setUserSession(userSession);

        RangerContextHolder.setSecurityContext(context);

        resetUserSessionForProfiles(userSession);
        resetUserModulePermission(userSession);

        return userSession;
    }

    /**
     * @param searchCriteria
     * @return
     */
    public VXAuthSessionList searchAuthSessions(SearchCriteria searchCriteria) {
        if (searchCriteria == null) {
            searchCriteria = new SearchCriteria();
        }

        if (searchCriteria.getParamList() != null && !searchCriteria.getParamList().isEmpty()) {
            int      clientTimeOffsetInMinute = RestUtil.getClientTimeOffset();
            DateUtil dateUtil                 = new DateUtil();

            if (searchCriteria.getParamList().containsKey("startDate")) {
                Date temp = (Date) searchCriteria.getParamList().get("startDate");

                temp = dateUtil.getDateFromGivenDate(temp, 0, 0, 0, 0);
                temp = dateUtil.addTimeOffset(temp, clientTimeOffsetInMinute);

                searchCriteria.getParamList().put("startDate", temp);
            }

            if (searchCriteria.getParamList().containsKey("endDate")) {
                Date temp = (Date) searchCriteria.getParamList().get("endDate");

                temp = dateUtil.getDateFromGivenDate(temp, 0, 23, 59, 59);
                temp = dateUtil.addTimeOffset(temp, clientTimeOffsetInMinute);

                searchCriteria.getParamList().put("endDate", temp);
            }
        }

        return authSessionService.search(searchCriteria);
    }

    public VXLong countAuthSessions(SearchCriteria searchCriteria) {
        return authSessionService.getSearchCount(searchCriteria, AuthSessionService.AUTH_SESSION_SEARCH_FLDS);
    }

    public VXAuthSession getAuthSession(Long id) {
        return authSessionService.readResource(id);
    }

    public VXAuthSession getAuthSessionBySessionId(String authSessionId) {
        if (stringUtil.isEmpty(authSessionId)) {
            throw restErrorUtil.createRESTException("Please provide the auth session id.", MessageEnums.INVALID_INPUT_DATA);
        }

        XXAuthSession xXAuthSession = daoManager.getXXAuthSession().getAuthSessionBySessionId(authSessionId);

        if (xXAuthSession == null) {
            throw restErrorUtil.createRESTException("Please provide a valid " + "session id.", MessageEnums.INVALID_INPUT_DATA);
        }

        return authSessionService.populateViewBean(xXAuthSession);
    }

    /**
     * Check whether the user failed to log in so many times that we need to lock it for
     * a while. The current limit of is to fail at most n times in a sliding time window,
     * otherwise the login verification will not be performed in the future.
     *
     * @param loginId
     * @return
     */
    public boolean isLoginIdLocked(String loginId) {
        boolean ret             = false;
        boolean autoLockEnabled = PropertiesUtil.getBooleanProperty("ranger.admin.login.autolock.enabled", true);

        if (autoLockEnabled) {
            int  windowSeconds    = PropertiesUtil.getIntProperty("ranger.admin.login.autolock.window.seconds", 300);
            int  maxFailuresCount = PropertiesUtil.getIntProperty("ranger.admin.login.autolock.maxfailure", 5);
            long failuresCount    = daoManager.getXXAuthSession().getRecentAuthFailureCountByLoginId(loginId, windowSeconds);

            ret = failuresCount >= maxFailuresCount;

            logger.debug("isLoginIdLocked(loginId={}): windowSeconds={}, maxFailuresCount={}, failuresCount={}, ret={}", loginId, windowSeconds, maxFailuresCount, failuresCount, ret);
        }

        return ret;
    }

    public boolean isValidXAUser(String loginId) {
        XXPortalUser pUser = daoManager.getXXPortalUser().findByLoginId(loginId);

        if (pUser == null || pUser.getUserSource() == RangerCommonEnums.USER_FEDERATED) {
            logger.error("Error getting user for loginId={} or  federated user", loginId);

            return false;
        } else {
            logger.debug("{} is a valid user", loginId);

            return true;
        }
    }

    public CopyOnWriteArrayList<UserSessionBase> getActiveSessionsOnServer() {
        CopyOnWriteArrayList<HttpSession>     activeHttpUserSessions   = RangerHttpSessionListener.getActiveSessionOnServer();
        CopyOnWriteArrayList<UserSessionBase> activeRangerUserSessions = new CopyOnWriteArrayList<>();

        if (CollectionUtils.isEmpty(activeHttpUserSessions)) {
            return activeRangerUserSessions;
        }

        for (HttpSession httpSession : activeHttpUserSessions) {
            if (httpSession.getAttribute(RangerSecurityContextFormationFilter.AKA_SC_SESSION_KEY) == null) {
                continue;
            }

            RangerSecurityContext securityContext = (RangerSecurityContext) httpSession.getAttribute(RangerSecurityContextFormationFilter.AKA_SC_SESSION_KEY);

            if (securityContext.getUserSession() != null) {
                activeRangerUserSessions.add(securityContext.getUserSession());
            }
        }

        return activeRangerUserSessions;
    }

    public Set<UserSessionBase> getActiveUserSessionsForPortalUserId(Long portalUserId) {
        CopyOnWriteArrayList<UserSessionBase> activeSessions = getActiveSessionsOnServer();

        if (CollectionUtils.isEmpty(activeSessions)) {
            return null;
        }

        Set<UserSessionBase> activeUserSessions = new HashSet<>();

        for (UserSessionBase session : activeSessions) {
            if (session.getUserId().equals(portalUserId)) {
                activeUserSessions.add(session);
            }
        }

        logger.debug("No Session Found with portalUserId: {}", portalUserId);

        return activeUserSessions;
    }

    public Set<UserSessionBase> getActiveUserSessionsForXUserId(Long xUserId) {
        XXPortalUser portalUser = daoManager.getXXPortalUser().findByXUserId(xUserId);

        if (portalUser != null) {
            return getActiveUserSessionsForPortalUserId(portalUser.getId());
        } else {
            logger.debug("Could not find corresponding portalUser for xUserId{}", xUserId);

            return null;
        }
    }

    public synchronized void refreshPermissionsIfNeeded(UserSessionBase userSession) {
        if (userSession != null) {
            Long lastUpdatedTime = (userSession.getRangerUserPermission() != null) ? userSession.getRangerUserPermission().getLastUpdatedTime() : null;

            if (lastUpdatedTime == null || (Calendar.getInstance().getTimeInMillis() - lastUpdatedTime) > SESSION_UPDATE_INTERVAL_IN_MILLIS) {
                this.resetUserModulePermission(userSession);
            }
        }
    }

    public Date getLastSuccessLoginAuthTimeByUserId(String loginId) {
        XXAuthSession xXAuthSession = daoManager.getXXAuthSession().getLastSuccessLoginAuthSessionByUserId(loginId);

        if (xXAuthSession != null) {
            return authSessionService.populateViewBean(xXAuthSession).getAuthTime();
        } else {
            logger.info("Session cleaned up or  User logged in for first time");
        }

        return null;
    }

    public static boolean isConcurrentSessionExpired(HttpSession session) {
        if (session == null) {
            return false;
        }

        try {
            return Boolean.TRUE.equals(session.getAttribute(SESSION_ATTR_CONCURRENT_EXPIRED));
        } catch (IllegalStateException e) {
            return false;
        }
    }

    public static boolean isConcurrentSessionExpiredSso(HttpSession session) {
        if (session == null) {
            return false;
        }

        try {
            return Boolean.TRUE.equals(session.getAttribute(SESSION_ATTR_CONCURRENT_EXPIRED_SSO));
        } catch (IllegalStateException e) {
            return false;
        }
    }

    /**
     * When {@code ranger.session.limit.concurrency} is exceeded, expire the oldest UI sessions
     * so the new login succeeds. SSO sessions are marked expired for Knox logout redirect.
     * The count is taken from this JVM's in-memory session list, not cluster-wide.
     * Find-and-expire is serialized per loginId so two concurrent UI logins for the same user
     * cannot both observe a count under the limit.
     */
    protected void enforceConcurrentSessionLimit(String loginId, HttpSession currentSession) {
        int limit = PropertiesUtil.getIntProperty(PROP_SESSION_LIMIT_CONCURRENCY, 0);

        if (limit <= 0 || StringUtils.isBlank(loginId) || currentSession == null) {
            return;
        }

        Object lock = CONCURRENT_SESSION_LOCKS.computeIfAbsent(loginId.toLowerCase(Locale.ROOT), id -> new Object());

        synchronized (lock) {
            List<HttpSession> otherSessions = findActiveUiSessionsForUser(loginId, currentSession);

            if (otherSessions.size() < limit) {
                return;
            }

            otherSessions.sort(Comparator.comparingLong(session -> {
                try {
                    return session.getCreationTime();
                } catch (IllegalStateException e) {
                    return 0L;
                }
            }));

            int toExpire = otherSessions.size() - limit + 1;

            logger.info("Concurrent session limit {} exceeded for user {}; expiring {} older session(s)", limit, loginId, toExpire);

            for (int i = 0; i < toExpire; i++) {
                expireConcurrentSession(otherSessions.get(i));
            }
        }
    }

    /**
     * Plugin and secure download URLs. Used both to skip x_auth_sess rows (unless
     * {@code ranger.downloadpolicy.session.log.enabled} is true) and to exclude
     * those sessions from the UI concurrent-session quota.
     */
    static boolean isPluginOrSecureDownloadRequest(String uri) {
        if (StringUtils.isEmpty(uri)) {
            return false;
        }

        return uri.contains("/secure/policies/download/")
                || uri.contains("/secure/download/")
                || uri.contains("/plugins/policies/download/")
                || uri.contains("/tags/download/")
                || uri.contains("/roles/download/")
                || uri.contains("/xusers/download/")
                || uri.contains("/gds/download/");
    }

    static boolean isBrowserUserAgent(String userAgent) {
        if (StringUtils.isBlank(userAgent)) {
            return false;
        }

        String agents = PropertiesUtil.getProperty("ranger.krb.browser-useragents-regex", DEFAULT_BROWSER_USER_AGENTS);

        if (StringUtils.isBlank(agents)) {
            agents = DEFAULT_BROWSER_USER_AGENTS;
        }

        String userAgentLower = userAgent.toLowerCase(Locale.ROOT);

        for (String agentPrefix : agents.split(",")) {
            if (StringUtils.isNotBlank(agentPrefix) && userAgentLower.startsWith(agentPrefix.trim().toLowerCase(Locale.ROOT))) {
                return true;
            }
        }

        return false;
    }

    private static String resolveUserAgent(String userAgent, HttpServletRequest httpRequest) {
        if (StringUtils.isNotBlank(userAgent)) {
            return userAgent;
        }

        return httpRequest != null ? httpRequest.getHeader(HTTPUtil.USER_AGENT) : null;
    }

    private List<HttpSession> findActiveUiSessionsForUser(String loginId, HttpSession currentSession) {
        CopyOnWriteArrayList<HttpSession> activeHttpSessions = RangerHttpSessionListener.getActiveSessionOnServer();
        List<HttpSession>                 matching           = new ArrayList<>();

        if (CollectionUtils.isEmpty(activeHttpSessions)) {
            return matching;
        }

        for (HttpSession httpSession : activeHttpSessions) {
            if (httpSession == null || httpSession == currentSession) {
                continue;
            }

            try {
                if (Boolean.TRUE.equals(httpSession.getAttribute(SESSION_ATTR_CONCURRENT_EXPIRED))) {
                    continue;
                }

                if (Boolean.TRUE.equals(httpSession.getAttribute(SESSION_ATTR_DOWNLOAD_ONLY))
                        || Boolean.TRUE.equals(httpSession.getAttribute(SESSION_ATTR_NON_UI))) {
                    continue;
                }

                if (httpSession.getAttribute(RangerSecurityContextFormationFilter.AKA_SC_SESSION_KEY) == null) {
                    continue;
                }

                RangerSecurityContext securityContext = (RangerSecurityContext) httpSession.getAttribute(RangerSecurityContextFormationFilter.AKA_SC_SESSION_KEY);
                UserSessionBase       userSession     = securityContext != null ? securityContext.getUserSession() : null;

                if (userSession != null && loginId.equalsIgnoreCase(userSession.getLoginId())) {
                    matching.add(httpSession);
                }
            } catch (IllegalStateException e) {
                logger.debug("Skipping invalidated session while counting concurrent sessions", e);
            }
        }

        return matching;
    }

    private void expireConcurrentSession(HttpSession httpSession) {
        try {
            RangerSecurityContext context     = (RangerSecurityContext) httpSession.getAttribute(RangerSecurityContextFormationFilter.AKA_SC_SESSION_KEY);
            UserSessionBase       userSession = context != null ? context.getUserSession() : null;
            boolean               ssoOrProxy  = userSession != null
                    && (Boolean.TRUE.equals(userSession.isSSOEnabled()) || Boolean.TRUE.equals(userSession.isSpnegoEnabled()));

            httpSession.setAttribute(SESSION_ATTR_CONCURRENT_EXPIRED, Boolean.TRUE);
            httpSession.setAttribute(SESSION_ATTR_CONCURRENT_EXPIRED_SSO, ssoOrProxy);

            if (context != null) {
                context.setUserSession(null);
            }

            logger.info("Expired concurrent Ranger Admin session (ssoOrTrustedProxy={})", ssoOrProxy);

            if (!ssoOrProxy) {
                httpSession.invalidate();
            }
        } catch (IllegalStateException e) {
            logger.debug("Session already invalidated while enforcing concurrent session limit", e);
        }
    }

    protected boolean validateUserSession(UserSessionBase userSession, String currentLoginId) {
        if (currentLoginId.equalsIgnoreCase(userSession.getXXPortalUser().getLoginId())) {
            return true;
        } else {
            logger.warn("loginId doesn't match loginId from HTTPSession. Will create new session. loginId={}, userSession={}", currentLoginId, userSession, new Exception());

            return false;
        }
    }

    @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
    protected XXAuthSession storeAuthSession(XXAuthSession gjAuthSession) {
        /*
        Recording an x_auth_sess row for every liveness/readiness probe is not required
         */
        if (gjAuthSession != null && bizUtil.isHealthCheckUser(gjAuthSession.getLoginId())) {
            return gjAuthSession;
        }

        XXAuthSession dbMAuthSession = daoManager.getXXAuthSession().create(gjAuthSession);

        return dbMAuthSession;
    }

    private void getSSOSpnegoAuthCheckForAPI(String currentLoginId, HttpServletRequest request) {
        RangerSecurityContext context    = RangerContextHolder.getSecurityContext();
        UserSessionBase       session    = context != null ? context.getUserSession() : null;
        boolean               ssoEnabled = session != null ? session.isSSOEnabled() : PropertiesUtil.getBooleanProperty("ranger.sso.enabled", false);
        XXPortalUser          gjUser     = daoManager.getXXPortalUser().findByLoginId(currentLoginId);

        if (gjUser == null && ((request.getAttribute("spnegoEnabled") != null && (boolean) request.getAttribute("spnegoEnabled")) || (ssoEnabled) || bizUtil.isHealthCheckUser(currentLoginId))) {
            logger.debug("User : {} doesn't exist in Ranger DB So creating user as it's SSO or Spnego authenticated", currentLoginId);

            if (bizUtil.isHealthCheckUser(currentLoginId)) {
                xUserMgr.createServiceConfigUserSynchronously(currentLoginId);
            } else {
                xUserMgr.createServiceConfigUser(currentLoginId);
            }
        }
    }

    private void setUserRoles(UserSessionBase userSession) {
        List<String>           strRoleList = new ArrayList<>();
        List<XXPortalUserRole> roleList    = daoManager.getXXPortalUserRole().findByUserId(userSession.getUserId());

        for (XXPortalUserRole gjUserRole : roleList) {
            String userRole = gjUserRole.getUserRole();

            strRoleList.add(userRole);
        }

        if (strRoleList.contains(RangerConstants.ROLE_SYS_ADMIN)) {
            userSession.setUserAdmin(true);
            userSession.setKeyAdmin(false);
            userSession.setAuditUserAdmin(false);
            userSession.setAuditKeyAdmin(false);
        } else if (strRoleList.contains(RangerConstants.ROLE_KEY_ADMIN)) {
            userSession.setKeyAdmin(true);
            userSession.setUserAdmin(false);
            userSession.setAuditUserAdmin(false);
            userSession.setAuditKeyAdmin(false);
        } else if (strRoleList.size() == 1 && RangerConstants.ROLE_USER.equals(strRoleList.get(0))) {
            userSession.setKeyAdmin(false);
            userSession.setUserAdmin(false);
            userSession.setAuditUserAdmin(false);
            userSession.setAuditKeyAdmin(false);
        } else if (strRoleList.contains(RangerConstants.ROLE_ADMIN_AUDITOR)) {
            userSession.setAuditUserAdmin(true);
            userSession.setAuditKeyAdmin(false);
            userSession.setKeyAdmin(false);
            userSession.setUserAdmin(false);
        } else if (strRoleList.contains(RangerConstants.ROLE_KEY_ADMIN_AUDITOR)) {
            userSession.setAuditKeyAdmin(true);
            userSession.setAuditUserAdmin(false);
            userSession.setKeyAdmin(false);
            userSession.setUserAdmin(false);
        }

        applyConfigSuperUserSessionFlags(userSession);

        if (userSession.isSuperUser()) {
            strRoleList = RangerSuperUserConfig.mergeConfigSuperUserRoles(strRoleList, true);
        }

        userSession.setUserRoleList(strRoleList);
    }

    /**
     * Applies config super-user session flag ({@code superUser}) when login matches
     * {@code ranger.admin.super.users} / super.groups.
     */
    private void applyConfigSuperUserSessionFlags(final UserSessionBase userSession) {
        if (userSession == null) {
            return;
        }

        String        loginId = userSession.getLoginId();
        final boolean isSuperUser;

        if (StringUtils.isBlank(loginId)) {
            isSuperUser = false;
        } else if (!RangerSuperUserConfig.isEnabled()) {
            isSuperUser = false;
        } else if (RangerSuperUserConfig.isSuperUser(loginId)) {
            isSuperUser = true;
        } else if (RangerSuperUserConfig.isSuperGroupsConfigured() && xUserMgr != null) {
            isSuperUser = RangerSuperUserConfig.isSuperUser(loginId, xUserMgr.getGroupsForUser(loginId));
        } else {
            isSuperUser = false;
        }

        if (isSuperUser) {
            userSession.setSuperUser(true);

            logger.info("Granted full admin privileges via config for user {}", loginId);
        } else {
            userSession.setSuperUser(false);
        }
    }
}
