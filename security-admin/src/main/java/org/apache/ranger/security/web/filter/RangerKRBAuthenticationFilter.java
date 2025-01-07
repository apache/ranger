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

package org.apache.ranger.security.web.filter;

import org.apache.commons.collections.iterators.IteratorEnumeration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.apache.ranger.biz.UserMgr;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.util.RestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetails;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.FilterRegistration;
import javax.servlet.FilterRegistration.Dynamic;
import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.SessionCookieConfig;
import javax.servlet.SessionTrackingMode;
import javax.servlet.descriptor.JspConfigDescriptor;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.EventListener;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerKRBAuthenticationFilter extends RangerKrbFilter {
    private static final Logger LOG = LoggerFactory.getLogger(RangerKRBAuthenticationFilter.class);

    public static final String LOGOUT_URL = "/logout";

    static final String NAME_RULES            = "hadoop.security.auth_to_local";
    static final String TOKEN_VALID           = "ranger.admin.kerberos.token.valid.seconds";
    static final String COOKIE_DOMAIN         = "ranger.admin.kerberos.cookie.domain";
    static final String COOKIE_PATH           = "ranger.admin.kerberos.cookie.path";
    static final String PRINCIPAL             = "ranger.spnego.kerberos.principal";
    static final String KEYTAB                = "ranger.spnego.kerberos.keytab";
    static final String NAME_RULES_PARAM      = "kerberos.name.rules";
    static final String TOKEN_VALID_PARAM     = "token.validity";
    static final String COOKIE_DOMAIN_PARAM   = "cookie.domain";
    static final String COOKIE_PATH_PARAM     = "cookie.path";
    static final String PRINCIPAL_PARAM       = "kerberos.principal";
    static final String KEYTAB_PARAM          = "kerberos.keytab";
    static final String AUTH_TYPE             = "type";
    static final String RANGER_AUTH_TYPE      = "hadoop.security.authentication";
    static final String AUTH_COOKIE_NAME      = "hadoop.auth";
    static final String HOST_NAME             = "ranger.service.host";
    static final String ALLOW_TRUSTED_PROXY   = "ranger.authentication.allow.trustedproxy";
    static final String PROXY_PREFIX          = "ranger.proxyuser.";
    static final String RULES_MECHANISM       = "hadoop.security.rules.mechanism";
    static final String RULES_MECHANISM_PARAM = "kerberos.name.rules.mechanism";

    private static final String KERBEROS_TYPE = "kerberos";
    private static final String S_USER        = "suser";

    protected static ServletContext noContext = new ServletContext() {
        @Override
        public String getContextPath() {
            return null;
        }

        @Override
        public ServletContext getContext(String uripath) {
            return null;
        }

        @Override
        public int getMajorVersion() {
            return 0;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public int getEffectiveMajorVersion() {
            return 0;
        }

        @Override
        public int getEffectiveMinorVersion() {
            return 0;
        }

        @Override
        public String getMimeType(String file) {
            return null;
        }

        @Override
        public Set<String> getResourcePaths(String path) {
            return null;
        }

        @Override
        public URL getResource(String path) {
            return null;
        }

        @Override
        public InputStream getResourceAsStream(String path) {
            return null;
        }

        @Override
        public RequestDispatcher getRequestDispatcher(String path) {
            return null;
        }

        @Override
        public RequestDispatcher getNamedDispatcher(String name) {
            return null;
        }

        @Override
        public Servlet getServlet(String name) {
            return null;
        }

        @Override
        public Enumeration<Servlet> getServlets() {
            return null;
        }

        @Override
        public Enumeration<String> getServletNames() {
            return null;
        }

        @Override
        public void log(String msg) {
        }

        @Override
        public void log(Exception exception, String msg) {
        }

        @Override
        public void log(String message, Throwable throwable) {
        }

        @Override
        public String getRealPath(String path) {
            return null;
        }

        @Override
        public String getServerInfo() {
            return null;
        }

        @Override
        public String getInitParameter(String name) {
            return null;
        }

        @Override
        public Enumeration<String> getInitParameterNames() {
            return null;
        }

        @Override
        public boolean setInitParameter(String name, String value) {
            return false;
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public Enumeration<String> getAttributeNames() {
            return null;
        }

        @Override
        public void setAttribute(String name, Object object) {
        }

        @Override
        public void removeAttribute(String name) {
        }

        @Override
        public String getServletContextName() {
            return null;
        }

        @Override
        public ServletRegistration.Dynamic addServlet(String servletName, String className) {
            return null;
        }

        @Override
        public ServletRegistration.Dynamic addServlet(String servletName, Servlet servlet) {
            return null;
        }

        @Override
        public ServletRegistration.Dynamic addServlet(String servletName, Class<? extends Servlet> servletClass) {
            return null;
        }

        @Override
        public <T extends Servlet> T createServlet(Class<T> clazz) {
            return null;
        }

        @Override
        public ServletRegistration getServletRegistration(String servletName) {
            return null;
        }

        @Override
        public Map<String, ? extends ServletRegistration> getServletRegistrations() {
            return null;
        }

        @Override
        public Dynamic addFilter(String filterName, String className) {
            return null;
        }

        @Override
        public Dynamic addFilter(String filterName, Filter filter) {
            return null;
        }

        @Override
        public Dynamic addFilter(String filterName, Class<? extends Filter> filterClass) {
            return null;
        }

        @Override
        public <T extends Filter> T createFilter(Class<T> clazz) {
            return null;
        }

        @Override
        public FilterRegistration getFilterRegistration(String filterName) {
            return null;
        }

        @Override
        public Map<String, ? extends FilterRegistration> getFilterRegistrations() {
            return null;
        }

        @Override
        public SessionCookieConfig getSessionCookieConfig() {
            return null;
        }

        @Override
        public void setSessionTrackingModes(Set<SessionTrackingMode> sessionTrackingModes) {
        }

        @Override
        public Set<SessionTrackingMode> getDefaultSessionTrackingModes() {
            return null;
        }

        @Override
        public Set<SessionTrackingMode> getEffectiveSessionTrackingModes() {
            return null;
        }

        @Override
        public void addListener(String className) {
        }

        @Override
        public <T extends EventListener> void addListener(T t) {
        }

        @Override
        public void addListener(Class<? extends EventListener> listenerClass) {
        }

        @Override
        public <T extends EventListener> T createListener(Class<T> clazz) {
            return null;
        }

        @Override
        public JspConfigDescriptor getJspConfigDescriptor() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return null;
        }

        @Override
        public void declareRoles(String... roleNames) {
        }

        @Override
        public String getVirtualServerName() {
            return null;
        }
    };

    private final String originalUrlQueryParam = "originalUrl";

    @Autowired
    UserMgr userMgr;

    @Autowired
    RESTErrorUtil restErrorUtil;

    public RangerKRBAuthenticationFilter() {
        try {
            init(null);
        } catch (ServletException e) {
            LOG.error("Error while initializing Filter : {}", e.getMessage());
        }
    }

    @Override
    public void init(FilterConfig conf) throws ServletException {
        final FilterConfig        globalConf = conf;
        final Map<String, String> params     = new HashMap<>();

        params.put(AUTH_TYPE, PropertiesUtil.getProperty(RANGER_AUTH_TYPE, "simple"));
        params.put(NAME_RULES_PARAM, PropertiesUtil.getProperty(NAME_RULES, "DEFAULT"));
        params.put(TOKEN_VALID_PARAM, PropertiesUtil.getProperty(TOKEN_VALID, "30"));
        params.put(COOKIE_DOMAIN_PARAM, PropertiesUtil.getProperty(COOKIE_DOMAIN, PropertiesUtil.getProperty(HOST_NAME, "localhost")));
        params.put(COOKIE_PATH_PARAM, PropertiesUtil.getProperty(COOKIE_PATH, "/"));
        params.put(ALLOW_TRUSTED_PROXY, PropertiesUtil.getProperty(ALLOW_TRUSTED_PROXY, "false"));
        params.put(RULES_MECHANISM_PARAM, PropertiesUtil.getProperty(RULES_MECHANISM, "hadoop"));

        try {
            params.put(PRINCIPAL_PARAM, SecureClientLogin.getPrincipal(PropertiesUtil.getProperty(PRINCIPAL, ""), PropertiesUtil.getProperty(HOST_NAME)));
        } catch (IOException ignored) {
            // do nothing
        }

        params.put(KEYTAB_PARAM, PropertiesUtil.getProperty(KEYTAB, ""));

        FilterConfig myConf = new FilterConfig() {
            @Override
            public String getFilterName() {
                return "KerberosFilter";
            }

            @Override
            public ServletContext getServletContext() {
                if (globalConf != null) {
                    return globalConf.getServletContext();
                } else {
                    return noContext;
                }
            }

            @Override
            public String getInitParameter(String param) {
                return params.get(param);
            }

            @SuppressWarnings("unchecked")
            @Override
            public Enumeration<String> getInitParameterNames() {
                return new IteratorEnumeration(params.keySet().iterator());
            }
        };

        super.init(myConf);

        Configuration conf1 = this.getProxyuserConfiguration();

        ProxyUsers.refreshSuperUserGroupsConfiguration(conf1, PROXY_PREFIX);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        String             authtype     = PropertiesUtil.getProperty(RANGER_AUTH_TYPE);
        HttpServletRequest httpRequest  = (HttpServletRequest) request;
        Authentication     existingAuth = SecurityContextHolder.getContext().getAuthentication();

        if (isSpnegoEnable(authtype) && (existingAuth == null || !existingAuth.isAuthenticated())) {
            KerberosName.setRules(PropertiesUtil.getProperty(NAME_RULES, "DEFAULT"));

            String userName = null;

            LOG.debug("isSpnegoEnable = {}, userName = {}, request URL = {}", isSpnegoEnable(authtype), userName, getRequestURL(httpRequest));

            if (existingAuth != null) {
                LOG.debug("isAuthenticated: {}", existingAuth.isAuthenticated());
            }

            try {
                if (StringUtils.equals(httpRequest.getParameter("action"), RestUtil.TIMEOUT_ACTION)) {
                    handleTimeoutRequest(httpRequest, (HttpServletResponse) response);
                } else {
                    super.doFilter(request, response, filterChain);
                }
            } catch (Exception e) {
                throw restErrorUtil.createRESTException("RangerKRBAuthenticationFilter Failed : " + e.getMessage());
            }
        } else {
            String action   = httpRequest.getParameter("action");
            String doAsUser = request.getParameter("doAs");

            LOG.debug("RangerKRBAuthenticationFilter: request URL = {}", httpRequest.getRequestURI());

            boolean allowTrustedProxy = PropertiesUtil.getBooleanProperty(ALLOW_TRUSTED_PROXY, false);

            if (isSpnegoEnable(authtype) && allowTrustedProxy && StringUtils.isNotEmpty(doAsUser) && existingAuth != null && existingAuth.isAuthenticated()) {
                request.setAttribute("spnegoEnabled", true);
                request.setAttribute("trustedProxyEnabled", true);
            }

            if (allowTrustedProxy && StringUtils.isNotEmpty(doAsUser) && existingAuth != null && existingAuth.isAuthenticated() && StringUtils.equals(action, RestUtil.TIMEOUT_ACTION)) {
                HttpServletResponse httpResponse = (HttpServletResponse) response;

                handleTimeoutRequest(httpRequest, httpResponse);
            } else {
                filterChain.doFilter(request, response);
            }
        }
    }

    @Override
    protected void doFilter(FilterChain filterChain, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        String  authType          = PropertiesUtil.getProperty(RANGER_AUTH_TYPE);
        String  userName          = null;
        boolean checkCookie       = response.containsHeader("Set-Cookie");
        boolean allowTrustedProxy = PropertiesUtil.getBooleanProperty(ALLOW_TRUSTED_PROXY, false);

        if (checkCookie) {
            Collection<String> authUserName = response.getHeaders("Set-Cookie");

            if (authUserName != null) {
                for (String cookie : authUserName) {
                    if (!StringUtils.isEmpty(cookie)) {
                        if (cookie.toLowerCase().startsWith(AUTH_COOKIE_NAME.toLowerCase()) && cookie.contains("u=")) {
                            String[] split = cookie.split(";");

                            for (String s : split) {
                                if (!StringUtils.isEmpty(s) && s.toLowerCase().startsWith(AUTH_COOKIE_NAME.toLowerCase())) {
                                    int ustr = s.indexOf("u=");

                                    if (ustr != -1) {
                                        int andStr = s.indexOf("&", ustr);

                                        if (andStr != -1) {
                                            try {
                                                userName = s.substring(ustr + 2, andStr);
                                            } catch (Exception e) {
                                                userName = null;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        String sessionUserName = request.getParameter(S_USER);
        String pathInfo        = request.getPathInfo();

        if (!StringUtils.isEmpty(sessionUserName) && "keyadmin".equalsIgnoreCase(sessionUserName) && !StringUtils.isEmpty(pathInfo) && pathInfo.contains("public/v2/api/service")) {
            LOG.info("Session will be created by : {}", sessionUserName);

            userName = sessionUserName;
        }

        LOG.debug("Remote user from request = {}", request.getRemoteUser());

        if ((isSpnegoEnable(authType) && (!StringUtils.isEmpty(userName)))) {
            Authentication existingAuth = SecurityContextHolder.getContext().getAuthentication();

            if (existingAuth == null || !existingAuth.isAuthenticated()) {
                //--------------------------- To Create Ranger Session --------------------------------------
                String rangerLdapDefaultRole = PropertiesUtil.getProperty("ranger.ldap.default.role", "ROLE_USER");

                LOG.debug("Http headers: {}", Collections.list(request.getHeaderNames()));

                String doAsUser = request.getParameter("doAs");

                if (allowTrustedProxy && doAsUser != null && !doAsUser.isEmpty()) {
                    LOG.debug("userPrincipal from request = {} request parameters = {}", request.getUserPrincipal(), request.getParameterMap().keySet());

                    AuthenticationToken authToken = (AuthenticationToken) request.getUserPrincipal();

                    if (authToken != null && authToken != AuthenticationToken.ANONYMOUS) {
                        LOG.debug("remote user from authtoken = {}", authToken.getUserName());

                        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(authToken.getUserName(), SaslRpcServer.AuthMethod.KERBEROS);

                        if (ugi != null) {
                            ugi = UserGroupInformation.createProxyUser(doAsUser, ugi);

                            LOG.debug("Real user from UGI = {}", ugi.getRealUser().getShortUserName());

                            try {
                                ProxyUsers.authorize(ugi, request.getRemoteAddr());
                            } catch (AuthorizationException ex) {
                                HttpExceptionUtils.createServletExceptionResponse(response, 403, ex);

                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Authentication exception: {}", ex.getMessage(), ex);
                                } else {
                                    LOG.warn("Authentication exception: {}", ex.getMessage());
                                }

                                return;
                            }

                            final List<GrantedAuthority> grantedAuths = new ArrayList<>();

                            grantedAuths.add(new SimpleGrantedAuthority(rangerLdapDefaultRole));

                            final UserDetails        principal      = new User(doAsUser, "", grantedAuths);
                            Authentication           authentication = new UsernamePasswordAuthenticationToken(principal, "", grantedAuths);
                            WebAuthenticationDetails webDetails     = new WebAuthenticationDetails(request);

                            ((AbstractAuthenticationToken) authentication).setDetails(webDetails);

                            authentication = getGrantedAuthority(authentication);

                            SecurityContextHolder.getContext().setAuthentication(authentication);

                            request.setAttribute("spnegoEnabled", true);
                            request.setAttribute("trustedProxyEnabled", true);

                            LOG.info("Logged into Ranger as doAsUser = {}, by authenticatedUser = {}", doAsUser, authToken.getUserName());
                        }
                    }
                } else {
                    //if we get the userName from the token then log into ranger using the same user
                    final List<GrantedAuthority> grantedAuths = new ArrayList<>();

                    grantedAuths.add(new SimpleGrantedAuthority(rangerLdapDefaultRole));

                    final UserDetails                 principal           = new User(userName, "", grantedAuths);
                    final AbstractAuthenticationToken finalAuthentication = new UsernamePasswordAuthenticationToken(principal, "", grantedAuths);
                    WebAuthenticationDetails          webDetails          = new WebAuthenticationDetails(request);

                    finalAuthentication.setDetails(webDetails);

                    Authentication authentication = getGrantedAuthority(finalAuthentication);

                    if (authentication != null && authentication.isAuthenticated()) {
                        if (request.getParameterMap().containsKey("doAs")) {
                            if (!response.isCommitted()) {
                                LOG.debug("Request contains unsupported parameter, doAs.");

                                request.setAttribute("spnegoenabled", false);
                                response.sendError(HttpServletResponse.SC_FORBIDDEN, "Missing authentication token.");
                            }
                        }
                        if (request.getParameterMap().containsKey("user.name")) {
                            if (!response.isCommitted()) {
                                LOG.debug("Request contains an unsupported parameter user.name");

                                request.setAttribute("spnegoenabled", false);
                                response.sendError(HttpServletResponse.SC_FORBIDDEN, "Missing authentication token.");
                            } else {
                                LOG.info("Response seems to be already committed for user.name.");
                            }
                        }
                    }

                    SecurityContextHolder.getContext().setAuthentication(authentication);

                    request.setAttribute("spnegoEnabled", true);

                    LOG.debug("Logged into Ranger as = {}", userName);
                }
                filterChain.doFilter(request, response);
            } else {
                try {
                    super.doFilter(filterChain, request, response);
                } catch (Exception e) {
                    throw restErrorUtil.createRESTException("RangerKRBAuthenticationFilter Failed : " + e.getMessage());
                }
            }
        } else {
            filterChain.doFilter(request, response);
        }
    }

    protected Configuration getProxyuserConfiguration() {
        Configuration       conf          = new Configuration(false);
        Map<String, String> propertiesMap = PropertiesUtil.getPropertiesMap();

        for (String key : propertiesMap.keySet()) {
            if (!key.startsWith(PROXY_PREFIX)) {
                continue;
            }

            conf.set(key, propertiesMap.get(key));
        }

        return conf;
    }

    private void handleTimeoutRequest(HttpServletRequest httpRequest, HttpServletResponse httpResponse) throws IOException {
        String xForwardedURL = RestUtil.constructForwardableURL(httpRequest);

        LOG.debug("xForwardedURL = {}", xForwardedURL);

        String logoutUrl = xForwardedURL;

        logoutUrl = StringUtils.replace(logoutUrl, httpRequest.getRequestURI(), LOGOUT_URL);

        LOG.debug("logoutUrl value is {}", logoutUrl);

        String redirectUrl = RestUtil.constructRedirectURL(httpRequest, logoutUrl, xForwardedURL, originalUrlQueryParam);

        LOG.debug("Redirect URL = {}", redirectUrl);
        LOG.debug("session id = {}", httpRequest.getRequestedSessionId());

        HttpSession httpSession = httpRequest.getSession(false);

        if (httpSession != null) {
            httpSession.invalidate();
        }

        httpResponse.setHeader("Content-Type", "application/x-http-headers");
        httpResponse.sendRedirect(redirectUrl);
    }

    private boolean isSpnegoEnable(String authType) {
        String principal  = PropertiesUtil.getProperty(PRINCIPAL);
        String keytabPath = PropertiesUtil.getProperty(KEYTAB);

        return ((!StringUtils.isEmpty(authType)) && KERBEROS_TYPE.equalsIgnoreCase(authType) && SecureClientLogin.isKerberosCredentialExists(principal, keytabPath));
    }

    private Authentication getGrantedAuthority(Authentication authentication) {
        UsernamePasswordAuthenticationToken result;

        if (authentication != null && authentication.isAuthenticated()) {
            final List<GrantedAuthority> grantedAuths = getAuthorities(authentication.getName());
            final UserDetails            userDetails  = new User(authentication.getName(), authentication.getCredentials().toString(), grantedAuths);

            result = new UsernamePasswordAuthenticationToken(userDetails, authentication.getCredentials(), grantedAuths);

            result.setDetails(authentication.getDetails());

            return result;
        }

        return authentication;
    }

    private List<GrantedAuthority> getAuthorities(String username) {
        Collection<String>           roleList     = userMgr.getRolesByLoginId(username);
        final List<GrantedAuthority> grantedAuths = new ArrayList<>();

        for (String role : roleList) {
            grantedAuths.add(new SimpleGrantedAuthority(role));
        }

        return grantedAuths;
    }
}
