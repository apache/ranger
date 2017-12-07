package org.apache.ranger.security.web.filter;

import org.apache.commons.collections.iterators.IteratorEnumeration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.ranger.biz.UserMgr;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.security.handler.RangerAuthenticationProvider;
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
import javax.servlet.*;
import javax.servlet.descriptor.JspConfigDescriptor;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * <p> The {@link AltiscaleRangerAuthFilter} is a subclass of {@link AuthenticationFilter} which enables protecting
 * web application resources with different (pluggable) authentication mechanisms and signer secret
 * providers.
 * </p>
 * <p>
 * This is a specialized filter that initializes parameters considering {@link com.altiscale.hadoop.security.AltiscaleAuthenticationHandler}
 * as its authentication handler. The super class handles authentication protocol. This subclass implements
 * {@link AuthenticationFilter#doFilter(FilterChain, HttpServletRequest, HttpServletResponse)} to create Ranger
 * Portal session.
 * </p>
 */
public class AltiscaleRangerAuthFilter extends AuthenticationFilter{
	private static final Logger LOG = LoggerFactory.getLogger(AltiscaleRangerAuthFilter.class);

	// Constants to initialize configuration parameters
	private static final String CONFIG_PREFIX = "ranger.web.authentication.";
	private static final String AUTH_TYPE = "type";
	private static final String RANGER_ALGORITHM = "ranger.web.authentication.alt-kerberos.algorithm";
	private static final String RANGER_CERTIFICATE_DIR = "ranger.web.authentication.alt-kerberos.certificatedir";
	private static final String RANGER_AUTHENTICATION_PORTAL = "ranger.web.authentication.alt-kerberos.portal";
	private static final String RANGER_ACCOUNT_ID = "ranger.web.authentication.alt-kerberos.accountid";
	private static final String RANGER_CLUSTER_ID = "ranger.web.authentication.alt-kerberos.clusterid";
	private static final String RANGER_SIGNATURE_SECRET_FILE = "ranger.web.authentication.alt-kerberos.signature.secret.file";
	private static final String RANGER_TOKEN_VALIDITY = "ranger.web.authentication.alt-kerberos.token.validity";
	private static final String RANGER_COOKIE_DOMAIN = "ranger.web.authentication.alt-kerberos.cookie.domain";
	private static final String RANGER_NON_BROWSER_USER_AGENTS = "ranger.web.authentication.alt-kerberos.non-browser.user-agents";
	private static final String RANGER_ALT_KERBEROS_ENABLED = "ranger.web.authentication.alt-kerberos.enabled";
	private static final String PRINCIPAL = "ranger.spnego.kerberos.principal";
	private static final String KEYTAB = "ranger.spnego.kerberos.keytab";
	private static final String HOST_NAME = "ranger.service.host";

	@Autowired
	UserMgr userMgr;

	public AltiscaleRangerAuthFilter() {
		try {
			init(null);
		} catch (ServletException e) {
			LOG.error("Error while initializing AltiscaleRangerAuthFilter: "+e.getMessage());
		}
	}

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		LOG.debug("AltiscaleRangerAuthFilter.init(FilterConfig filterConfig) <== started");

		final FilterConfig globalConf = filterConfig;
		final Map<String, String> params = new HashMap<String, String>();
		params.put(RANGER_ALGORITHM.replaceFirst(CONFIG_PREFIX,""), PropertiesUtil.getProperty(RANGER_ALGORITHM));
		params.put(RANGER_CERTIFICATE_DIR.replaceFirst(CONFIG_PREFIX,""), PropertiesUtil.getProperty(RANGER_CERTIFICATE_DIR));
		params.put(RANGER_AUTHENTICATION_PORTAL.replaceFirst(CONFIG_PREFIX,""), PropertiesUtil.getProperty(RANGER_AUTHENTICATION_PORTAL));
		params.put(RANGER_ACCOUNT_ID.replaceFirst(CONFIG_PREFIX,""), PropertiesUtil.getProperty(RANGER_ACCOUNT_ID));
		params.put(RANGER_CLUSTER_ID.replaceFirst(CONFIG_PREFIX,""), PropertiesUtil.getProperty(RANGER_CLUSTER_ID));
		params.put(RANGER_ALT_KERBEROS_ENABLED.replaceFirst(CONFIG_PREFIX,""), PropertiesUtil.getProperty(RANGER_ALT_KERBEROS_ENABLED));
		params.put(RANGER_SIGNATURE_SECRET_FILE.replaceFirst(CONFIG_PREFIX,""), PropertiesUtil.getProperty(RANGER_SIGNATURE_SECRET_FILE));
		params.put(RANGER_TOKEN_VALIDITY.replaceFirst(CONFIG_PREFIX,""), PropertiesUtil.getProperty(RANGER_TOKEN_VALIDITY));
		params.put(RANGER_COOKIE_DOMAIN.replaceFirst(CONFIG_PREFIX+"alt-kerberos.",""), PropertiesUtil.getProperty(RANGER_COOKIE_DOMAIN));
		params.put(RANGER_NON_BROWSER_USER_AGENTS.replaceFirst(CONFIG_PREFIX,""), PropertiesUtil.getProperty(RANGER_NON_BROWSER_USER_AGENTS));
		try {
			params.put("kerberos.principal", SecureClientLogin.getPrincipal(PropertiesUtil.getProperty(PRINCIPAL,""), PropertiesUtil.getProperty(HOST_NAME)));
		} catch (IOException ignored) {
			// do nothing
		}
		params.put("kerberos.keytab", PropertiesUtil.getProperty(KEYTAB,""));
		params.put(AUTH_TYPE, "com.altiscale.hadoop.security.AltiscaleAuthenticationHandler");
		FilterConfig myConf = new FilterConfig() {
			@Override
			public ServletContext getServletContext() {
				if (globalConf != null) {
					return globalConf.getServletContext();
				} else {
					return noContext;
				}
			}

			@SuppressWarnings("unchecked")
			@Override
			public Enumeration<String> getInitParameterNames() {
				return new IteratorEnumeration(params.keySet().iterator());
			}

			@Override
			public String getInitParameter(String param) {
				return params.get(param);
			}

			@Override
			public String getFilterName() {
				return "KerberosFilter";
			}
		};
		super.init(myConf);
		LOG.debug("AltiscaleRangerAuthFilter.init(FilterConfig filterConfig) ==> ended");
	}

	/**
	 * This function performs post tasks after {@link AuthenticationFilter#doFilter(ServletRequest, ServletResponse, FilterChain)}
	 * to create a ranger session if the request is authenticated.
	 */
	@Override
	protected void doFilter(FilterChain filterChain, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

		String userName = null;
		boolean checkCookie = response.containsHeader("Set-Cookie");
		if(checkCookie){
			Collection<String> authUserName = response.getHeaders("Set-Cookie");
			if(authUserName != null){
				Iterator<String> i = authUserName.iterator();
				while(i.hasNext()){
					String cookie = i.next();
					if(!StringUtils.isEmpty(cookie)){
						if(cookie.toLowerCase().startsWith(AuthenticatedURL.AUTH_COOKIE.toLowerCase()) && cookie.contains("u=")){
							String[] split = cookie.split(";");
							if(split != null){
								for(String s : split){
									if(!StringUtils.isEmpty(s) && s.toLowerCase().startsWith(AuthenticatedURL.AUTH_COOKIE.toLowerCase())){
										int ustr = s.indexOf("u=");
										if(ustr != -1){
											int andStr = s.indexOf("&", ustr);
											if(andStr != -1){
												try{
													userName = s.substring(ustr+2, andStr);
												}catch(Exception e){
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
		}
		// if security context does not have a user session, authenticate the security context and create a session
		if(!isAuthenticated()){
			String defaultUserRole = "ROLE_USER";
			//if the userName is found on the token then log into ranger using the same user
			if (userName != null && !userName.trim().isEmpty()) {
				final List<GrantedAuthority> grantedAuths = new ArrayList<>();
				grantedAuths.add(new SimpleGrantedAuthority(defaultUserRole));
				final UserDetails principal = new User(userName, "",grantedAuths);
				final Authentication finalAuthentication = new UsernamePasswordAuthenticationToken(principal, "", grantedAuths);
				WebAuthenticationDetails webDetails = new WebAuthenticationDetails(request);
				((AbstractAuthenticationToken) finalAuthentication).setDetails(webDetails);
				RangerAuthenticationProvider authenticationProvider = new RangerAuthenticationProvider();
				authenticationProvider.setAlt_ssoEnabled(true);
				Authentication authentication = authenticationProvider.authenticate(finalAuthentication);
				authentication = getGrantedAuthority(authentication);
				SecurityContextHolder.getContext().setAuthentication(authentication);
			}
		}
		// Delegate call to next filters
		super.doFilter(filterChain, request, response);
	}

	private Authentication getGrantedAuthority(Authentication authentication) {
		UsernamePasswordAuthenticationToken result=null;
		if(authentication!=null && authentication.isAuthenticated()){
			final List<GrantedAuthority> grantedAuths=getAuthorities(authentication.getName().toString());
			final UserDetails userDetails = new User(authentication.getName().toString(), authentication.getCredentials().toString(),grantedAuths);
			result = new UsernamePasswordAuthenticationToken(userDetails,authentication.getCredentials(),grantedAuths);
			result.setDetails(authentication.getDetails());
			return result;
		}
		return authentication;
	}

	private List<GrantedAuthority> getAuthorities(String username) {
		Collection<String> roleList=userMgr.getRolesByLoginId(username);
		final List<GrantedAuthority> grantedAuths = new ArrayList<>();
		for(String role:roleList){
			grantedAuths.add(new SimpleGrantedAuthority(role));
		}
		return grantedAuths;
	}

	private boolean isAuthenticated() {
		Authentication existingAuth = SecurityContextHolder.getContext().getAuthentication();
		return (existingAuth != null && existingAuth.isAuthenticated());
	}

	protected static ServletContext noContext = new ServletContext() {

		@Override
		public void setSessionTrackingModes(
				Set<SessionTrackingMode> sessionTrackingModes) {
		}

		@Override
		public boolean setInitParameter(String name, String value) {
			return false;
		}

		@Override
		public void setAttribute(String name, Object object) {
		}

		@Override
		public void removeAttribute(String name) {
		}

		@Override
		public void log(String message, Throwable throwable) {
		}

		@Override
		public void log(Exception exception, String msg) {
		}

		@Override
		public void log(String msg) {
		}

		@Override
		public String getVirtualServerName() {
			return null;
		}

		@Override
		public SessionCookieConfig getSessionCookieConfig() {
			return null;
		}

		@Override
		public Enumeration<Servlet> getServlets() {
			return null;
		}

		@Override
		public Map<String, ? extends ServletRegistration> getServletRegistrations() {
			return null;
		}

		@Override
		public ServletRegistration getServletRegistration(String servletName) {
			return null;
		}

		@Override
		public Enumeration<String> getServletNames() {
			return null;
		}

		@Override
		public String getServletContextName() {
			return null;
		}

		@Override
		public Servlet getServlet(String name) throws ServletException {
			return null;
		}

		@Override
		public String getServerInfo() {
			return null;
		}

		@Override
		public Set<String> getResourcePaths(String path) {
			return null;
		}

		@Override
		public InputStream getResourceAsStream(String path) {
			return null;
		}

		@Override
		public URL getResource(String path) throws MalformedURLException {
			return null;
		}

		@Override
		public RequestDispatcher getRequestDispatcher(String path) {
			return null;
		}

		@Override
		public String getRealPath(String path) {
			return null;
		}

		@Override
		public RequestDispatcher getNamedDispatcher(String name) {
			return null;
		}

		@Override
		public int getMinorVersion() {
			return 0;
		}

		@Override
		public String getMimeType(String file) {
			return null;
		}

		@Override
		public int getMajorVersion() {
			return 0;
		}

		@Override
		public JspConfigDescriptor getJspConfigDescriptor() {
			return null;
		}

		@Override
		public Enumeration<String> getInitParameterNames() {
			return null;
		}

		@Override
		public String getInitParameter(String name) {
			return null;
		}

		@Override
		public Map<String, ? extends FilterRegistration> getFilterRegistrations() {
			return null;
		}

		@Override
		public FilterRegistration getFilterRegistration(String filterName) {
			return null;
		}

		@Override
		public Set<SessionTrackingMode> getEffectiveSessionTrackingModes() {
			return null;
		}

		@Override
		public int getEffectiveMinorVersion() {
			return 0;
		}

		@Override
		public int getEffectiveMajorVersion() {
			return 0;
		}

		@Override
		public Set<SessionTrackingMode> getDefaultSessionTrackingModes() {
			return null;
		}

		@Override
		public String getContextPath() {
			return null;
		}

		@Override
		public ServletContext getContext(String uripath) {
			return null;
		}

		@Override
		public ClassLoader getClassLoader() {
			return null;
		}

		@Override
		public Enumeration<String> getAttributeNames() {
			return null;
		}

		@Override
		public Object getAttribute(String name) {
			return null;
		}

		@Override
		public void declareRoles(String... roleNames) {
		}

		@Override
		public <T extends Servlet> T createServlet(Class<T> clazz)
				throws ServletException {
			return null;
		}

		@Override
		public <T extends EventListener> T createListener(Class<T> clazz)
				throws ServletException {
			return null;
		}

		@Override
		public <T extends Filter> T createFilter(Class<T> clazz)
				throws ServletException {
			return null;
		}

		@Override
		public javax.servlet.ServletRegistration.Dynamic addServlet(
				String servletName, Class<? extends Servlet> servletClass) {
			return null;
		}

		@Override
		public javax.servlet.ServletRegistration.Dynamic addServlet(
				String servletName, Servlet servlet) {
			return null;
		}

		@Override
		public javax.servlet.ServletRegistration.Dynamic addServlet(
				String servletName, String className) {
			return null;
		}

		@Override
		public void addListener(Class<? extends EventListener> listenerClass) {
		}

		@Override
		public <T extends EventListener> void addListener(T t) {
		}

		@Override
		public void addListener(String className) {
		}

		@Override
		public FilterRegistration.Dynamic addFilter(String filterName,
													Class<? extends Filter> filterClass) {
			return null;
		}

		@Override
		public FilterRegistration.Dynamic addFilter(String filterName, Filter filter) {
			return null;
		}

		@Override
		public FilterRegistration.Dynamic addFilter(String filterName, String className) {
			return null;
		}
	};
}
