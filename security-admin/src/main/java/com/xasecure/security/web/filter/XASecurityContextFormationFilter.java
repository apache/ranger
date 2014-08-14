/**
 *
 */
package com.xasecure.security.web.filter;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.xasecure.biz.SessionMgr;
import com.xasecure.common.GUIDUtil;
import com.xasecure.common.HTTPUtil;
import com.xasecure.common.PropertiesUtil;
import com.xasecure.common.RequestContext;
import com.xasecure.common.UserSessionBase;
import com.xasecure.entity.XXAuthSession;
import com.xasecure.util.RestUtil;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.GenericFilterBean;

import com.xasecure.security.context.XAContextHolder;
import com.xasecure.security.context.XASecurityContext;

public class XASecurityContextFormationFilter extends GenericFilterBean {

	static Logger logger = Logger
			.getLogger(XASecurityContextFormationFilter.class);

	public static final String AKA_SC_SESSION_KEY = "AKA_SECURITY_CONTEXT";
	public static final String USER_AGENT = "User-Agent";

	@Autowired
	SessionMgr sessionMgr;

	@Autowired
	HTTPUtil httpUtil;

	String testIP = null;

	public XASecurityContextFormationFilter() {
		testIP = PropertiesUtil.getProperty("xa.env.ip");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see javax.servlet.Filter#doFilter(javax.servlet.ServletRequest,
	 * javax.servlet.ServletResponse, javax.servlet.FilterChain)
	 */
	@Override
	public void doFilter(ServletRequest request, ServletResponse response,
			FilterChain chain) throws IOException, ServletException {
		
		try {
			Authentication auth = SecurityContextHolder.getContext()
					.getAuthentication();

			if (auth instanceof AnonymousAuthenticationToken) {
				// ignore
			} else {
				HttpServletRequest httpRequest = (HttpServletRequest) request;
				HttpSession httpSession = httpRequest.getSession(false);

				// [1]get the context from session
				XASecurityContext context = (XASecurityContext) httpSession.getAttribute(AKA_SC_SESSION_KEY);
				int clientTimeOffset = 0;
				if (context == null) {
					context = new XASecurityContext();
					httpSession.setAttribute(AKA_SC_SESSION_KEY, context);					
				}
				String userAgent = httpRequest.getHeader(USER_AGENT);
				if(httpRequest!=null){						
					clientTimeOffset=RestUtil.getTimeOffset(httpRequest);	
					
				}
				// Get the request specific info
				RequestContext requestContext = new RequestContext();
				String reqIP = testIP;
				if (testIP == null) {
					reqIP = httpRequest.getRemoteAddr();
				}
				requestContext.setIpAddress(reqIP);
				requestContext.setUserAgent(userAgent);
				requestContext.setDeviceType(httpUtil
						.getDeviceType(httpRequest));
				requestContext.setServerRequestId(GUIDUtil.genGUI());
				requestContext.setRequestURL(httpRequest.getRequestURI());				
										
				requestContext.setClientTimeOffsetInMinute(clientTimeOffset);
				context.setRequestContext(requestContext);			

				XAContextHolder.setSecurityContext(context);

				UserSessionBase userSession = sessionMgr.processSuccessLogin(
						XXAuthSession.AUTH_TYPE_PASSWORD, userAgent);
				
				if(userSession!=null && userSession.getClientTimeOffsetInMinute()==0){
					userSession.setClientTimeOffsetInMinute(clientTimeOffset);
				}
				
				context.setUserSession(userSession);
			}
			chain.doFilter(request, response);

		} finally {
			// [4]remove context from thread-local
			XAContextHolder.resetSecurityContext();
		}
	}
}
