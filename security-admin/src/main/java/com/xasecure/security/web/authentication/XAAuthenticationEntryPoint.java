/**
 *
 */
package com.xasecure.security.web.authentication;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.xasecure.common.JSONUtil;
import com.xasecure.common.PropertiesUtil;
import com.xasecure.common.XAConfigUtil;
import com.xasecure.view.VXResponse;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;


/**
 * 
 *
 */
public class XAAuthenticationEntryPoint extends
LoginUrlAuthenticationEntryPoint {
    static Logger logger = Logger.getLogger(XAAuthenticationEntryPoint.class);
    static int ajaxReturnCode = -1;
    
    @Autowired
    XAConfigUtil configUtil;
    
    @Autowired
    JSONUtil jsonUtil;

    public XAAuthenticationEntryPoint() {
	super();
	if (logger.isDebugEnabled()) {
	    logger.debug("AjaxAwareAuthenticationEntryPoint(): constructor");
	}

	if (ajaxReturnCode < 0) {
	    ajaxReturnCode = PropertiesUtil.getIntProperty(
		    "xa.ajax.auth.required.code", 401);
	}
    }

    @Override
    public void commence(HttpServletRequest request,
	    HttpServletResponse response, AuthenticationException authException)
    throws IOException, ServletException {
	String ajaxRequestHeader = request.getHeader("X-Requested-With");
	if (logger.isDebugEnabled()) {
	    logger.debug("commence() X-Requested-With=" + ajaxRequestHeader);
	}

	String requestURL = (request.getRequestURL() != null) ? request.getRequestURL().toString() : "";
	String servletPath = PropertiesUtil.getProperty("xa.servlet.mapping.url.pattern", "service");
	String reqServletPath = configUtil.getWebAppRootURL() + "/" + servletPath;

	response.setContentType("application/json;charset=UTF-8");
	response.setHeader("Cache-Control", "no-cache");
	try {

		VXResponse vXResponse = new VXResponse();

		vXResponse.setStatusCode(HttpServletResponse.SC_UNAUTHORIZED);
		vXResponse.setMsgDesc("Authentication Failed");

		response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
		response.getWriter().write(jsonUtil.writeObjectAsString(vXResponse));
	} catch (IOException e) {
		logger.info("Error while writing JSON in HttpServletResponse");
	}
	
	if (ajaxRequestHeader != null && ajaxRequestHeader.equalsIgnoreCase("XMLHttpRequest")) {
	    if (logger.isDebugEnabled()) {
		logger.debug("commence() AJAX request. Authentication required. Returning "
			+ ajaxReturnCode + ". URL=" + request.getRequestURI());
	    }
    	response.sendError(ajaxReturnCode, "");
	} else if(!(requestURL.startsWith(reqServletPath))) {
		super.commence(request, response, authException);
	}
    }
}