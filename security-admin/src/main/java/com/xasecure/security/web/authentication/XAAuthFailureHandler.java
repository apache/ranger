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
import com.xasecure.view.VXResponse;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.ExceptionMappingAuthenticationFailureHandler;

/**
 * 
 *
 */
public class XAAuthFailureHandler extends
ExceptionMappingAuthenticationFailureHandler {
    static Logger logger = Logger.getLogger(XAAuthFailureHandler.class);

    String ajaxLoginfailurePage = null;
    
    @Autowired
    JSONUtil jsonUtil;

    public XAAuthFailureHandler() {
	super();
	if (ajaxLoginfailurePage == null) {
	    ajaxLoginfailurePage = PropertiesUtil.getProperty(
		    "xa.ajax.auth.failure.page", "/ajax_failure.jsp");
	}
    }

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.security.web.authentication.
     * ExceptionMappingAuthenticationFailureHandler
     * #onAuthenticationFailure(javax.servlet.http.HttpServletRequest,
     * javax.servlet.http.HttpServletResponse,
     * org.springframework.security.core.AuthenticationException)
     */
    @Override
    public void onAuthenticationFailure(HttpServletRequest request,
	    HttpServletResponse response, AuthenticationException exception)
    throws IOException, ServletException {
	String ajaxRequestHeader = request.getHeader("X-Requested-With");
	if (logger.isDebugEnabled()) {
	    logger.debug("commence() X-Requested-With=" + ajaxRequestHeader);
	}
	
		response.setContentType("application/json;charset=UTF-8");
		response.setHeader("Cache-Control", "no-cache");
		String jsonResp = "";
		try {
			VXResponse vXResponse = new VXResponse();
			vXResponse.setStatusCode(HttpServletResponse.SC_UNAUTHORIZED);
			vXResponse.setMsgDesc("Bad Credentials");

			jsonResp = jsonUtil.writeObjectAsString(vXResponse);
			response.getWriter().write(jsonResp);
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
		} catch (IOException e) {
			logger.info("Error while writing JSON in HttpServletResponse");
		}
	
	if (ajaxRequestHeader != null && ajaxRequestHeader.equalsIgnoreCase("XMLHttpRequest")) {
//	    if (logger.isDebugEnabled()) {
//		logger.debug("Forwarding AJAX login request failure to "
//			+ ajaxLoginfailurePage);
//	    }
//	    request.getRequestDispatcher(ajaxLoginfailurePage).forward(request,
//		    response);
		if (logger.isDebugEnabled()) {
			logger.debug("Sending login failed response : " + jsonResp);
		}
	} else {
//	    super.onAuthenticationFailure(request, response, exception);
	}
    }

}
