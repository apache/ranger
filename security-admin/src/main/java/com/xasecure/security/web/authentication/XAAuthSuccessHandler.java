/**
 *
 */
package com.xasecure.security.web.authentication;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.xasecure.biz.SessionMgr;
import com.xasecure.common.JSONUtil;
import com.xasecure.common.PropertiesUtil;
import com.xasecure.entity.XXAuthSession;
import com.xasecure.view.VXResponse;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.SavedRequestAwareAuthenticationSuccessHandler;
import org.springframework.security.web.authentication.WebAuthenticationDetails;

/**
 * 
 *
 */
public class XAAuthSuccessHandler extends
SavedRequestAwareAuthenticationSuccessHandler {
    static Logger logger = Logger.getLogger(XAAuthSuccessHandler.class);

    String ajaxLoginSuccessPage = null;
    
    @Autowired
    SessionMgr sessionMgr;
    
    @Autowired
    JSONUtil jsonUtil;

    public XAAuthSuccessHandler() {
	super();
	if (ajaxLoginSuccessPage == null) {
	    ajaxLoginSuccessPage = PropertiesUtil.getProperty(
		    "xa.ajax.auth.success.page", "/ajax_success.html");
	}
    }

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.security.web.authentication.
     * SavedRequestAwareAuthenticationSuccessHandler
     * #onAuthenticationSuccess(javax.servlet.http.HttpServletRequest,
     * javax.servlet.http.HttpServletResponse,
     * org.springframework.security.core.Authentication)
     */
    @Override
    public void onAuthenticationSuccess(HttpServletRequest request,
	    HttpServletResponse response, Authentication authentication)
    throws ServletException, IOException {
    	
    	WebAuthenticationDetails details = (WebAuthenticationDetails) authentication
    		.getDetails();
    	String remoteAddress = details != null ? details.getRemoteAddress()
    		: "";
    	String sessionId = details != null ? details.getSessionId() : "";
    	
    	boolean isValidUser = sessionMgr.isValidXAUser(authentication.getName());
    	
    	response.setContentType("application/json;charset=UTF-8");
		response.setHeader("Cache-Control", "no-cache");

		VXResponse vXResponse = new VXResponse();
    	
    	if(!isValidUser) {
    		sessionMgr.processFailureLogin(
    				XXAuthSession.AUTH_STATUS_USER_NOT_FOUND,
    				XXAuthSession.AUTH_TYPE_PASSWORD, authentication.getName(),
    				remoteAddress, sessionId);
    		authentication.setAuthenticated(false);
    		
			vXResponse.setStatusCode(HttpServletResponse.SC_PRECONDITION_FAILED);
			vXResponse.setMsgDesc("Auth Succeeded but user is not synced yet for " + authentication.getName());

			response.setStatus(HttpServletResponse.SC_PRECONDITION_FAILED);
			response.getWriter().write(jsonUtil.writeObjectAsString(vXResponse));

			// response.sendError(HttpServletResponse.SC_PRECONDITION_FAILED);
			logger.info("Auth Succeeded but user is not synced yet for "
					+ authentication.getName());
    		
    	} else {
    	
			String ajaxRequestHeader = request.getHeader("X-Requested-With");
			if (logger.isDebugEnabled()) {
			    logger.debug("commence() X-Requested-With=" + ajaxRequestHeader);
			}
			if (ajaxRequestHeader != null && ajaxRequestHeader.equalsIgnoreCase("XMLHttpRequest")) {
				// if (logger.isDebugEnabled()) {
				// logger.debug("Forwarding AJAX login request success to "
				// + ajaxLoginSuccessPage + " for user "
				// + authentication.getName());
				// }
				// request.getRequestDispatcher(ajaxLoginSuccessPage).forward(request,
				// response);
				
				String jsonResp = "";
				try {
					vXResponse.setStatusCode(HttpServletResponse.SC_OK);
					vXResponse.setMsgDesc("Login Successful");

					response.setStatus(HttpServletResponse.SC_OK);
					jsonResp = jsonUtil.writeObjectAsString(vXResponse);
					response.getWriter().write(jsonResp);
				} catch (IOException e) {
					logger.info("Error while writing JSON in HttpServletResponse");
				}
				if (logger.isDebugEnabled()) {
					logger.debug("Sending login success response : " + jsonResp);
				}
			    clearAuthenticationAttributes(request);
			} else {
				String jsonResp = "";
				try {
					vXResponse.setStatusCode(HttpServletResponse.SC_OK);
					vXResponse.setMsgDesc("Login Successful");

					response.setStatus(HttpServletResponse.SC_OK);
					jsonResp = jsonUtil.writeObjectAsString(vXResponse);
					response.getWriter().write(jsonResp);
				} catch (IOException e) {
					logger.info("Error while writing JSON in HttpServletResponse");
				}
				if (logger.isDebugEnabled()) {
					logger.debug("Sending login success response : " + jsonResp);
				}
				// super.onAuthenticationSuccess(request, response,
				// authentication);
			}
    	}
    }

}
