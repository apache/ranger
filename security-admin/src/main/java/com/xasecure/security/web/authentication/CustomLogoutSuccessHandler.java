package com.xasecure.security.web.authentication;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;
import org.springframework.security.web.authentication.logout.SimpleUrlLogoutSuccessHandler;

import com.xasecure.common.JSONUtil;
import com.xasecure.view.VXResponse;

public class CustomLogoutSuccessHandler extends SimpleUrlLogoutSuccessHandler
		implements LogoutSuccessHandler {

	@Autowired
	JSONUtil jsonUtil;

	@Override
	public void onLogoutSuccess(HttpServletRequest request,
			HttpServletResponse response, Authentication authentication)
			throws IOException, ServletException {

		response.setContentType("application/json;charset=UTF-8");
		response.setHeader("Cache-Control", "no-cache");
		String jsonStr = "";
		try {
			VXResponse vXResponse = new VXResponse();
			vXResponse.setStatusCode(HttpServletResponse.SC_OK);
			vXResponse.setMsgDesc("Logout Successful");
			jsonStr = jsonUtil.writeObjectAsString(vXResponse);

			response.setStatus(HttpServletResponse.SC_OK);
			response.getWriter().write(jsonStr);
			
			if (logger.isDebugEnabled()) {
				logger.debug("Log-out Successfully done. Returning Json : " +jsonStr);
			}
			
		} catch (IOException e) {
			logger.info("Error while writing JSON in HttpServletResponse");
		}
	}

}
