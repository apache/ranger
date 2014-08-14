/**
 *
 */
package com.xasecure.security.standalone;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;

import com.xasecure.biz.SessionMgr;
import com.xasecure.common.XAConstants;
import com.xasecure.entity.XXAuthSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.security.access.ConfigAttribute;
import org.springframework.security.access.SecurityConfig;
import org.springframework.security.access.vote.AffirmativeBased;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

@Component
public class StandaloneSecurityHandler {
	public static final String AUTH_MANAGER_BEAN_NAME = "authenticationManager";
	public static final String ACCESS_DECISION_MANAGER_BEAN_NAME = "customAccessDecisionManager";

	@Autowired
	SessionMgr sessionMgr;

	public void login(String userName, String password,
			ApplicationContext context) throws Exception {
		// [1] Create AUTH Token
		Authentication token = new UsernamePasswordAuthenticationToken(
				userName, password);

		// [2] Authenticate User
		AuthenticationManager am = (AuthenticationManager) context
				.getBean(AUTH_MANAGER_BEAN_NAME);
		token = am.authenticate(token);

		// [3] Check User Access
		AffirmativeBased accessDecisionManager = (AffirmativeBased) context
				.getBean(ACCESS_DECISION_MANAGER_BEAN_NAME);
		Collection<ConfigAttribute> list = new ArrayList<ConfigAttribute>();
		SecurityConfig config = new SecurityConfig(XAConstants.ROLE_SYS_ADMIN);
		list.add(config);
		accessDecisionManager.decide(token, null, list);

		// [4] set token in spring context
		SecurityContextHolder.getContext().setAuthentication(token);

		// [5] Process Success login
		InetAddress thisIp = InetAddress.getLocalHost();
		sessionMgr.processStandaloneSuccessLogin(
				XXAuthSession.AUTH_TYPE_PASSWORD, thisIp.getHostAddress());
	}
}
