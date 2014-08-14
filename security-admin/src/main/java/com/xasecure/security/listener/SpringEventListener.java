package com.xasecure.security.listener;

import com.xasecure.biz.SessionMgr;
import com.xasecure.entity.XXAuthSession;

import org.apache.log4j.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.security.authentication.event.AbstractAuthenticationEvent;
import org.springframework.security.authentication.event.AuthenticationFailureBadCredentialsEvent;
import org.springframework.security.authentication.event.AuthenticationFailureDisabledEvent;
import org.springframework.security.authentication.event.AuthenticationSuccessEvent;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.WebAuthenticationDetails;


public class SpringEventListener implements
	ApplicationListener<AbstractAuthenticationEvent> {

    static Logger logger = Logger.getLogger(SpringEventListener.class);

    @Autowired
    SessionMgr sessionMgr;

    @Override
    public void onApplicationEvent(AbstractAuthenticationEvent event) {
	try {
	    if (event instanceof AuthenticationSuccessEvent) {
		process((AuthenticationSuccessEvent) event);
	    } else if (event instanceof AuthenticationFailureBadCredentialsEvent) {
		process((AuthenticationFailureBadCredentialsEvent) event);
	    } else if (event instanceof AuthenticationFailureDisabledEvent) {
		process((AuthenticationFailureDisabledEvent) event);
	    } else {
		// igonre all other events
	    }

	} catch (Exception e) {
	    logger.error("Exception in Spring Event Listener.", e);
	}
    }

    protected void process(AuthenticationSuccessEvent authSuccessEvent) {
	Authentication auth = authSuccessEvent.getAuthentication();
	WebAuthenticationDetails details = (WebAuthenticationDetails) auth
		.getDetails();
	String remoteAddress = details != null ? details.getRemoteAddress()
		: "";
	String sessionId = details != null ? details.getSessionId() : "";

	logger.info("Login Successful:" + auth.getName() + " | Ip Address:"
		+ remoteAddress + " | sessionId=" + sessionId);

	// success logins are processed further in
	// AKASecurityContextFormationFilter
    }

    protected void process(
	    AuthenticationFailureBadCredentialsEvent authFailEvent) {
	Authentication auth = authFailEvent.getAuthentication();
	WebAuthenticationDetails details = (WebAuthenticationDetails) auth
		.getDetails();
	String remoteAddress = details != null ? details.getRemoteAddress()
		: "";
	String sessionId = details != null ? details.getSessionId() : "";

	logger.info("Login Unsuccessful:" + auth.getName() + " | Ip Address:"
		+ remoteAddress + " | Bad Credentials");

	sessionMgr.processFailureLogin(
		XXAuthSession.AUTH_STATUS_WRONG_PASSWORD,
		XXAuthSession.AUTH_TYPE_PASSWORD, auth.getName(),
		remoteAddress, sessionId);
    }

    protected void process(AuthenticationFailureDisabledEvent authFailEvent) {
	Authentication auth = authFailEvent.getAuthentication();
	WebAuthenticationDetails details = (WebAuthenticationDetails) auth
		.getDetails();
	String remoteAddress = details != null ? details.getRemoteAddress()
		: "";
	String sessionId = details != null ? details.getSessionId() : "";

	logger.info("Login Unsuccessful:" + auth.getName() + " | Ip Address:"
		+ remoteAddress);

	sessionMgr.processFailureLogin(XXAuthSession.AUTH_STATUS_DISABLED,
		XXAuthSession.AUTH_TYPE_PASSWORD, auth.getName(),
		remoteAddress, sessionId);

    }

}
