package com.xasecure.test.authentication;

import javax.security.auth.login.LoginContext;

public class UnixAuthenticationTester {

	public static void main(String[] args) throws Throwable {
		new UnixAuthenticationTester().run();
	}
		
	public void run() throws Throwable {
		LoginContext loginContext =  new LoginContext("PolicyManager") ;
		System.err.println("After login ...") ;
		loginContext.login(); 
		System.err.println("Subject:" + loginContext.getSubject() ) ;
		loginContext.logout(); 
	}

}
