package com.xasecure.authentication.unix.jaas;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

public class ConsolePromptCallbackHandler implements CallbackHandler {


	@Override
	public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in)) ;
		
		for(Callback cb : callbacks) {
			if (cb instanceof NameCallback) {
		          NameCallback nc = (NameCallback)cb ;
		          System.out.print(nc.getPrompt());
		          System.out.flush();
		          nc.setName(reader.readLine());
			}
			else if (cb instanceof PasswordCallback) {
		          PasswordCallback pc = (PasswordCallback)cb;
		          System.out.print(pc.getPrompt());
		          System.out.flush();
		          pc.setPassword(reader.readLine().toCharArray());				
			}
			else {
				System.out.println("Unknown callbacl [" + cb.getClass().getName() + "]") ;
			}
		}
	}

}
