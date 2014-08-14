package com.xasecure.util;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Component;

import com.xasecure.security.context.XAContextHolder;

@Component
public class RestUtil {

	public static final String timeOffsetCookieName = "clientTimeOffset";

	public static Integer getTimeOffset(HttpServletRequest request) {
		Integer cookieVal = 0;
		try{		
			Cookie[] cookies = request.getCookies();
			String timeOffset = null;
			
			if (cookies != null) {
				for (Cookie cookie : cookies) {
					try {
						if (cookie.getName().equals(timeOffsetCookieName)) {
							timeOffset = cookie.getValue();
							if (timeOffset != null) {
								cookieVal = Integer.parseInt(timeOffset);
							}
							break;
						}
					} catch (Exception ex) {
						cookieVal = 0;
					}
				}
			}
		}catch(Exception ex){
			
		}
		return cookieVal;
	}
	
	public static int getClientTimeOffset(){
		int clientTimeOffsetInMinute = 0;
		try{
			clientTimeOffsetInMinute= XAContextHolder.getSecurityContext().getRequestContext().getClientTimeOffsetInMinute();
		}catch(Exception ex){
			
		}
		if(clientTimeOffsetInMinute==0){
			try{
				clientTimeOffsetInMinute= XAContextHolder.getSecurityContext().getUserSession().getClientTimeOffsetInMinute();
			}catch(Exception ex){
				
			}
		}
		return clientTimeOffsetInMinute;
	}

}