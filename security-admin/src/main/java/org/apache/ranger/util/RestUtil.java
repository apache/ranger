/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

 package org.apache.ranger.util;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.apache.ranger.security.context.RangerContextHolder;
import org.springframework.stereotype.Component;

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
						if (timeOffsetCookieName.equals(cookie.getName())) {
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
			clientTimeOffsetInMinute= RangerContextHolder.getSecurityContext().getRequestContext().getClientTimeOffsetInMinute();
		}catch(Exception ex){
			
		}
		if(clientTimeOffsetInMinute==0){
			try{
				clientTimeOffsetInMinute= RangerContextHolder.getSecurityContext().getUserSession().getClientTimeOffsetInMinute();
			}catch(Exception ex){
				
			}
		}
		return clientTimeOffsetInMinute;
	}

}