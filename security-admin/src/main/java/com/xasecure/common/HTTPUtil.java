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

 /**
 *
 */
package com.xasecure.common;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Component;

@Component
public class HTTPUtil {

    public static final String USER_AGENT = "User-Agent";

    public static final String IPHONE = "iPhone";
    public static final String IPAD = "iPad";
    public static final String IPOD = "iPod";
    public static final String ANDROID = "Android";

    public int getDeviceType(HttpServletRequest httpRequest) {
	return getDeviceType(httpRequest.getHeader(USER_AGENT));

    }

    public int getDeviceType(String userAgent) {
	if (userAgent == null) {
	    return XACommonEnums.DEVICE_UNKNOWN;
	}

	if (userAgent.contains(IPHONE)) {
	    return XACommonEnums.DEVICE_IPHONE;
	} else if (userAgent.contains(IPAD)) {
	    return XACommonEnums.DEVICE_IPAD;
	} else if (userAgent.contains(IPOD)) {
	    return XACommonEnums.DEVICE_IPOD;
	} else if (userAgent.contains(ANDROID)) {
	    return XACommonEnums.DEVICE_ANDROID;
	} else {
	    return XACommonEnums.DEVICE_BROWSER;
	}
    }

    
}
