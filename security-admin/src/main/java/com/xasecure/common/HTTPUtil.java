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
