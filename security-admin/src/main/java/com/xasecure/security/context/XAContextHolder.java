/**
 *
 */
package com.xasecure.security.context;

public class XAContextHolder {

    private static final ThreadLocal<XASecurityContext> securityContextThreadLocal = new ThreadLocal<XASecurityContext>();

    private XAContextHolder() {

    }

    public static XASecurityContext getSecurityContext(){
	return securityContextThreadLocal.get();
    }

    public static void setSecurityContext(XASecurityContext context){
	securityContextThreadLocal.set(context);
    }

    public static void resetSecurityContext(){
	securityContextThreadLocal.remove();
    }

}
