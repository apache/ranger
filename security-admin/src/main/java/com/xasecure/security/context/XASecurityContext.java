/**
 *
 */
package com.xasecure.security.context;

import java.io.Serializable;

import com.xasecure.common.RequestContext;
import com.xasecure.common.UserSessionBase;

public class XASecurityContext implements Serializable{
    private static final long serialVersionUID = 1L;
    private UserSessionBase userSession;
    private RequestContext requestContext;

    public UserSessionBase getUserSession() {
        return userSession;
    }

    public void setUserSession(UserSessionBase userSession) {
        this.userSession = userSession;
    }

    /**
     * @return the requestContext
     */
    public RequestContext getRequestContext() {
        return requestContext;
    }

    /**
     * @param requestContext the requestContext to set
     */
    public void setRequestContext(RequestContext requestContext) {
        this.requestContext = requestContext;
    }


}
