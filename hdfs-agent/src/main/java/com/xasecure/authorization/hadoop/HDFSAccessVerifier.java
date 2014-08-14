/**************************************************************************
 *                                                                        *
 * The information in this document is proprietary to XASecure Inc.,      *
 * It may not be used, reproduced or disclosed without the written        *
 * approval from the XASecure Inc.,                                       *
 *                                                                        *
 * PRIVILEGED AND CONFIDENTIAL XASECURE PROPRIETARY INFORMATION           *
 *                                                                        *
 * Copyright (c) 2013 XASecure, Inc.  All rights reserved.                *
 *                                                                        *
 *************************************************************************/

 /**
  *
  *	@version: 1.0.004
  *
  */

package com.xasecure.authorization.hadoop;

import java.util.Set;

public interface HDFSAccessVerifier {
	public class AccessContext {
		String agentId;
		int repositoryType;
		String sessionId;
		String clientType;
		String clientIP;
		String requestData;
	}
	
	public boolean isAccessGranted(String aPathName, String aPathOwnerName, String access, String username, Set<String> groups);
	public boolean isAuditLogEnabled(String aPathName) ;
}
