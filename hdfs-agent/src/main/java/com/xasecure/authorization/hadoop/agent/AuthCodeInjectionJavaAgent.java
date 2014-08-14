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

package com.xasecure.authorization.hadoop.agent;

import java.lang.instrument.Instrumentation;

public class AuthCodeInjectionJavaAgent {
	public static final String AUTHORIZATION_AGENT_PARAM = "authagent";

	public static void premain(String agentArgs, Instrumentation inst) {
		if (agentArgs != null && AUTHORIZATION_AGENT_PARAM.equalsIgnoreCase(agentArgs.trim())) {
			inst.addTransformer(new HadoopAuthClassTransformer());
		}
	}

}
