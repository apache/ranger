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
package org.apache.ranger.util;

import org.apache.log4j.Logger;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.security.standalone.StandaloneSecurityHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

/**
 * 
 *
 */
@Component
public class CLIUtil {
	static Logger logger = Logger.getLogger(CLIUtil.class);

	@Autowired
	StandaloneSecurityHandler securityHandler;

	static ApplicationContext context = null;

	public static void init() {
		if (context == null) {
			context = new ClassPathXmlApplicationContext(
					"applicationContext.xml",
					"security-applicationContext.xml",
					"asynctask-applicationContext.xml");
		}
	}

	public static Object getBean(Class<?> beanClass) {
		init();
		return context.getBean(beanClass);
	}

	public void authenticate() throws Exception {
		String user = PropertiesUtil.getProperty("xa.cli.user");
		String pwd = PropertiesUtil.getProperty("xa.cli.password");
		logger.info("Authenticating user:" + user);
		securityHandler.login(user, pwd, context);
	}

}
