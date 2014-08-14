/**
 *
 */
package com.xasecure.util;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.xasecure.common.PropertiesUtil;
import com.xasecure.security.standalone.StandaloneSecurityHandler;

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
