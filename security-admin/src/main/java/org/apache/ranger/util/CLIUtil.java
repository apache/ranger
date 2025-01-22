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

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.apache.ranger.security.standalone.StandaloneSecurityHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import java.util.Locale;

/**
 *
 */
@Component
public class CLIUtil {
    private static final Logger             logger                         = LoggerFactory.getLogger(CLIUtil.class);
    private static final String             JAVA_PATCHES_CLASS_NAME_PREFIX = "Patch";
    static               ApplicationContext context;
    @Autowired
    StandaloneSecurityHandler securityHandler;

    public static void init() {
        if (context == null) {
            context = new ClassPathXmlApplicationContext("applicationContext.xml", "security-applicationContext.xml", "asynctask-applicationContext.xml");
        }
    }

    public static Object getBean(Class<?> beanClass) {
        init();
        checkIfJavaPatchesExecuting(beanClass);
        return context.getBean(beanClass);
    }

    public static String getMessage(String messagekey, HttpServletRequest request) {
        ServletContext     servletContext = request.getSession().getServletContext();
        ApplicationContext ctx            = WebApplicationContextUtils.getWebApplicationContext(servletContext);
        Object[]           args           = new Object[] {};
        String             messageValue   = ctx != null ? ctx.getMessage(messagekey, args, Locale.getDefault()) : "";
        return messageValue;
    }

    public void authenticate() throws Exception {
        String user = PropertiesUtil.getProperty("xa.cli.user");
        String pwd  = PropertiesUtil.getProperty("xa.cli.password");
        logger.info("Authenticating user: {}", user);
        securityHandler.login(user, pwd, context);
    }

    private static void checkIfJavaPatchesExecuting(Class<?> beanClass) {
        if (beanClass != null) {
            final String className = beanClass.getSimpleName();
            if (StringUtils.isNotEmpty(className)) {
                if (className.startsWith(JAVA_PATCHES_CLASS_NAME_PREFIX)) {
                    UserSessionBase userSessBase = new UserSessionBase();
                    userSessBase.setUserAdmin(true);
                    userSessBase.setAuditUserAdmin(true);
                    userSessBase.setKeyAdmin(true);
                    userSessBase.setAuditKeyAdmin(true);
                    RangerSecurityContext rangerSecCtx = new RangerSecurityContext();
                    rangerSecCtx.setUserSession(userSessBase);
                    RangerContextHolder.setSecurityContext(rangerSecCtx);
                }
            }
        }
    }
}
