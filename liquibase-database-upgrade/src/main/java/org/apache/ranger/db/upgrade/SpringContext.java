/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.db.upgrade;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringContext {
    private static final Logger             logger = LoggerFactory.getLogger(SpringContext.class);
    private static       ApplicationContext context;

    private SpringContext() {
    }

    public static void init() {
        if (context == null) {
            logger.info("context is null, re-initializing from ApplicationContext files");
            String contextFilesString = System.getProperty("applicationContext.files");
            if (contextFilesString != null) {
                logger.info("ApplicationContext Files={}", contextFilesString);
                String[] contextFiles = contextFilesString.split(",");
                context = new ClassPathXmlApplicationContext(contextFiles);
            } else {
                logger.info("No applicationContext files found in system property params. Set -DapplicationContext.files=file1,file2,file3 in the java command");
                logger.info("Creating application context from annotations only");
                context = new AnnotationConfigApplicationContext("org.apache.ranger");
            }
        } else {
            logger.info("context not null");
        }
        if (logger.isDebugEnabled()) {
            if (context != null) {
                String[] beanNames = context.getBeanDefinitionNames();
                logger.debug("Beans initialized by Spring from classpath:");
                for (String beanName : beanNames) {
                    logger.debug(beanName);
                }
            } else {
                logger.debug("context is null");
            }
        }
    }

    public static <T> T getBean(Class<T> beanClass) {
        if (context == null) {
            logger.info("SpringContext.init()");
            init();
        }
        if (context != null) {
            logger.info("SpringContext.context not null");
            return context.getBean(beanClass);
        } else {
            logger.info("SpringContext.context is null");
            return null;
        }
    }

    public static <T> T getBean(String beanName) {
        if (context == null) {
            logger.info("SpringContext.init()");
            init();
        }
        if (context != null) {
            logger.info("SpringContext.context not null");
            return (T) context.getBean(beanName);
        } else {
            logger.info("SpringContext.context is null");
            return null;
        }
    }
}
