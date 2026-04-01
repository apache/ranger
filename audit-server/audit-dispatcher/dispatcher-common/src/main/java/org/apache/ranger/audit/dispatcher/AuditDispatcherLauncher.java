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

package org.apache.ranger.audit.dispatcher;

import org.apache.ranger.audit.server.AuditConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class AuditDispatcherLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(AuditDispatcherLauncher.class);

    private AuditDispatcherLauncher() {
    }

    public static void main(String[] args) {
        try {
            AuditConfig config = new AuditConfig();
            config.addResourceIfReadable("ranger-audit-dispatcher-site.xml");

            String dispatcherType = System.getProperty("ranger.audit.dispatcher.type");
            if (dispatcherType == null) {
                dispatcherType = config.get("ranger.audit.dispatcher.type");
                if (dispatcherType != null) {
                    System.setProperty("ranger.audit.dispatcher.type", dispatcherType);
                }
            }

            if (dispatcherType == null) {
                LOG.error("Dispatcher type is not specified. Cannot determine which dispatcher jars to load.");
                System.exit(1);
            }

            LOG.info("Initializing classloader for dispatcher type: {}", dispatcherType);

            // Build dynamic classpath
            String homeDir = System.getenv("AUDIT_DISPATCHER_HOME_DIR");
            if (homeDir == null) {
                homeDir = System.getProperty("user.dir");
            }

            File dispatcherLibDir = new File(homeDir, "lib/dispatchers/" + dispatcherType);
            List<URL> urls = new ArrayList<>();

            if (dispatcherLibDir.exists() && dispatcherLibDir.isDirectory()) {
                File[] jars = dispatcherLibDir.listFiles((dir, name) -> name.endsWith(".jar"));
                if (jars != null) {
                    for (File jar : jars) {
                        urls.add(jar.toURI().toURL());
                    }
                }
            } else {
                LOG.warn("Dispatcher lib directory does not exist: {}", dispatcherLibDir.getAbsolutePath());
            }

            ClassLoader    parentClassLoader     = AuditDispatcherLauncher.class.getClassLoader();
            URLClassLoader dispatcherClassLoader = new URLClassLoader(urls.toArray(new URL[0]), parentClassLoader);

            Thread.currentThread().setContextClassLoader(dispatcherClassLoader);

            String mainClassName = config.get("ranger.audit.dispatcher.main.class", "org.apache.ranger.audit.dispatcher.AuditDispatcherApplication");

            LOG.info("Launching main class: {}", mainClassName);

            Class<?> mainClass  = Class.forName(mainClassName, true, dispatcherClassLoader);
            Method   mainMethod = mainClass.getMethod("main", String[].class);
            mainMethod.invoke(null, (Object) args);
        } catch (Exception e) {
            LOG.error("Failed to launch audit dispatcher", e);
            System.exit(1);
        }
    }
}
