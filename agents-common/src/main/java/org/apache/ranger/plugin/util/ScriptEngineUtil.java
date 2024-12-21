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

package org.apache.ranger.plugin.util;


import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.apache.ranger.plugin.conditionevaluator.RangerScriptConditionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;


public class ScriptEngineUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RangerScriptConditionEvaluator.class);

    private static final String   SCRIPT_ENGINE_CREATOR_NASHHORN = "org.apache.ranger.plugin.util.NashornScriptEngineCreator";
    private static final String   SCRIPT_ENGINE_CREATOR_GRAAL    = "org.apache.ranger.plugin.util.GraalScriptEngineCreator";
    private static final String   SCRIPT_ENGINE_CREATOR_JS       = "org.apache.ranger.plugin.util.JavaScriptEngineCreator";
    private static final String[] SCRIPT_ENGINE_CREATORS         = new String[] {SCRIPT_ENGINE_CREATOR_NASHHORN, SCRIPT_ENGINE_CREATOR_GRAAL, SCRIPT_ENGINE_CREATOR_JS};
    private static final int      JVM_MAJOR_CLASS_VERSION_JDK8   = 52;
    private static final int      JVM_MAJOR_CLASS_VERSION_JDK15  = 59;
    private static final int      JVM_MAJOR_CLASS_VERSION        = getJVMMajorClassVersion();

    private static volatile ScriptEngineCreator SCRIPT_ENGINE_CREATOR             = null;
    private static volatile boolean             SCRIPT_ENGINE_CREATOR_INITIALIZED = false;

    private ScriptEngineUtil() {
        // to block instantiation
    }

    // for backward compatibility with any plugin that might use this API
    public static ScriptEngine createScriptEngine(String engineName, String serviceType) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("ScriptEngineUtil.createScriptEngine(engineName=" + engineName + ", serviceType=" + serviceType + "): engineName ignored");
        }

        return createScriptEngine(serviceType);
    }

    public static ScriptEngine createScriptEngine(String serviceType) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ScriptEngineUtil.createScriptEngine(serviceType=" + serviceType + ")");
        }

        ScriptEngine        ret     = null;
        ScriptEngineCreator creator = getScriptEngineCreator(serviceType);

        if (creator != null) {
            ret = creator.getScriptEngine(null);

            if (ret == null) {
                ClassLoader pluginClsLoader = getPrevActiveClassLoader(serviceType);

                if (pluginClsLoader != null) {
                    ret = creator.getScriptEngine(pluginClsLoader);
                }
            }
        } else {
            LOG.info("createScriptEngine(serviceType={}): no engine creator found", serviceType);
        }

        if (ret == null) {
            LOG.warn("createScriptEngine(serviceType={}): failed to create script engine", serviceType);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== ScriptEngineUtil.createScriptEngine(serviceType={}): ret={}", serviceType, ret);
        }

        return ret;
    }

    private static ScriptEngineCreator getScriptEngineCreator(String serviceType) {
        boolean isInitialized = SCRIPT_ENGINE_CREATOR_INITIALIZED;

        if (!isInitialized) {
            synchronized (ScriptEngineUtil.class) {
                isInitialized = SCRIPT_ENGINE_CREATOR_INITIALIZED;

                if (!isInitialized) {
                    initScriptEngineCreator(serviceType);
                }

                SCRIPT_ENGINE_CREATOR_INITIALIZED = true;
            }
        }

        return SCRIPT_ENGINE_CREATOR;
    }

    private static void initScriptEngineCreator(String serviceType) {
        for (String creatorClsName : SCRIPT_ENGINE_CREATORS) {
            ScriptEngineCreator creator = null;

            try {
                Class<ScriptEngineCreator> creatorClass = (Class<ScriptEngineCreator>) Class.forName(creatorClsName);

                creator = creatorClass.newInstance();
            } catch (Throwable t) {
                boolean logWarn;

                if (creatorClsName.equals(SCRIPT_ENGINE_CREATOR_NASHHORN)) { // not available JDK15 onwards
                    logWarn = JVM_MAJOR_CLASS_VERSION < JVM_MAJOR_CLASS_VERSION_JDK15;
                } else if (creatorClsName.equals(SCRIPT_ENGINE_CREATOR_GRAAL)) { // available only after JDK15 onwards
                    logWarn = JVM_MAJOR_CLASS_VERSION >= JVM_MAJOR_CLASS_VERSION_JDK15;
                } else {
                    logWarn = true;
                }

                if (logWarn) {
                    LOG.warn("initScriptEngineCreator(): failed to instantiate engine creator {}", creatorClsName, t);
                }
            }

            if (creator == null) {
                continue;
            }

            ScriptEngine engine = creator.getScriptEngine(null);

            if (engine == null) {
                ClassLoader prevActiveClassLoader = getPrevActiveClassLoader(serviceType);

                if (prevActiveClassLoader != null) {
                    LOG.debug("initScriptEngineCreator(): trying to create engine using plugin-class-loader for service-type {}", serviceType);

                    engine = creator.getScriptEngine(prevActiveClassLoader);

                    if (engine == null) {
                        LOG.warn("initScriptEngineCreator(): failed to create engine using plugin-class-loader by creator {}", creatorClsName);
                    }
                }
            }

            if (engine != null) {
                SCRIPT_ENGINE_CREATOR = creator;

                break;
            }
        }
    }

    private static int getJVMMajorClassVersion() {
        int ret = JVM_MAJOR_CLASS_VERSION_JDK8;

        try {
            String   javaClassVersion = System.getProperty("java.class.version");
            String[] versionElements  = javaClassVersion != null ? javaClassVersion.split("\\.") : new String[0];

            ret = versionElements.length > 0 ? Integer.parseInt(versionElements[0]) : JVM_MAJOR_CLASS_VERSION_JDK8;
        } catch (Throwable t) {
            // ignore
        }

        return ret;
    }

    private static ClassLoader getPrevActiveClassLoader(String serviceType) {
        ClassLoader ret = null;

        try {
            RangerPluginClassLoader pluginClassLoader = RangerPluginClassLoader.getInstance(serviceType, null);

            if (pluginClassLoader != null) {
                ret = pluginClassLoader.getPrevActiveClassLoader();
            } else {
                LOG.debug("Cannot get plugin-class-loader for serviceType {}", serviceType);
            }
        } catch (Throwable excp) {
            LOG.debug("Failed to get plugin-class-loader for serviceType {}", serviceType, excp);
        }

        return ret;
    }
}
