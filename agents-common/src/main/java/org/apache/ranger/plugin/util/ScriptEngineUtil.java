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
import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;

public class ScriptEngineUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RangerScriptConditionEvaluator.class);

    private static final String[] SCRIPT_ENGINE_ARGS = new String[0];

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

        ScriptEngine ret = getScriptEngine(null);

        if (ret != null) {
            LOG.debug("Created script-engine in current class-loader");
        } else {
            LOG.warn("Failed to create script-engine in current class-loader. Will try plugin-class-loader for service-type:[" + serviceType + "]");

            ClassLoader prevActiveClassLoader = null;

            try {
                RangerPluginClassLoader pluginClassLoader = RangerPluginClassLoader.getInstance(serviceType, null);

                if (pluginClassLoader != null) {
                    prevActiveClassLoader = pluginClassLoader.getPrevActiveClassLoader();
                } else {
                    LOG.error("Cannot get script-engine from null plugin-class-loader");
                }
            } catch (Throwable exp) {
                LOG.error("RangerScriptConditionEvaluator.init() failed", exp);
            }

            if (prevActiveClassLoader != null) {
                ret = getScriptEngine(prevActiveClassLoader);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== ScriptEngineUtil.createScriptEngine(serviceType=" + serviceType + ") : ret=" + ret);
        }
        return ret;
    }

    private static ScriptEngine getScriptEngine(ClassLoader clsLoader) {
        ScriptEngine ret;

        try {
            final NashornScriptEngineFactory factory = new NashornScriptEngineFactory();

            if (clsLoader != null) {
                ret = factory.getScriptEngine(SCRIPT_ENGINE_ARGS, clsLoader, RangerClassFilter.INSTANCE);
            } else {
                ret = factory.getScriptEngine(RangerClassFilter.INSTANCE);
            }
        } catch (Throwable t) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ScriptEngineUtil.getScriptEngine(clsLoader={}): failed", clsLoader, t);
            }

            ret = null;
        }

        return ret;
    }

    private static class RangerClassFilter implements ClassFilter {
        static final RangerClassFilter INSTANCE = new RangerClassFilter();

        private RangerClassFilter() {
        }

        @Override
        public boolean exposeToScripts(String className) {
            LOG.warn("script blocked: attempt to use Java class {}", className);

            return false;
        }
    }
}
