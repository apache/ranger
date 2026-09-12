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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class NashornScriptEngineCreator implements ScriptEngineCreator {
    private static final Logger LOG = LoggerFactory.getLogger(NashornScriptEngineCreator.class);

    private static final String[] SCRIPT_ENGINE_ARGS = new String[] { "--no-java", "--no-syntax-extensions" };
    private static final String   ENGINE_NAME        = "NashornScriptEngine";
    private static final String   NASHORN_FACTORY    = "jdk.nashorn.api.scripting.NashornScriptEngineFactory";
    private static final String   CLASS_FILTER       = "jdk.nashorn.api.scripting.ClassFilter";

    @Override
    public ScriptEngine getScriptEngine(ClassLoader clsLoader) {
        ScriptEngine ret = null;

        if (clsLoader == null) {
            clsLoader = getDefaultClassLoader();
        }

        try {
            Class<?> classFilterType = Class.forName(CLASS_FILTER, true, clsLoader);
            Class<?> factoryClass    = Class.forName(NASHORN_FACTORY, true, clsLoader);
            Object   classFilter     = Proxy.newProxyInstance(clsLoader, new Class<?>[] { classFilterType }, RangerClassFilterHandler.INSTANCE);
            Object   factory         = factoryClass.getDeclaredConstructor().newInstance();
            Method   getScriptEngine = factoryClass.getMethod("getScriptEngine", String[].class, ClassLoader.class, classFilterType);

            ret = (ScriptEngine) getScriptEngine.invoke(factory, SCRIPT_ENGINE_ARGS, clsLoader, classFilter);
        } catch (Throwable t) {
            LOG.debug("NashornScriptEngineCreator.getScriptEngine(): failed to create engine type {}", ENGINE_NAME, t);
        }

        return ret;
    }

    private static final class RangerClassFilterHandler implements InvocationHandler {
        static final RangerClassFilterHandler INSTANCE = new RangerClassFilterHandler();

        private RangerClassFilterHandler() {
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            if ("exposeToScripts".equals(method.getName()) && args != null && args.length == 1 && args[0] instanceof String) {
                LOG.warn("script blocked: attempt to use Java class {}", args[0]);
                return Boolean.FALSE;
            }

            return null;
        }
    }
}
