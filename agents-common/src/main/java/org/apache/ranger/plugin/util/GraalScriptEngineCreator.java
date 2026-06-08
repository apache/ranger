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

import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.function.Predicate;

public class GraalScriptEngineCreator implements ScriptEngineCreator {
    private static final Logger LOG = LoggerFactory.getLogger(GraalScriptEngineCreator.class);

    private static final String ENGINE_NAME = "graal.js";
    private static final String CLS_HOST_ACCESS = "org.graalvm.polyglot.HostAccess";
    private static final String CLS_HOST_ACCESS_BUILDER = "org.graalvm.polyglot.HostAccess$Builder";
    private static final String CLS_CONTEXT = "org.graalvm.polyglot.Context";
    private static final String CLS_CONTEXT_BUILDER = "org.graalvm.polyglot.Context$Builder";
    private static final String CLS_ENGINE = "org.graalvm.polyglot.Engine";
    private static final String CLS_GRAAL_JS_ENGINE = "com.oracle.truffle.js.scriptengine.GraalJSScriptEngine";

    // Script-API classes whose public instance methods are exported to scripts.
    // getDeclaredMethods() returns only methods declared directly in each class,
    // NOT inherited ones (e.g. Object.getClass()) — so the reflection chain is
    // closed without needing @HostAccess.Export annotations on those classes.
    private static final String[] SCRIPT_API_CLASSES = new String[] {
            "org.apache.ranger.plugin.policyengine.RangerRequestScriptEvaluator",
            "org.apache.ranger.plugin.contextenricher.RangerTagForEval"
    };
    private final Method createMethod;
    private final Object ctxBuilder;

    public GraalScriptEngineCreator() {
        Method createMeth = null;
        Object builder = null;
        try {
            Object hostAccess = buildHostAccess();
            builder = buildContextBuilder(hostAccess);
            Class<?> engineCls = Class.forName(CLS_ENGINE);
            Class<?> graalJsCls = Class.forName(CLS_GRAAL_JS_ENGINE);
            Class<?> ctxBldCls = Class.forName(CLS_CONTEXT_BUILDER);
            createMeth = graalJsCls.getMethod("create", engineCls, ctxBldCls);
        } catch (Throwable t) {
            LOG.warn("GraalScriptEngineCreator(): failed to initialize", t);
        } finally {
            this.createMethod = createMeth;
            this.ctxBuilder = builder;
        }
    }

    public ScriptEngine getScriptEngine(ClassLoader clsLoader) {
        ScriptEngine ret = null;
        try {
            if (createMethod != null && ctxBuilder != null) {
                ret = (ScriptEngine) createMethod.invoke(null, null, ctxBuilder);
            }
        } catch (Throwable t) {
            LOG.debug("GraalScriptEngineCreator.getScriptEngine(): failed to create engine type {}", ENGINE_NAME, t);
        }

        if (ret == null) {
            LOG.debug("GraalScriptEngineCreator.getScriptEngine(): failed to create engine type {}", ENGINE_NAME);
        }
        return ret;
    }

    private Object buildHostAccess() throws Exception {
        Class<?> hostAccessCls = Class.forName(CLS_HOST_ACCESS);
        Class<?> haBuilderCls = Class.forName(CLS_HOST_ACCESS_BUILDER);
        Object haBuilder = hostAccessCls.getMethod("newBuilder").invoke(null);
        Method allowAccessMeth = haBuilderCls.getMethod("allowAccess", Executable.class);
        for (String className : SCRIPT_API_CLASSES) {
            try {
                for (Method m : Class.forName(className).getDeclaredMethods()) {
                    if (Modifier.isPublic(m.getModifiers()) && !Modifier.isStatic(m.getModifiers())) {
                        allowAccessMeth.invoke(haBuilder, m);
                    }
                }
            } catch (ClassNotFoundException e) {
                LOG.warn("GraalScriptEngineCreator.buildHostAccess(): class not found: {}", className);
            }
        }
        return haBuilderCls.getMethod("build").invoke(haBuilder);
    }

    private Object buildContextBuilder(Object hostAccess) throws Exception {
        Class<?> hostAccessCls = Class.forName(CLS_HOST_ACCESS);
        Class<?> contextCls = Class.forName(CLS_CONTEXT);
        Class<?> ctxBuilderCls = Class.forName(CLS_CONTEXT_BUILDER);
        Object builder = contextCls.getMethod("newBuilder", String[].class).invoke(null, new Object[] {new String[] {"js"}});
        builder = ctxBuilderCls.getMethod("allowExperimentalOptions", boolean.class).invoke(builder, true);
        builder = ctxBuilderCls.getMethod("allowAllAccess", boolean.class).invoke(builder, false);
        builder = ctxBuilderCls.getMethod("allowHostAccess", hostAccessCls).invoke(builder, hostAccess);
        builder = ctxBuilderCls.getMethod("allowHostClassLookup", Predicate.class).invoke(builder, (Predicate<String>) s -> false);
        builder = ctxBuilderCls.getMethod("allowHostClassLoading", boolean.class).invoke(builder, false);
        builder = ctxBuilderCls.getMethod("allowCreateThread", boolean.class).invoke(builder, false);
        builder = ctxBuilderCls.getMethod("allowNativeAccess", boolean.class).invoke(builder, false);
        return builder;
    }
}
