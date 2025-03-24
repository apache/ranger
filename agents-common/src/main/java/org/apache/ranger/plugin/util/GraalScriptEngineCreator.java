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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class GraalScriptEngineCreator implements ScriptEngineCreator {
    private static final Logger LOG = LoggerFactory.getLogger(GraalScriptEngineCreator.class);

    private static final String ENGINE_NAME            = "graal.js";
    private static final String CONFIG_PREDIX_JVM      = "polyglot";
    private static final String CONFIG_PREDIX_PLUGIN   = "ranger.plugin.script.";
    private static final String CONFIG_JAVA_CLASS_PATH = "java.class.path";

    // instance variables
    private final Map<String, Boolean> graalVmConfigs = new HashMap<>();

    // default constructor
    public GraalScriptEngineCreator() {
        Map<String, Boolean> graalVmConfigsDefault = new HashMap<>(4); //setting smallest size, which is big enough to avoid expand
        Configuration        configuration         = new Configuration();
        FilenameFilter       fileNameFilter        = (dir, name) -> name.startsWith("ranger-") && name.endsWith("security.xml");

        graalVmConfigsDefault.put("polyglot.js.allowHostAccess", Boolean.TRUE); //default is true for backward(Nashorn) compatibility
        graalVmConfigsDefault.put("polyglot.js.nashorn-compat", Boolean.TRUE);  //default is true for backward(Nashorn) compatibility

        for (String file: findFiles(fileNameFilter)) {
            configuration.addResource(new Path(file));
        }

        graalVmConfigs.putAll(getGraalVmConfigs(configuration, graalVmConfigsDefault));
    }

    public ScriptEngine getScriptEngine(ClassLoader clsLoader) {
        ScriptEngine ret = null;

        if (clsLoader == null) {
            clsLoader = getDefaultClassLoader();
        }

        try {
            ScriptEngineManager mgr = new ScriptEngineManager(clsLoader);

            ret = mgr.getEngineByName(ENGINE_NAME);

            if (ret != null) {
                // enable configured script features
                Bindings bindings = ret.getBindings(ScriptContext.ENGINE_SCOPE);
                bindings.putAll(graalVmConfigs);
                ret.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
            }
        } catch (Throwable t) {
            LOG.debug("GraalScriptEngineCreator.getScriptEngine(): failed to create engine type {}", ENGINE_NAME, t);
        }

        if (ret == null) {
            LOG.debug("GraalScriptEngineCreator.getScriptEngine(): failed to create engine type {}", ENGINE_NAME);
        }
        return ret;
    }

    private Map<String, Boolean> getGraalVmConfigs(Configuration configuration, Map<String, Boolean> graalVmConfigsDefault) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("===>> GraalScriptEngineCreator.getGraalVmConfigs()");
        }

        Map<String, Boolean> ret = new HashMap<>();

        // set configs from ranger security config values, if present
        for (Map.Entry<String, String> entry : configuration.getPropsWithPrefix(CONFIG_PREDIX_PLUGIN).entrySet()) {
            String key   = entry.getKey();
            String value = entry.getValue();

            if (StringUtils.isNotBlank(value)) {
                ret.put(key, Boolean.valueOf(value));
            }
        }

        // add JVM options if not already set
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            if (entry.getKey().toString().startsWith(CONFIG_PREDIX_JVM)) {
                String key   = entry.getKey().toString();
                String value = entry.getValue().toString();

                if (StringUtils.isNotBlank(value) && ret.get(key) == null) {
                    ret.put(key, Boolean.valueOf(value));
                }
            }
        }

        // add default values if not already set
        for (Map.Entry<String, Boolean> entry : graalVmConfigsDefault.entrySet()) {
            String key   = entry.getKey();
            Boolean value = entry.getValue();

            ret.putIfAbsent(key, value);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<<=== GraalScriptEngineCreator.getGraalVmConfigs(): ret={}", ret);
        }

        return ret;
    }

    private Set<String> findFiles(FilenameFilter filenameFilter) {
        String classPath = System.getProperty(CONFIG_JAVA_CLASS_PATH);
        List<String> configDirs = new ArrayList<>(5);
        Set<String> ret = new HashSet<>();

        for (String path : classPath.split(":")) {
            if (!path.endsWith("jar")) {  //ignore jars
                if (path.endsWith("/")) {
                    path = path.substring(0, path.length() - 1);
                }
                configDirs.add(path);
            }
        }

        for (String configDir : configDirs) {
            File confDir = new File(configDir);
            if (confDir.isDirectory()) {
                for (File file : Objects.requireNonNull(confDir.listFiles(filenameFilter))) {
                    ret.add(file.getAbsolutePath());
                }
            }
        }
        return ret;
    }
}
