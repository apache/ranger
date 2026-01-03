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

package org.apache.ranger.plugin.classloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class RangerPluginClassLoaderUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPluginClassLoaderUtil.class);

    private static final String RANGER_PLUGIN_LIB_DIR = "ranger-%-plugin-impl";

    private static volatile RangerPluginClassLoaderUtil config;

    public static RangerPluginClassLoaderUtil getInstance() {
        RangerPluginClassLoaderUtil result = config;

        if (result == null) {
            synchronized (RangerPluginClassLoaderUtil.class) {
                result = config;

                if (result == null) {
                    result = new RangerPluginClassLoaderUtil();
                    config = result;
                }
            }
        }

        return result;
    }

    public URL[] getPluginFilesForServiceTypeAndPluginclass(String serviceType, Class<?> pluginClass) throws Exception {
        LOG.debug("==> RangerPluginClassLoaderUtil.getPluginFilesForServiceTypeAndPluginclass({}) Pluging Class :{}", serviceType, pluginClass.getName());

        String[] libDirs = new String[] {getPluginImplLibPath(serviceType, pluginClass)};
        URL[]    ret     = getPluginFiles(libDirs);

        LOG.debug("<== RangerPluginClassLoaderUtil.getPluginFilesForServiceTypeAndPluginclass({}) Pluging Class :{}", serviceType, pluginClass.getName());

        return ret;
    }

    private URL[] getPluginFiles(String[] libDirs) {
        LOG.debug("==> RangerPluginClassLoaderUtil.getPluginFiles()");

        List<URL> ret = new ArrayList<>();

        for (String libDir : libDirs) {
            getFilesInDirectory(libDir, ret);
        }

        LOG.debug("<== RangerPluginClassLoaderUtil.getPluginFilesForServiceType(): {} files", ret.size());

        return ret.toArray(new URL[] {});
    }

    private void getFilesInDirectory(String dirPath, List<URL> files) {
        LOG.debug("==> RangerPluginClassLoaderUtil.getFilesInDirectory({})", dirPath);

        if (dirPath != null) {
            try {
                File[] dirFiles = new File(dirPath).listFiles();

                if (dirFiles != null) {
                    for (File dirFile : dirFiles) {
                        try {
                            if (!dirFile.canRead()) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("getFilesInDirectory('{}'): {} is not readable!", dirPath, dirFile.getAbsolutePath());
                                }
                            }

                            URL jarPath = dirFile.toURI().toURL();

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("getFilesInDirectory('{}'): adding {}", dirPath, dirFile.getAbsolutePath());
                            }

                            files.add(jarPath);
                        } catch (Exception excp) {
                            LOG.warn("getFilesInDirectory('{}'): failed to get URI for file {}", dirPath, dirFile.getAbsolutePath(), excp);
                        }
                    }
                }
            } catch (Exception excp) {
                LOG.warn("getFilesInDirectory('{}'): error", dirPath, excp);
            }
        } else {
            LOG.debug("getFilesInDirectory(dirPath=null, files={}): invalid dirPath - null", files);
        }

        LOG.debug("<== RangerPluginClassLoaderUtil.getFilesInDirectory({})", dirPath);
    }

    private String getPluginImplLibPath(String serviceType, Class<?> pluginClass) throws Exception {
        LOG.debug("==> RangerPluginClassLoaderUtil.getPluginImplLibPath for Class ({})", pluginClass.getName());

        URI    uri  = pluginClass.getProtectionDomain().getCodeSource().getLocation().toURI();
        Path   path = Paths.get(URI.create(uri.toString()));
        String ret  = path.getParent().toString() + File.separatorChar + RANGER_PLUGIN_LIB_DIR.replaceAll("%", serviceType);

        LOG.debug("<== RangerPluginClassLoaderUtil.getPluginImplLibPath for Class ({} PATH :{})", pluginClass.getName(), ret);

        return ret;
    }
}
