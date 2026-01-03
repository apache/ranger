/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.authorization.knox;

import org.apache.ranger.plugin.classloader.PluginClassLoaderActivator;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import java.io.IOException;

public class RangerPDPKnoxFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPDPKnoxFilter.class);

    private static final String RANGER_PLUGIN_TYPE                    = "knox";
    private static final String RANGER_PDP_KNOX_FILTER_IMPL_CLASSNAME = "org.apache.ranger.authorization.knox.RangerPDPKnoxFilter";

    private Filter                  rangerPDPKnoxFilteImpl;
    private RangerPluginClassLoader pluginClassLoader;

    public RangerPDPKnoxFilter() {
        LOG.debug("==> RangerPDPKnoxFilter.RangerPDPKnoxFilter()");

        this.init0();

        LOG.debug("<== RangerPDPKnoxFilter.RangerPDPKnoxFilter()");
    }

    @Override
    public void init(FilterConfig fiterConfig) throws ServletException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "init")) {
            rangerPDPKnoxFilteImpl.init(fiterConfig);
        }
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "doFilter")) {
            rangerPDPKnoxFilteImpl.doFilter(servletRequest, servletResponse, filterChain);
        }
    }

    @Override
    public void destroy() {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "destroy")) {
            rangerPDPKnoxFilteImpl.destroy();
        }
    }

    private void init0() {
        LOG.debug("==> RangerPDPKnoxFilter.init0()");

        try {
            pluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<Filter> cls = (Class<Filter>) Class.forName(RANGER_PDP_KNOX_FILTER_IMPL_CLASSNAME, true, pluginClassLoader);

            try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "init0")) {
                rangerPDPKnoxFilteImpl = cls.newInstance();
            }
        } catch (Exception e) {
            LOG.error("Error Enabling RangerKnoxPlugin", e);
        }

        LOG.debug("<== RangerPDPKnoxFilter.init0()");
    }
}
