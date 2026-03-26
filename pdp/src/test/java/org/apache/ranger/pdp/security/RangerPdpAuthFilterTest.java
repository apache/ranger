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

package org.apache.ranger.pdp.security;

import org.junit.jupiter.api.Test;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RangerPdpAuthFilterTest {
    @Test
    public void testInit_skipsHeaderHandlerWhenDisabled() {
        RangerPdpAuthFilter filter = new RangerPdpAuthFilter();
        Map<String, String> params = new HashMap<>();

        params.put(RangerPdpAuthFilter.PARAM_AUTH_TYPES, "header");
        params.put(RangerPdpAuthFilter.PARAM_HEADER_AUTHN_ENABLED, "false");

        assertThrows(ServletException.class, () -> filter.init(new TestFilterConfig(params)));
    }

    @Test
    public void testInit_registersHeaderHandlerWhenEnabled() throws Exception {
        RangerPdpAuthFilter filter = new RangerPdpAuthFilter();
        Map<String, String> params = new HashMap<>();

        params.put(RangerPdpAuthFilter.PARAM_AUTH_TYPES, "header");
        params.put(RangerPdpAuthFilter.PARAM_HEADER_AUTHN_ENABLED, "true");

        filter.init(new TestFilterConfig(params));

        Field handlersField = RangerPdpAuthFilter.class.getDeclaredField("handlers");

        handlersField.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<PdpAuthHandler> handlers = (List<PdpAuthHandler>) handlersField.get(filter);

        assertEquals(1, handlers.size());
        assertEquals(HttpHeaderAuthHandler.class, handlers.get(0).getClass());
    }

    private static final class TestFilterConfig implements FilterConfig {
        private final Map<String, String> initParams;

        private TestFilterConfig(Map<String, String> initParams) {
            this.initParams = initParams;
        }

        @Override
        public String getFilterName() {
            return "testFilter";
        }

        @Override
        public ServletContext getServletContext() {
            return null;
        }

        @Override
        public String getInitParameter(String name) {
            return initParams.get(name);
        }

        @Override
        public Enumeration<String> getInitParameterNames() {
            return Collections.enumeration(initParams.keySet());
        }
    }
}
