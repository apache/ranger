/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key.kms.server;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestKMSMDCFilter {
    @Test
    public void testInit() throws ServletException {
        KMSMDCFilter kmsMDCFilter = new KMSMDCFilter();
        FilterConfig config       = mock(FilterConfig.class);
        kmsMDCFilter.init(config);
    }

    @Test
    public void testDestroy() {
        KMSMDCFilter kmsMDCFilter = new KMSMDCFilter();
        kmsMDCFilter.destroy();
    }

    @Test
    public void testDoFilter_withKmsApiPath() throws Exception {
        KMSMDCFilter filter = new KMSMDCFilter();

        HttpServletRequest  request  = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain         chain    = mock(FilterChain.class);

        when(request.getRequestURI()).thenReturn("/kms/api/status");

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
    }

    @Test
    public void testDoFilter_withOtherPath() throws Exception {
        KMSMDCFilter filter = new KMSMDCFilter();

        HttpServletRequest  request  = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain         chain    = mock(FilterChain.class);

        when(request.getRequestURI()).thenReturn("/kms/v1/keys");
        when(request.getMethod()).thenReturn("GET");
        when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost/kms/v1/keys"));
        when(request.getQueryString()).thenReturn(null);

        try {
            filter.doFilter(request, response, chain);
        } catch (Exception e) {
            // optional logging/assertion
        }

        verify(chain).doFilter(request, response);
    }
}
