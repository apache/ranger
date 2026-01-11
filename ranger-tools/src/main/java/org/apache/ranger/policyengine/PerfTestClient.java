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

package org.apache.ranger.policyengine;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PerfTestClient extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(PerfTestClient.class);

    private static final Gson gson;

    final PerfTestEngine perfTestEngine;
    final int            clientId;
    final URL            requestFileURL;
    final int            maxCycles;

    List<RequestData> requests;

    public PerfTestClient(final PerfTestEngine perfTestEngine, final int clientId, final URL requestFileURL, final int maxCycles) {
        LOG.debug("==> PerfTestClient(clientId={}, maxCycles={})", clientId, maxCycles);

        this.perfTestEngine = perfTestEngine;
        this.clientId       = clientId;
        this.requestFileURL = requestFileURL;
        this.maxCycles      = maxCycles;

        setName("PerfTestClient-" + clientId);
        setDaemon(true);

        LOG.debug("<== PerfTestClient(clientId={}, maxCycles={})", clientId, maxCycles);
    }

    public boolean init() {
        LOG.debug("==> init()");

        boolean ret    = false;

        try (InputStream in = requestFileURL.openStream()) {
            try (Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
                Type listType = new TypeToken<List<RequestData>>() {}.getType();

                requests = gson.fromJson(reader, listType);

                ret = true;
            }
        } catch (Exception excp) {
            LOG.error("Error opening request data stream or loading load request data from file, URL={}", requestFileURL, excp);
        }

        LOG.debug("<== init() : {}", ret);

        return ret;
    }

    @Override
    public void run() {
        LOG.debug("==> run()");

        try {
            for (int i = 0; i < maxCycles; i++) {
                for (RequestData data : requests) {
                    data.setResult(perfTestEngine.execute(data.getRequest()));
                }
            }
        } catch (Exception excp) {
            LOG.error("PerfTestClient.run() : interrupted! Exiting thread", excp);
        }

        LOG.debug("<== run()");
    }

    private static class RequestData {
        private String              name;
        private RangerAccessRequest request;
        private RangerAccessResult  result;

        public RequestData() {
            this(null, null, null);
        }

        public RequestData(String name, RangerAccessRequest request, RangerAccessResult result) {
            setName(name);
            setRequest(request);
            setResult(result);
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public RangerAccessRequest getRequest() {
            return request;
        }

        public void setRequest(RangerAccessRequest request) {
            this.request = request;
        }

        public RangerAccessResult getResult() {
            return result;
        }

        public void setResult(RangerAccessResult result) {
            this.result = result;
        }
    }

    static {
        GsonBuilder builder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z");
        gson = builder
                .setPrettyPrinting()
                .registerTypeAdapter(RangerAccessRequest.class, new RangerAccessRequestDeserializer(builder))
                .registerTypeAdapter(RangerAccessResource.class, new RangerResourceDeserializer(builder))
                .create();
    }
}
