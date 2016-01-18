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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.util.PerfDataRecorder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RangerPolicyenginePerfTester {
    static final Log LOG = LogFactory.getLog(RangerPolicyenginePerfTester.class);

    public static void main(String[] args) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyenginePerfTester.main()");
        }

        CommandLineParser commandLineParser = new CommandLineParser();

        PerfTestOptions perfTestOptions = commandLineParser.parse(args);

        if (perfTestOptions != null) {
            URL statCollectionFileURL = perfTestOptions.getStatCollectionFileURL();

            List<String> perfModuleNames = statCollectionFileURL != null ? buildPerfModuleNames(statCollectionFileURL) : new ArrayList<String>();

            PerfDataRecorder.initialize(perfModuleNames);

            URL servicePoliciesFileURL = perfTestOptions.getServicePoliciesFileURL();

            PerfTestEngine perfTestEngine = new PerfTestEngine(servicePoliciesFileURL);
            if (!perfTestEngine.init()) {
                LOG.error("Error initializing test data. Existing...");
                System.exit(1);
            }

            URL[] requestFileURLs = perfTestOptions.getRequestFileURLs();
            int requestFilesCount = requestFileURLs.length;

            int clientsCount = perfTestOptions.getConcurrentClientCount();
            List<PerfTestClient> perfTestClients = new ArrayList<PerfTestClient>(clientsCount);

            for (int i = 0; i < clientsCount; i++) {

                URL requestFileURL = requestFileURLs[i % requestFilesCount];

                PerfTestClient perfTestClient = new PerfTestClient(perfTestEngine, i, requestFileURL, perfTestOptions.getIterationsCount());

                if (!perfTestClient.init()) {
                    LOG.error("Error initializing PerfTestClient: (id=" + i + ")");
                } else {
                    perfTestClients.add(perfTestClient);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Number of perfTestClients=" + perfTestClients.size());
            }

            for (PerfTestClient client : perfTestClients) {
                try {
                    client.start();
                } catch (Throwable t) {
                    LOG.error("Error in starting client: " + client.getName(), t);
                }
            }

            LOG.info("Waiting for " + perfTestClients.size() + " clients to finish up");

            for (PerfTestClient client : perfTestClients) {
                try {
                    if (client.isAlive()) {
                        LOG.info("Waiting for " + client.getName() + " to finish up.");
                        client.join();
                    }
                } catch (InterruptedException interruptedException) {
                    LOG.error("PerfTestClient.join() was interrupted");
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== RangerPolicyenginePerfTester.main()");
            }

            LOG.info("Completed performance-run");

            perfTestEngine.cleanup();

            PerfDataRecorder.printStatistics();
        }

        LOG.info("Exiting...");

    }

    private static List<String> buildPerfModuleNames(URL statCollectionFileURL) {
        List<String> perfModuleNames = new ArrayList<String>();

        try (
                InputStream inStream = statCollectionFileURL.openStream();
                InputStreamReader reader = new InputStreamReader(inStream, Charset.forName("UTF-8"));
                BufferedReader br = new BufferedReader(reader)
        ) {

            String line;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    String[] moduleNames = line.split(" ");
                    perfModuleNames.addAll(Arrays.asList(moduleNames));
                }
            }
        } catch (IOException exception) {
            System.out.println("Error reading arguments:" + exception);
        }

        return perfModuleNames;
    }
}
