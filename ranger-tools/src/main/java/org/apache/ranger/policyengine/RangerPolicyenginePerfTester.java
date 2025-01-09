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

import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.PerfDataRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RangerPolicyenginePerfTester {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPolicyenginePerfTester.class);

    private RangerPolicyenginePerfTester() {
        // to block instantiation
    }

    public static void main(String[] args) {
        LOG.debug("==> RangerPolicyenginePerfTester.main()");

        CommandLineParser commandLineParser = new CommandLineParser();
        PerfTestOptions   perfTestOptions   = commandLineParser.parse(args);

        if (perfTestOptions != null) {
            URL          statCollectionFileURL = perfTestOptions.getStatCollectionFileURL();
            List<String> perfModuleNames       = statCollectionFileURL != null ? buildPerfModuleNames(statCollectionFileURL) : new ArrayList<>();

            PerfDataRecorder.initialize(perfModuleNames);

            URL                       servicePoliciesFileURL = perfTestOptions.getServicePoliciesFileURL();
            RangerPolicyEngineOptions policyEngineOptions    = new RangerPolicyEngineOptions();

            policyEngineOptions.disableTagPolicyEvaluation = false;
            policyEngineOptions.evaluatorType              = RangerPolicyEvaluator.EVALUATOR_TYPE_OPTIMIZED;
            policyEngineOptions.cacheAuditResults          = false;
            policyEngineOptions.disableTrieLookupPrefilter = perfTestOptions.getIsTrieLookupPrefixDisabled();
            policyEngineOptions.optimizeTrieForRetrieval   = perfTestOptions.getIsOnDemandTriePostSetupDisabled();
            policyEngineOptions.optimizeTagTrieForSpace    = perfTestOptions.getIsTagTrieOptimizedForSpace();
            policyEngineOptions.optimizeTrieForSpace       = perfTestOptions.getIsPolicyTrieOptimizedForSpace();

            URL                 configurationFileURL = perfTestOptions.getPerfConfigurationFileURL();
            RangerConfiguration configuration        = new PerfTestConfiguration(configurationFileURL);

            policyEngineOptions.optimizeTrieForSpace        = configuration.getBoolean("ranger.policyengine.option.optimize.policy.trie.for.space", false);
            policyEngineOptions.optimizeTagTrieForSpace     = configuration.getBoolean("ranger.policyengine.option.optimize.tag.trie.for.space", false);
            policyEngineOptions.optimizeTagTrieForRetrieval = configuration.getBoolean("ranger.policyengine.option.optimize.tag.trie.for.retrieval", false);

            PerfTestEngine perfTestEngine = new PerfTestEngine(servicePoliciesFileURL, policyEngineOptions, configurationFileURL);

            if (!perfTestEngine.init()) {
                LOG.error("Error initializing test data. Existing...");

                System.exit(1);
            }

            URL[] requestFileURLs   = perfTestOptions.getRequestFileURLs();
            int   requestFilesCount = requestFileURLs.length;

            // warm-up policy engine
            LOG.error("Warming up..");

            try {
                for (URL requestFileURL : requestFileURLs) {
                    PerfTestClient perfTestClient = new PerfTestClient(perfTestEngine, 0, requestFileURL, 1);

                    if (perfTestClient.init()) {
                        perfTestClient.start();
                        perfTestClient.join();
                    } else {
                        LOG.error("Error initializing warm-up PerfTestClient");
                    }
                }
            } catch (Throwable t) {
                LOG.error("Error during warmup", t);
            }

            LOG.error("Warmed up!");

            PerfDataRecorder.clearStatistics();

            int                  clientsCount    = perfTestOptions.getConcurrentClientCount();
            List<PerfTestClient> perfTestClients = new ArrayList<>(clientsCount);

            for (int i = 0; i < clientsCount; i++) {
                URL requestFileURL = requestFileURLs[i % requestFilesCount];

                PerfTestClient perfTestClient = new PerfTestClient(perfTestEngine, i, requestFileURL, perfTestOptions.getIterationsCount());

                if (!perfTestClient.init()) {
                    LOG.error("Error initializing PerfTestClient: (id={})", i);
                } else {
                    perfTestClients.add(perfTestClient);
                }
            }

            LOG.debug("Number of perfTestClients={}", perfTestClients.size());

            Runtime runtime = Runtime.getRuntime();

            runtime.gc();

            long totalMemory = runtime.totalMemory();
            long freeMemory  = runtime.freeMemory();

            LOG.info("Before performance-run start: Memory stats: max-available=:{}; in-use={}; free={}", runtime.maxMemory(), totalMemory - freeMemory, freeMemory);
            LOG.info("Starting {} clients..", perfTestClients.size());

            for (PerfTestClient client : perfTestClients) {
                try {
                    client.start();
                } catch (Throwable t) {
                    LOG.error("Error in starting client: {}", client.getName(), t);
                }
            }

            LOG.info("Started {} clients", perfTestClients.size());
            LOG.info("Waiting for {} clients to finish up", perfTestClients.size());

            for (PerfTestClient client : perfTestClients) {
                while (client.isAlive()) {
                    try {
                        client.join(1000);
                    } catch (InterruptedException interruptedException) {
                        LOG.error("PerfTestClient.join() was interrupted");
                    }
                }
            }

            LOG.debug("<== RangerPolicyenginePerfTester.main()");
            LOG.info("Completed performance-run");

            runtime.gc();

            totalMemory = runtime.totalMemory();
            freeMemory  = runtime.freeMemory();

            LOG.info("After performance-run end: Memory stats: max-available=:{}; in-use={}; free={}", runtime.maxMemory(), totalMemory - freeMemory, freeMemory);

            perfTestEngine.cleanUp();

            PerfDataRecorder.printStatistics();
        }

        LOG.info("Exiting...");
    }

    private static List<String> buildPerfModuleNames(URL statCollectionFileURL) {
        List<String> perfModuleNames = new ArrayList<>();

        try (InputStream inStream = statCollectionFileURL.openStream();
                InputStreamReader reader = new InputStreamReader(inStream, StandardCharsets.UTF_8);
                BufferedReader    br = new BufferedReader(reader)) {
            for (String line = br.readLine(); line != null; line = br.readLine()) {
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
