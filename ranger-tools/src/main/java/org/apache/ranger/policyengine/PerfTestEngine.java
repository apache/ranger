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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.*;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;

public class PerfTestEngine {
	static final Log LOG      = LogFactory.getLog(PerfTestEngine.class);

	static private final long POLICY_ENGINE_REORDER_AFTER_PROCESSING_REQUESTS_COUNT = 100;
	private final URL servicePoliciesFileURL;
	private final RangerPolicyEngineOptions policyEngineOptions;
	private RangerPolicyEngine policyEvaluationEngine;
	private final boolean disableDynamicPolicyEvalReordering;
	private AtomicLong requestCount = new AtomicLong();

	public PerfTestEngine(final URL servicePoliciesFileURL, RangerPolicyEngineOptions policyEngineOptions, boolean disableDynamicPolicyEvalReordering) {
		this.servicePoliciesFileURL = servicePoliciesFileURL;
		this.policyEngineOptions = policyEngineOptions;
		this.disableDynamicPolicyEvalReordering = disableDynamicPolicyEvalReordering;
	}

	public boolean init() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> init()");
		}

		boolean ret = false;

		Gson gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z")
				.setPrettyPrinting()
				.create();

		Reader reader = null;
		ServicePolicies servicePolicies;

		try {
			InputStream in = servicePoliciesFileURL.openStream();

			reader = new InputStreamReader(in, Charset.forName("UTF-8"));

			servicePolicies = gsonBuilder.fromJson(reader, ServicePolicies.class);

			policyEvaluationEngine = new RangerPolicyEngineImpl("perf-test", servicePolicies, policyEngineOptions);

			requestCount.set(0L);

			ret = true;

		} catch (Exception excp) {
			LOG.error("Error opening service-policies file or loading service-policies from file, URL=" + servicePoliciesFileURL, excp);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception excp) {
					LOG.error("Error closing file", excp);
				}
			}
		}


		if (LOG.isDebugEnabled()) {
			LOG.debug("<== init() : " + ret);
		}

		return ret;

	}
	public RangerAccessResult execute(final RangerAccessRequest request) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> execute(" + request + ")");
		}

		RangerAccessResult ret = null;

		if (policyEvaluationEngine != null) {

			long processedRequestCount = requestCount.getAndIncrement();

			if (!disableDynamicPolicyEvalReordering && (processedRequestCount % POLICY_ENGINE_REORDER_AFTER_PROCESSING_REQUESTS_COUNT) == 0) {
				policyEvaluationEngine.reorderPolicyEvaluators();
			}

			policyEvaluationEngine.preProcess(request);

			ret = policyEvaluationEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ACCESS, null);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Executed request = {" + request + "}, result={" + ret + "}");
			}
		} else {
			LOG.error("Error executing request: PolicyEngine is null!");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== execute(" + request + ") : " + ret);
		}

		return ret;
	}

	public void cleanup() {
		if (policyEvaluationEngine != null) {
			policyEvaluationEngine.cleanup();
			policyEvaluationEngine = null;
		}
	}
}
