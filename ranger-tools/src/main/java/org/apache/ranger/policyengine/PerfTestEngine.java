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
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.*;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

public class PerfTestEngine {
	static final Log LOG      = LogFactory.getLog(PerfTestEngine.class);

	private final URL servicePoliciesFileURL;
	private RangerPolicyEngine policyEvaluationEngine;

	public PerfTestEngine(final URL servicePoliciesFileURL) {
		this.servicePoliciesFileURL = servicePoliciesFileURL;
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

			reader = new InputStreamReader(in);

			servicePolicies = gsonBuilder.fromJson(reader, ServicePolicies.class);

			RangerPolicyEngineOptions engineOptions = new RangerPolicyEngineOptions();
			engineOptions.disableTagPolicyEvaluation = false;

			policyEvaluationEngine = new RangerPolicyEngineImpl("perf-test", servicePolicies, engineOptions);

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
	public boolean execute(final RangerAccessRequest request) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> execute(" + request + ")");
		}

		boolean ret = true;

		if (policyEvaluationEngine != null) {

			RangerAccessResultProcessor auditHandler = null;

			policyEvaluationEngine.preProcess(request);

			RangerAccessResult result = policyEvaluationEngine.isAccessAllowed(request, auditHandler);
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
