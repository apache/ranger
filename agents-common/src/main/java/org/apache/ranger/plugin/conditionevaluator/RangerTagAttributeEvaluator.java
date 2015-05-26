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

package org.apache.ranger.plugin.conditionevaluator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerResource;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

public class RangerTagAttributeEvaluator extends RangerAbstractConditionEvaluator {
	private static final Log LOG = LogFactory.getLog(RangerTagAttributeEvaluator.class);

	private ScriptEngine scriptEngine;

	@Override
	public void init() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagAttributeEvaluator.init(" + condition + ")");
		}

		super.init();

		Map<String, String> evalOptions = conditionDef.getEvaluatorOptions();

		if (evalOptions != null) {
			String engineType = evalOptions.get("interpreter");
			if (StringUtils.equals(engineType, "JavaScript")) {
				ScriptEngineManager manager = new ScriptEngineManager();
				scriptEngine = manager.getEngineByName("JavaScript");
			}
		}

		//scriptEngine.put("conditionDef", conditionDef);
		//scriptEngine.put("condition", condition);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagAttributeEvaluator.init(" + condition + ")");
		}
	}

	@Override
	public boolean isMatched(RangerAccessRequest request) {
		// TODO
		// Set up environment: selected parts of request
		if (LOG.isDebugEnabled()) {
			LOG.debug("==>RangerTagAttributeEvaluator.isMatched()");
		}

		Map<String, Object> requestContext = request.getContext();

		@SuppressWarnings("unchecked")
		RangerResource.RangerResourceTag tagObject = (RangerResource.RangerResourceTag)requestContext.get(RangerPolicyEngine.KEY_CONTEXT_TAG_OBJECT);

		if (tagObject == null) {
			LOG.error("RangerTagAttributeEvalator.isMatched(), No tag object found in the context. Weird!!!!");
			return false;
		}

		String tagAsJSON = tagObject.getJSONRepresentation();

		if (LOG.isDebugEnabled()) {
			LOG.debug("RangerTagAttributeEvaluator.isMatched(), tagObject as JSON=" + tagAsJSON);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("RangerTagAttributeEvaluator.isMatched(), tagObject=" + tagObject);
		}

		RangerTagAttributeEvaluatorResultProcessor resultProcessor = new RangerTagAttributeEvaluatorResultProcessor();

		/*
		Map<String, String> map = new HashMap<String, String>();
		map.put("bye", "now");
		*/
		/*
		// Convert it to a NativeObject (yes, this could have been done directly)
		NativeObject nobj = new NativeObject();
		for (Map.Entry<String, String> entry : map.entrySet()) {
			nobj.defineProperty(entry.getKey(), entry.getValue(), NativeObject.READONLY);
		}

		// Place native object into the context
		scriptEngine.put("map", nobj);
		*/

		/*
		try {
			//scriptEngine.eval("println(map.bye)");

			scriptEngine.eval("var map = " + new Gson().toJson(map) + ";\n"
					+ "println(map.bye);");
		} catch (Exception e) {
			System.out.println("Failed");
		}
		System.out.println("Succeeded");
		return true;
		*/

		// Place remaining objects directly into context
		/*
		scriptEngine.put("tagName", tagObject.getName());
		scriptEngine.put("request", request);
		*/
		scriptEngine.put("result", resultProcessor);

		String preamble = "var tag = " + tagAsJSON +";\n";

		List<String> values = condition.getValues();

		if (LOG.isDebugEnabled()) {
			LOG.debug("RangerTagAttributeEvaluator.isMatched(), values=" + values);
		}

		if (!CollectionUtils.isEmpty(values)) {

			String script = values.get(0);

			if (!StringUtils.isEmpty(script)) {

				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerTagAttributeEvaluator.isMatched(), evaluating script '" + script +"'");
				}
				if (scriptEngine != null) {
					try {
						scriptEngine.eval(preamble+script);
					} catch (ScriptException exception) {
						LOG.error("RangerTagAttributeEvaluator.isMatched(): failed to evaluate script," +
								" exception=" + exception);
					}
				} else {
					LOG.error("RangerTagAttributeEvaluator.isMatched(), No engine to evaluate script '" + script + "'");
					resultProcessor.setFailed();
				}

			}

		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<==RangerTagAttributeEvaluator.isMatched(), result=" + resultProcessor.getResult());
		}

		return resultProcessor.getResult();

	}
}
