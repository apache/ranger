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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerResource;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RangerScriptConditionEvaluator extends RangerAbstractConditionEvaluator {
	private static final Log LOG = LogFactory.getLog(RangerScriptConditionEvaluator.class);

	private ScriptEngine scriptEngine;

	@Override
	public void init() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerScriptConditionEvaluator.init(" + condition + ")");
		}

		super.init();

		String engineName = "JavaScript";

		Map<String, String> evalOptions = conditionDef. getEvaluatorOptions();

		if (MapUtils.isNotEmpty(evalOptions)) {
			engineName = evalOptions.get("engineName");
		}

		if (StringUtils.isBlank(engineName)) {
			engineName = "JavaScript";
		}

		ScriptEngineManager manager = new ScriptEngineManager();
		scriptEngine = manager.getEngineByName(engineName);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerScriptConditionEvaluator.init(" + condition + ")");
		}
	}

	@Override
	public boolean isMatched(RangerAccessRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==>RangerScriptConditionEvaluator.isMatched()");
		}

		Boolean result = false;

		List<String> values = condition.getValues();

		if (!CollectionUtils.isEmpty(values)) {

			// Evaluate the first string
			String value = values.get(0);
			if (StringUtils.isNotBlank(value)) {

				RangerAccessRequest readOnlyRequest = request.getReadOnlyCopy();

				@SuppressWarnings("unchecked")
				List<RangerResource.RangerResourceTag> tagsList = (List <RangerResource.RangerResourceTag>)readOnlyRequest.getContext().get("TAGS");

				Bindings bindings   = scriptEngine.createBindings();

				if (CollectionUtils.isNotEmpty(tagsList)) {
					List<RangerResource.RangerResourceTag> readOnlyTags = Collections.unmodifiableList(tagsList);
					bindings.put("tags", readOnlyTags);
				}

				bindings.put("request", readOnlyRequest);
				bindings.put("ctx", readOnlyRequest.getContext());
				bindings.put("result", result);

				String script = value.trim();

				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerScriptConditionEvaluator.isMatched(): script={" + script + "}");
				}
				try {

					Object ret = scriptEngine.eval(script, bindings);

					if (ret == null) {
						ret = bindings.get("result");
					}
					if (ret != null && ret instanceof Boolean) {
						result = (Boolean) ret;
					}

				} catch (NullPointerException nullp) {
					LOG.error("RangerScriptConditionEvaluator.isMatched(): eval called with NULL argument(s)");

				} catch (ScriptException exception) {
					LOG.error("RangerScriptConditionEvaluator.isMatched(): failed to evaluate script," +
							" exception=" + exception);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<==RangerScriptConditionEvaluator.isMatched(), result=" + result);
		}

		return result;

	}
}
