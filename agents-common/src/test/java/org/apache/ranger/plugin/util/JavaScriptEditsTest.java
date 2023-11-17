/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.plugin.util;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JavaScriptEditsTest {

	@Test
	public void testExpressions() {
		Map<String, String> tests = new HashMap<>();

		tests.put("[[TAG.value]].intersects([[USER[TAG._type]]])", "TAG.value.split(\",\").intersects(USER[TAG._type].split(\",\"))");
		tests.put("${{[[\"$USER.eventType\",'|']]}}.includes(jsonAttr.eventType)", "${{\"$USER.eventType\".split(\"|\")}}.includes(jsonAttr.eventType)");
		tests.put("TAG.value == 'email'", "TAG.value == 'email'");       // output same as input
		tests.put("UGNAMES[0] == 'analyst'", "UGNAMES[0] == 'analyst'"); // output same as input

		for (Map.Entry<String, String> test : tests.entrySet()) {
			String input  = test.getKey();
			String output = test.getValue();

			assertEquals("input: " + input, output, JavaScriptEdits.replaceDoubleBrackets(input));
		}
	}
}
