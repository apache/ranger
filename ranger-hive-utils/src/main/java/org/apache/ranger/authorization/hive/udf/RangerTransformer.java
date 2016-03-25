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

package org.apache.ranger.authorization.hive.udf;

import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
import java.util.HashMap;
import java.util.Map;


public abstract class RangerTransformer {
	static final Short   SHORT_0   = Short.valueOf((short)0);
	static final Integer INTEGER_0 = Integer.valueOf((int)0);
	static final Long    LONG_0    = Long.valueOf((long)0);
	static final Date    DATE_0    = new Date(0);
	static final String  STRING_0  = new String("0");

	final Map<String, String> options = new HashMap<String, String>();


	abstract void    init(String initParam);

	abstract String  transform(String value);
	abstract Short   transform(Short value);
	abstract Integer transform(Integer value);
	abstract Long    transform(Long value);
	abstract Date    transform(Date value);


	int getCharOption(String name, int defValue) {
		String value = options.get(name);

		return StringUtils.isNotEmpty(value) ? value.charAt(0) : defValue;
	}

	int getIntOption(String name, int defValue) {
		String value = options.get(name);

		return StringUtils.isNotEmpty(value) ? Integer.parseInt(value) : defValue;
	}
}


