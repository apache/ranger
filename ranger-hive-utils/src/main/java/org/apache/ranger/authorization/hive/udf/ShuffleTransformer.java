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

import java.sql.Date;


public class ShuffleTransformer extends RangerTransformer {
	public ShuffleTransformer() {
	}

	@Override
	public void init(String initParam) {
	}

	@Override
	String transform(String value) {
		String ret = value;

		if(value != null) {
			final char[] chars = value.toCharArray();

			for(int i = 0; i < chars.length; i++) {
				int rIdx = (int)(Math.random() * chars.length);
				char swapTmp = chars[i];
				chars[i] = chars[rIdx];
				chars[rIdx] = swapTmp;
			}

			ret = new String(chars);
		}

		return ret;
	}

	@Override
	Short transform(Short value) {
		Short ret = value;

		if(value != null) {
			String strValue = Short.toString(value);

			ret = Short.parseShort(transform(strValue));
		}

		return ret;
	}

	@Override
	Integer transform(Integer value) {
		Integer ret = value;

		if(value != null) {
			String strValue = Integer.toString(value);

			ret = Integer.parseInt(transform(strValue));
		}

		return ret;
	}

	@Override
	Long transform(Long value) {
		Long ret = value;

		if(value != null) {
			String strValue = Long.toString(value);

			ret = Long.parseLong(transform(strValue));
		}

		return ret;
	}

	@Override
	Date transform(Date value) {
		Date ret = value;

		if(value != null) {
			// TODO: date shuffle
		}

		return ret;
	}
}
