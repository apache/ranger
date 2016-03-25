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


public class MaskTransformer extends RangerTransformer {
	final static int MASKED_UPPERCASE            = 'X';
	final static int MASKED_LOWERCASE            = 'x';
	final static int MASKED_NUMBER               = '1';
	final static int MASKED_DAY_COMPONENT_VAL    = 1;
	final static int MASKED_MONTH_COMPONENT_VAL  = 0;
	final static int MASKED_YEAR_COMPONENT_VAL   = 0;
	final static int UNMASKED_DATE_COMPONENT_VAL = -1;

	int maskedUpperChar   = MASKED_UPPERCASE;
	int maskedLowerChar   = MASKED_LOWERCASE;
	int maskedDigitChar   = MASKED_NUMBER;
	int maskedNumber      = MASKED_NUMBER;
	int maskedDayValue    = MASKED_DAY_COMPONENT_VAL;
	int maskedMonthValue  = MASKED_MONTH_COMPONENT_VAL;
	int maskedYearValue   = MASKED_YEAR_COMPONENT_VAL;

	public MaskTransformer() {
	}

	@Override
	public void init(String initParam) {
		if(StringUtils.isNotEmpty(initParam)) {
			for(String nvStr : initParam.split(";")) {
				if(StringUtils.isNotEmpty(nvStr)) {
					String[] nameValue = nvStr.split("=", 2);
					String  name  = nameValue != null && nameValue.length > 0 ? nameValue[0] : null;
					String  value = nameValue != null && nameValue.length > 1 ? nameValue[1] : null;

					if(StringUtils.isNotEmpty(name)) {
						options.put(name.trim(), value.trim());
					}
				}
			}
		}

		maskedUpperChar  = getCharOption("upper", MASKED_UPPERCASE);
		maskedLowerChar  = getCharOption("lower", MASKED_LOWERCASE);
		maskedDigitChar  = getCharOption("digit", MASKED_NUMBER);
		maskedNumber     = getCharOption("number", MASKED_NUMBER);
		maskedDayValue   = getIntOption("day", MASKED_DAY_COMPONENT_VAL);
		maskedMonthValue = getIntOption("month", MASKED_MONTH_COMPONENT_VAL);
		maskedYearValue  = getIntOption("year", MASKED_YEAR_COMPONENT_VAL);
	}

	@Override
	String transform(String value) {
		return value == null ? STRING_0 : transformAlphaNum(value);
	}

	@Override
	Short transform(Short value) {
		return value == null ? SHORT_0 : Short.parseShort(transformNum(value.toString()));
	}

	@Override
	Integer transform(Integer value) {
		return value == null ? INTEGER_0 : Integer.parseInt(transformNum(value.toString()));
	}

	@Override
	Long transform(Long value) {
		return value == null ? LONG_0 : Long.parseLong(transformNum(value.toString()));
	}

	@Override
	Date transform(Date value) {
		return mask(value, maskedDayValue, maskedMonthValue, maskedYearValue);
	}

	String transformNum(String val) {
		return transformNum(val, maskedNumber, getMaskStartIndex(val), getMaskEndIndex(val));
	}

	String transformAlphaNum(String val) {
		return transformAlphaNum(val, maskedUpperChar, maskedLowerChar, maskedDigitChar, getMaskStartIndex(val), getMaskEndIndex(val));
	}

	Date mask(Date value, int maskedDay, int maskedMonth, int maskedYear) {
		int year  = maskedYear  == UNMASKED_DATE_COMPONENT_VAL ? value.getYear()  : MASKED_YEAR_COMPONENT_VAL;
		int month = maskedMonth == UNMASKED_DATE_COMPONENT_VAL ? value.getMonth() : MASKED_MONTH_COMPONENT_VAL;
		int day   = maskedDay   == UNMASKED_DATE_COMPONENT_VAL ? value.getDate()  : MASKED_DAY_COMPONENT_VAL;

		return new Date(year, month, day);
	}

	int getMaskStartIndex(String val) {
		return 0;
	}

	int getMaskEndIndex(String val) {
		return val.length();
	}


	String transformNum(String val, int replaceDigit, int startIdx, int endIdx) {
		if(val == null) {
			return null;
		}

		StringBuffer strBuf = new StringBuffer(val.length());

		if(startIdx < 0) {
			startIdx = 0;
		}

		if(endIdx > val.length()) {
			endIdx = val.length();
		}

		for(int i = 0; i < startIdx; i++) {
			strBuf.appendCodePoint(val.charAt(i));
		}

		for(int i = startIdx; i < endIdx; i++) {
			int c = val.charAt(i);

			switch(Character.getType(c)) {
				case Character.DECIMAL_DIGIT_NUMBER:
					c = replaceDigit;
					break;
			}

			strBuf.appendCodePoint(c);
		}

		for(int i = endIdx; i < val.length(); i++) {
			strBuf.appendCodePoint(val.charAt(i));
		}

		String ret = strBuf.toString();

		return ret;
	}

	String transformAlphaNum(String val, int replaceUpperChar, int replaceLowerChar, int replaceDigit, int startIdx, int endIdx) {
		if(val == null) {
			return null;
		}

		StringBuffer strBuf = new StringBuffer(val.length());

		if(startIdx < 0) {
			startIdx = 0;
		}

		if(endIdx > val.length()) {
			endIdx = val.length();
		}

		for(int i = 0; i < startIdx; i++) {
			strBuf.appendCodePoint(val.charAt(i));
		}

		for(int i = startIdx; i < endIdx; i++) {
			int c = val.charAt(i);

			switch(Character.getType(c)) {
				case Character.UPPERCASE_LETTER:
					c = replaceUpperChar;
					break;

				case Character.LOWERCASE_LETTER:
					c = replaceLowerChar;
					break;

				case Character.DECIMAL_DIGIT_NUMBER:
					c = replaceDigit;
					break;
			}

			strBuf.appendCodePoint(c);
		}

		for(int i = endIdx; i < val.length(); i++) {
			strBuf.appendCodePoint(val.charAt(i));
		}

		String ret = strBuf.toString();

		return ret;
	}
}
