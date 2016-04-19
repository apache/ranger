/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.authorization.hive.udf;


import java.sql.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class RangerUdfMask extends RangerBaseUdf {
	public RangerUdfMask() {
		super(new MaskTransformer());
	}
}

class MaskTransformer extends RangerBaseUdf.RangerTransformer {
	final static int MASKED_UPPERCASE            = 'X';
	final static int MASKED_LOWERCASE            = 'x';
	final static int MASKED_NUMBER               = '1';
	final static int MASKED_OTHER_CHAR           = -1;
	final static int MASKED_DAY_COMPONENT_VAL    = 1;
	final static int MASKED_MONTH_COMPONENT_VAL  = 0;
	final static int MASKED_YEAR_COMPONENT_VAL   = 0;
	final static int UNMASKED_DATE_COMPONENT_VAL = -1;

	int maskedUpperChar   = MASKED_UPPERCASE;
	int maskedLowerChar   = MASKED_LOWERCASE;
	int maskedDigitChar   = MASKED_NUMBER;
	int maskedOtherChar   = MASKED_OTHER_CHAR;
	int maskedNumber      = MASKED_NUMBER;
	int maskedDayValue    = MASKED_DAY_COMPONENT_VAL;
	int maskedMonthValue  = MASKED_MONTH_COMPONENT_VAL;
	int maskedYearValue   = MASKED_YEAR_COMPONENT_VAL;

	public MaskTransformer() {
	}

	@Override
	public void init(ObjectInspector[] arguments, int startIdx) {
		maskedUpperChar  = getCharArg(arguments, startIdx + 0, MASKED_UPPERCASE);
		maskedLowerChar  = getCharArg(arguments, startIdx + 1, MASKED_LOWERCASE);
		maskedDigitChar  = getCharArg(arguments, startIdx + 2, MASKED_NUMBER);
		maskedOtherChar  = getCharArg(arguments, startIdx + 3, MASKED_OTHER_CHAR);
		maskedNumber     = getCharArg(arguments, startIdx + 4, MASKED_NUMBER);
		maskedDayValue   = getIntArg(arguments, startIdx + 5, MASKED_DAY_COMPONENT_VAL);
		maskedMonthValue = getIntArg(arguments, startIdx + 6, MASKED_MONTH_COMPONENT_VAL);
		maskedYearValue  = getIntArg(arguments, startIdx + 7, MASKED_YEAR_COMPONENT_VAL);
	}

	@Override
	String transform(String value) {
		return maskString(value, maskedUpperChar, maskedLowerChar, maskedDigitChar, maskedOtherChar, 0, value.length());
	}

	@Override
	Short transform(Short value) {
		String strValue = value.toString();

		return Short.parseShort(maskNumber(strValue, maskedDigitChar, 0, strValue.length()));
	}

	@Override
	Integer transform(Integer value) {
		String strValue = value.toString();

		return Integer.parseInt(maskNumber(strValue, maskedDigitChar, 0, strValue.length()));
	}

	@Override
	Long transform(Long value) {
		String strValue = value.toString();

		return Long.parseLong(maskNumber(strValue, maskedDigitChar, 0, strValue.length()));
	}

	@Override
	Date transform(Date value) {
		return maskDate(value, maskedDayValue, maskedMonthValue, maskedYearValue);
	}


	String maskString(String val, int replaceUpperChar, int replaceLowerChar, int replaceDigitChar, int replaceOtherChar, int startIdx, int endIdx) {
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
					if(replaceUpperChar != -1) {
						c = replaceUpperChar;
					}
					break;

				case Character.LOWERCASE_LETTER:
					if(replaceLowerChar != -1) {
						c = replaceLowerChar;
					}
					break;

				case Character.DECIMAL_DIGIT_NUMBER:
					if(replaceDigitChar != -1) {
						c = replaceDigitChar;
					}
					break;

				default:
					if(replaceOtherChar != -1) {
						c = replaceOtherChar;
					}
					break;
			}

			strBuf.appendCodePoint(c);
		}

		for(int i = endIdx; i < val.length(); i++) {
			strBuf.appendCodePoint(val.charAt(i));
		}

		return strBuf.toString();
	}

	String maskNumber(String val, int replaceDigit, int startIdx, int endIdx) {
		if(replaceDigit != -1) {
			StringBuffer strBuf = new StringBuffer(val.length());

			if (startIdx < 0) {
				startIdx = 0;
			}

			if (endIdx > val.length()) {
				endIdx = val.length();
			}

			for (int i = 0; i < startIdx; i++) {
				strBuf.appendCodePoint(val.charAt(i));
			}

			for (int i = startIdx; i < endIdx; i++) {
				int c = val.charAt(i);

				switch (Character.getType(c)) {
					case Character.DECIMAL_DIGIT_NUMBER:
						c = replaceDigit;
					break;
				}

				strBuf.appendCodePoint(c);
			}

			for (int i = endIdx; i < val.length(); i++) {
				strBuf.appendCodePoint(val.charAt(i));
			}

			return strBuf.toString();
		}

		return val;
	}

	Date maskDate(Date value, int maskedDay, int maskedMonth, int maskedYear) {
		int year  = maskedYear  == UNMASKED_DATE_COMPONENT_VAL ? value.getYear()  : MASKED_YEAR_COMPONENT_VAL;
		int month = maskedMonth == UNMASKED_DATE_COMPONENT_VAL ? value.getMonth() : MASKED_MONTH_COMPONENT_VAL;
		int day   = maskedDay   == UNMASKED_DATE_COMPONENT_VAL ? value.getDate()  : MASKED_DAY_COMPONENT_VAL;

		return new Date(year, month, day);
	}

	String getStringArg(ObjectInspector[] arguments, int index, String defaultValue) {
		String ret = defaultValue;

		ObjectInspector arg = (arguments != null && arguments.length > index) ? arguments[index] : null;

		if (arg != null) {
			if (arg instanceof WritableConstantStringObjectInspector) {
				Text value = ((WritableConstantStringObjectInspector) arg).getWritableConstantValue();

				if (value != null && value.getLength() > 0) {
					ret = value.toString();
				}
			}
		}

		return ret;
	}

	int getCharArg(ObjectInspector[] arguments, int index, int defaultValue) {
		int ret = defaultValue;

		String value = getStringArg(arguments, index, null);

		if (StringUtils.isNotEmpty(value)) {
			ret = value.charAt(0);
		}

		return ret;
	}

	int getIntArg(ObjectInspector[] arguments, int index, int defaultValue) {
		int ret = defaultValue;

		ObjectInspector arg = (arguments != null && arguments.length > index) ? arguments[index] : null;

		if (arg != null) {
			if (arg instanceof WritableConstantStringObjectInspector) {
				Text value = ((WritableConstantStringObjectInspector) arg).getWritableConstantValue();

				if (value != null) {
					try {
						ret = Integer.parseInt(value.toString());
					} catch(NumberFormatException excp) {
						// ignored; defaultValue will be returned
					}
				}
			} else if(arg instanceof WritableConstantIntObjectInspector) {
				IntWritable value = ((WritableConstantIntObjectInspector)arg).getWritableConstantValue();

				if(value != null) {
					ret = value.get();
				}
			}
		}

		return ret;
	}
}

