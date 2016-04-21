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

import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


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
	final static int UNMASKED_VAL                = -1;

	int maskedUpperChar  = MASKED_UPPERCASE;
	int maskedLowerChar  = MASKED_LOWERCASE;
	int maskedDigitChar  = MASKED_NUMBER;
	int maskedOtherChar  = MASKED_OTHER_CHAR;
	int maskedNumber     = MASKED_NUMBER;
	int maskedDayValue   = MASKED_DAY_COMPONENT_VAL;
	int maskedMonthValue = MASKED_MONTH_COMPONENT_VAL;
	int maskedYearValue  = MASKED_YEAR_COMPONENT_VAL;

	public MaskTransformer() {
	}

	@Override
	public void init(ObjectInspector[] arguments, int startIdx) {
		int idx = startIdx;

		maskedUpperChar  = getCharArg(arguments, idx++, MASKED_UPPERCASE);
		maskedLowerChar  = getCharArg(arguments, idx++, MASKED_LOWERCASE);
		maskedDigitChar  = getCharArg(arguments, idx++, MASKED_NUMBER);
		maskedOtherChar  = getCharArg(arguments, idx++, MASKED_OTHER_CHAR);
		maskedNumber     = getCharArg(arguments, idx++, MASKED_NUMBER);
		maskedDayValue   = getIntArg(arguments, idx++, MASKED_DAY_COMPONENT_VAL);
		maskedMonthValue = getIntArg(arguments, idx++, MASKED_MONTH_COMPONENT_VAL);
		maskedYearValue  = getIntArg(arguments, idx++, MASKED_YEAR_COMPONENT_VAL);
	}

	@Override
	String transform(String value) {
		return maskString(value, 0, value.length());
	}

	@Override
	Byte transform(Byte value) {
		String strValue = value.toString();

		return toByte(Long.parseLong(maskNumber(strValue, 0, strValue.length())));
	}

	@Override
	Short transform(Short value) {
		String strValue = value.toString();

		return toShort(Long.parseLong(maskNumber(strValue, 0, strValue.length())));
	}

	@Override
	Integer transform(Integer value) {
		String strValue = value.toString();

		return toInteger(Long.parseLong(maskNumber(strValue, 0, strValue.length())));
	}

	@Override
	Long transform(Long value) {
		String strValue = value.toString();

		return Long.parseLong(maskNumber(strValue, 0, strValue.length()));
	}

	@Override
	Date transform(Date value) {
		return maskDate(value);
	}


	String maskString(String val, int startIdx, int endIdx) {
		StringBuilder strBuf = new StringBuilder(val.length());

		if(startIdx < 0) {
			startIdx = 0;
		}

		if(endIdx < 0) {
			endIdx = 0;
		}

		if(startIdx > val.length()) {
			startIdx = val.length();
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
					if(maskedUpperChar != UNMASKED_VAL) {
						c = maskedUpperChar;
					}
					break;

				case Character.LOWERCASE_LETTER:
					if(maskedLowerChar != UNMASKED_VAL) {
						c = maskedLowerChar;
					}
					break;

				case Character.DECIMAL_DIGIT_NUMBER:
					if(maskedDigitChar != UNMASKED_VAL) {
						c = maskedDigitChar;
					}
					break;

				default:
					if(maskedOtherChar != UNMASKED_VAL) {
						c = maskedOtherChar;
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

	String maskNumber(String val, int startIdx, int endIdx) {
		if(maskedNumber != UNMASKED_VAL) {
			StringBuilder strBuf = new StringBuilder(val.length());

			if (startIdx < 0) {
				startIdx = 0;
			}

			if (endIdx < 0) {
				endIdx = 0;
			}

			if (startIdx > val.length()) {
				startIdx = val.length();
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
						c = maskedNumber;
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

	Date maskDate(Date value) {
		int year  = maskedYearValue  == UNMASKED_VAL ? value.getYear()  : maskedYearValue;
		int month = maskedMonthValue == UNMASKED_VAL ? value.getMonth() : maskedMonthValue;
		int day   = maskedDayValue   == UNMASKED_VAL ? value.getDate()  : maskedDayValue;

		return new Date(year, month, day);
	}

	Byte toByte(long value) {
		if(value < Byte.MIN_VALUE) {
			return Byte.MIN_VALUE;
		} else if(value > Byte.MAX_VALUE) {
			return Byte.MAX_VALUE;
		} else {
			return (byte)value;
		}
	}

	Short toShort(long value) {
		if(value < Short.MIN_VALUE) {
			return Short.MIN_VALUE;
		} else if(value > Short.MAX_VALUE) {
			return Short.MAX_VALUE;
		} else {
			return (short)value;
		}
	}

	Integer toInteger(long value) {
		if(value < Integer.MIN_VALUE) {
			return Integer.MIN_VALUE;
		} else if(value > Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		} else {
			return (int)value;
		}
	}

	int getCharArg(ObjectInspector[] arguments, int index, int defaultValue) {
		int ret = defaultValue;

		ObjectInspector arg = (arguments != null && arguments.length > index) ? arguments[index] : null;

		if (arg != null) {
			if(arg instanceof WritableConstantIntObjectInspector) {
				IntWritable value = ((WritableConstantIntObjectInspector)arg).getWritableConstantValue();

				if(value != null) {
					ret = value.get();
				}
			} else if(arg instanceof WritableConstantLongObjectInspector) {
				LongWritable value = ((WritableConstantLongObjectInspector)arg).getWritableConstantValue();

				if(value != null) {
					ret = (int)value.get();
				}
			} else if(arg instanceof WritableConstantShortObjectInspector) {
				ShortWritable value = ((WritableConstantShortObjectInspector)arg).getWritableConstantValue();

				if(value != null) {
					ret = value.get();
				}
			} else if(arg instanceof ConstantObjectInspector) {
				Object value = ((ConstantObjectInspector) arg).getWritableConstantValue();

				if (value != null) {
					String strValue = value.toString();

					if (strValue != null && strValue.length() > 0) {
						ret = strValue.charAt(0);
					}
				}
			}
		}

		return ret;
	}

	int getIntArg(ObjectInspector[] arguments, int index, int defaultValue) {
		int ret = defaultValue;

		ObjectInspector arg = (arguments != null && arguments.length > index) ? arguments[index] : null;

		if (arg != null) {
			if (arg instanceof WritableConstantIntObjectInspector) {
				IntWritable value = ((WritableConstantIntObjectInspector) arg).getWritableConstantValue();

				if (value != null) {
					ret = value.get();
				}
			} else if (arg instanceof WritableConstantLongObjectInspector) {
				LongWritable value = ((WritableConstantLongObjectInspector) arg).getWritableConstantValue();

				if (value != null) {
					ret = (int) value.get();
				}
			} else if (arg instanceof WritableConstantShortObjectInspector) {
				ShortWritable value = ((WritableConstantShortObjectInspector) arg).getWritableConstantValue();

				if (value != null) {
					ret = value.get();
				}
			} else if (arg instanceof ConstantObjectInspector) {
				Object value = ((ConstantObjectInspector) arg).getWritableConstantValue();

				if (value != null) {
					String strValue = value.toString();

					if (strValue != null && strValue.length() > 0) {
						ret = Integer.parseInt(value.toString());
					}
				}
			}
		}

		return ret;
	}
}

