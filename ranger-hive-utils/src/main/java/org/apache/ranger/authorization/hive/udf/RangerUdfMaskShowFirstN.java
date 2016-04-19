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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;


public class RangerUdfMaskShowFirstN extends RangerBaseUdf {
    public RangerUdfMaskShowFirstN() {
        super(new MaskShowFirstNTransformer());
    }
}

class MaskShowFirstNTransformer extends MaskTransformer {
    int charCount = 4;

    public MaskShowFirstNTransformer() {
        super();
    }

    @Override
    public void init(ObjectInspector[] arguments, int argsStartIdx) {
        super.init(arguments, argsStartIdx + 1); // first argument is charCount, which is consumed here

        charCount = getIntArg(arguments, argsStartIdx, 4);
    }

    @Override
    String transform(String value) {
        return maskString(value, maskedUpperChar, maskedLowerChar, maskedDigitChar, maskedOtherChar, charCount, value.length());
    }

    @Override
    Short transform(Short value) {
        String strValue = value.toString();

        return Short.parseShort(maskNumber(strValue, maskedDigitChar, charCount, strValue.length()));
    }

    @Override
    Integer transform(Integer value) {
        String strValue = value.toString();

        return Integer.parseInt(maskNumber(strValue, maskedDigitChar, charCount, strValue.length()));
    }

    @Override
    Long transform(Long value) {
        String strValue = value.toString();

        return Long.parseLong(maskNumber(strValue, maskedDigitChar, charCount, strValue.length()));
    }
}
