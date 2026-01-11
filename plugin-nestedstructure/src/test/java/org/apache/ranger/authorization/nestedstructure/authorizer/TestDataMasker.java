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

package org.apache.ranger.authorization.nestedstructure.authorizer;

import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.CUSTOM;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK_DATE_SHOW_YEAR;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK_HASH;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK_NONE;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK_NULL;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK_SHOW_FIRST_4;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK_SHOW_LAST_4;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDataMasker {
    private static final Pattern HEXADECIMAL_PATTERN = Pattern.compile("\\p{XDigit}+");

    public Object[][] testMaskProvider() {
        return new Object[][] {
                {"1234567890", MASK, null, "**********"},
                {"1", MASK, null, "*****"},
                {"", MASK, null, "*****"},
                {null, MASK, null, "*****"},
                {"1234567890", MASK_SHOW_LAST_4, null, "xxxxxx7890"},

                {null, MASK_SHOW_LAST_4, null, null},
                {"1", MASK_SHOW_LAST_4, null, "1"},
                {"12", MASK_SHOW_LAST_4, null, "12"},
                {"abc", MASK_SHOW_LAST_4, null, "abc"},
                {"abcd", MASK_SHOW_LAST_4, null, "abcd"},
                {"abcde", MASK_SHOW_LAST_4, null, "xbcde"},
                {"abcde1234567890a", MASK_SHOW_LAST_4, null, "xxxxxxxxxxxx890a"},

                {null, MASK_SHOW_FIRST_4, null, null},
                {"1", MASK_SHOW_FIRST_4, null, "1"},
                {"12", MASK_SHOW_FIRST_4, null, "12"},
                {"abc", MASK_SHOW_FIRST_4, null, "abc"},
                {"abcd", MASK_SHOW_FIRST_4, null, "abcd"},
                {"abcde", MASK_SHOW_FIRST_4, null, "abcdx"},
                {"abcde1234567890a", MASK_SHOW_FIRST_4, null, "abcdxxxxxxxxxxxx"},

                {null, MASK_NULL, null, null},
                {"1", MASK_NULL, null, null},
                {"12", MASK_NULL, null, null},
                {"abc", MASK_NULL, null, null},
                {"abcd", MASK_NULL, null, null},
                {"abcde", MASK_NULL, null, null},
                {"abcde1234567890a", MASK_NULL, null, null},

                {null, CUSTOM, null, null},
                {"1", CUSTOM, null, null},
                {"12", CUSTOM, "woot", "woot"},
                {"abc", CUSTOM, "woot", "woot"},
                {"abcd", CUSTOM, "woot", "woot"},
                {"abcde", CUSTOM, "woot", "woot"},
                {"abcde1234567890a", CUSTOM, "woot", "woot"},

                {"1234567890", MASK, null, "**********"},
                {"1234567890", MASK, null, "**********"},
                {"1234567890", MASK, null, "**********"},
                {"1", MASK, null, "*****"},
                {"1B", MASK, null, "*****"},
                {"akdkajkjkdfsjkdfsjklfjkfjkljkjklfjkldfsdfjkldfjkldfsjkdfjkljkljklf", MASK, null, "******************************"},
        };
    }

    public Object[][] shaProvider() {
        return new Object[][] {
                {"1234567890"},
                {null},
                {""},
                {"djfklasfjkjkdsjadsjkladfsjkl;adfsjewi9etwigodsfojkkmcv  " +
                        "]e3djfjkadsjkfls;jkfjdsfkj;kldsjfdsl;jfas" +
                        "dsfjadsl;fjdsklfjewfl fjdsjw fkl;jfkldsj9049023902390234902349023490" +
                        "]389439023490234890234890234890234890fjfsjdfsjkldsjkldfsjkdfsjklef" +
                        "ershjewjrjkl;erwjkl;erwijo23490234890234890fjkdfsjkdfsjkadsf" +
                        "23490234890890dfiudfsjkdfsjkldfsjkl90890234890234890fdjklfj!@#%^))(*&^%$(" +
                        ")(*&^%$#!@#$%^&*()(*&^%$@#$%^&*((*&^%$!@#$%^&*((*&^%$@#$%^&*()(*&^%$@#$%^"},
                {"fdjkls"},
                {"    "}
        };
    }

    public Object[][] badMasks() {
        return new Object[][] {
                {"1234567890", null, null, "**********"},
                {"1", null, null, "*****"},

                {null, "", null, null},
                {"1", "", null, "1"},
                {"12", "", null, "12"},

                {"abcd", "mask", null, "abcd"},
                {"abcde", "mask", null, "abcdx"},
                {"abcde1234567890a", "mask", null, "abcdxxxxxxxxxxxx"},
        };
    }

    public Object[][] dateformats() {
        return new Object[][] {
                {"", MASK_DATE_SHOW_YEAR, null, ""},
                {null, MASK_DATE_SHOW_YEAR, null, null},
                {"20111203", MASK_DATE_SHOW_YEAR, null, "2011"},
                {"2011-12-03", MASK_DATE_SHOW_YEAR, null, "2011"},
                {"2011-12-03+01:00", MASK_DATE_SHOW_YEAR, null, "2011"},
                {"2012-12-03+01:00", MASK_DATE_SHOW_YEAR, null, "2012"},
                {"2011-12-03T10:15:30", MASK_DATE_SHOW_YEAR, null, "2011"},
                {"2011-12-03T10:15:30+01:00", MASK_DATE_SHOW_YEAR, null, "2011"},
                {"2015-12-03T10:15:30+01:00[Europe/Paris]", MASK_DATE_SHOW_YEAR, null, "2015"},
                {"2012-12-03T10:15:30+01:00[Europe/Paris]", MASK_DATE_SHOW_YEAR, null, "2012"},
                {"2012-337", MASK_DATE_SHOW_YEAR, null, "2012"},
                {"2012-W48-6", MASK_DATE_SHOW_YEAR, null, "2012"},
                {"Tue, 3 Jun 2008 11:05:30 GMT", MASK_DATE_SHOW_YEAR, null, "2008"},
                {"3 Jun 2008 11:05:30 GMT", MASK_DATE_SHOW_YEAR, null, "2008"}
        };
    }

    public Object[][] dateformatsBad() {
        return new Object[][] {
                {" ", MASK_DATE_SHOW_YEAR, null, ""},
                {"null", MASK_DATE_SHOW_YEAR, null, null},
                {"2011120354", MASK_DATE_SHOW_YEAR, null, "2011"},
                {"2011--12-03", MASK_DATE_SHOW_YEAR, null, "2011"},
                {"2011-12 01:00", MASK_DATE_SHOW_YEAR, null, "2011"},
                {"2012-12-03T+01:00", MASK_DATE_SHOW_YEAR, null, "2012"},
                {"2011-12-0310:15:30", MASK_DATE_SHOW_YEAR, null, "2011"},
        };
    }

    public Object[][] booleans() {
        return new Object[][] {
                {true, MASK, null, false},
                {false, MASK, null, false},

                {true, CUSTOM, "true", true},
                {false, CUSTOM, "true", true},
                {true, CUSTOM, "false", false},
                {false, CUSTOM, "false", false},

                {true, MASK_NULL, "true", null},
                {false, MASK_NULL, "false", null},

                {true, MASK_NONE, null, true},
                {false, MASK_NONE, null, false},
        };
    }

    public Object[][] booleansBad() {
        return new Object[][] {
                {false, MASK_DATE_SHOW_YEAR, null, null},
                {false, MASK_HASH, null, null},
                {true, MASK_HASH, null, null},
                {false, null, null, null},
        };
    }

    public Object[][] numbers() {
        return new Object[][] {
                {0, MASK, null, DataMasker.DEFAULT_NUMBER_MASK},
                {-101, MASK, null, DataMasker.DEFAULT_NUMBER_MASK},
                {1.215, MASK, null, DataMasker.DEFAULT_NUMBER_MASK},
                {12345648976453L, MASK, null, DataMasker.DEFAULT_NUMBER_MASK},

                {0, MASK_NULL, null, null},
                {-101, MASK_NULL, null, null},
                {1.215, MASK_NULL, null, null},
                {12345648976453L, MASK_NULL, null, null},

                {0, MASK_NONE, null, 0},
                {-101, MASK_NONE, null, -101},
                {1.215, MASK_NONE, null, 1.215},
                {12345648976453L, MASK_NONE, null, 12345648976453L},

                {0, CUSTOM, "100", 100},
                {-101, CUSTOM, "202", 202},
                {1.215, CUSTOM, "303", 303},
                {12345648976453L, CUSTOM, "-404", -404},
        };
    }

    public Object[][] numbersBad() {
        return new Object[][] {
                {1, MASK_DATE_SHOW_YEAR, null, null},
                {null, MASK_HASH, null, null},
                {1000, MASK_HASH, null, null},
                {1001.012345, null, null, null},

                {1, CUSTOM, "", null},
                {null, CUSTOM, "null", null},
                {1000, CUSTOM, "102456fdafdasfda45fghnhjjuio", null},
                {1001.012345, CUSTOM, "a big brown bear came lolloping over the mountain", null},
        };
    }

    @Test
    void testMask() {
        for (Object[] row : testMaskProvider()) {
            String value = (String) row[0];
            String maskType = (String) row[1];
            String customValue = (String) row[2];
            String result = (String) row[3];
            assertEquals(result, DataMasker.maskString(value, maskType, customValue));
        }
    }

    @Test
    void testShaMask() {
        for (Object[] row : shaProvider()) {
            String value = (String) row[0];
            String masked = DataMasker.maskString(value, MASK_HASH, null);
            assertEquals(64, masked.length());
            assertTrue(isHexadecimal(masked));
        }
    }

    @Test
    void testInvalidMask() {
        for (Object[] row : badMasks()) {
            String value = (String) row[0];
            String maskType = (String) row[1];
            String customValue = (String) row[2];
            assertThrows(MaskingException.class, () -> DataMasker.maskString(value, maskType, customValue));
        }
    }

    @Test
    void testMaskYear() {
        for (Object[] row : dateformats()) {
            String value = (String) row[0];
            String maskType = (String) row[1];
            String customValue = (String) row[2];
            String result = (String) row[3];
            assertEquals(result, DataMasker.maskString(value, maskType, customValue));
        }
    }

    @Test
    void testMaskYearBad() {
        for (Object[] row : dateformatsBad()) {
            String value = (String) row[0];
            String maskType = (String) row[1];
            String customValue = (String) row[2];
            assertThrows(MaskingException.class, () -> DataMasker.maskString(value, maskType, customValue));
        }
    }

    @Test
    void testMaskBooleans() {
        for (Object[] row : booleans()) {
            Boolean value = (Boolean) row[0];
            String maskType = (String) row[1];
            String customValue = (String) row[2];
            Boolean result = (Boolean) row[3];
            assertEquals(result, DataMasker.maskBoolean(value, maskType, customValue));
        }
    }

    @Test
    void testMaskBooleansBad() {
        for (Object[] row : booleansBad()) {
            Boolean value = (Boolean) row[0];
            String maskType = (String) row[1];
            String customValue = (String) row[2];
            assertThrows(MaskingException.class, () -> DataMasker.maskBoolean(value, maskType, customValue));
        }
    }

    @Test
    void testNumbers() {
        for (Object[] row : numbers()) {
            Number value = (Number) row[0];
            String maskType = (String) row[1];
            String customValue = (String) row[2];
            Number result = (Number) row[3];
            Number masked = DataMasker.maskNumber(value, maskType, customValue);
            assertEquals(String.valueOf(result), String.valueOf(masked));
        }
    }

    @Test
    void testNumbersBad() {
        for (Object[] row : numbersBad()) {
            Number value = (Number) row[0];
            String maskType = (String) row[1];
            String customValue = (String) row[2];
            assertThrows(MaskingException.class, () -> DataMasker.maskNumber(value, maskType, customValue));
        }
    }

    private boolean isHexadecimal(String input) {
        final Matcher matcher = HEXADECIMAL_PATTERN.matcher(input);
        return matcher.matches();
    }
}
