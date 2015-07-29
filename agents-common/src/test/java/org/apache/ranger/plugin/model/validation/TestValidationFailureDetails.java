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

package org.apache.ranger.plugin.model.validation;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Created by alal on 6/17/15.
 */
public class TestValidationFailureDetails {

    @Test
    public void test1() {
        String[] templates = new String[] {
                "The {field}, was missing and sub-field {sub-field} was mssing, too. Validation failed due to {reason}", // pattern at end.
                "{field}, was missing and sub-field {sub-field} was mssing, too. Validation failed due to {reason}.",    // pattern at start but not end.
                "The {field}, was missing and sub-field {sub-field} was mssing, too. Validation failed due to {missing}.",    // unknown substitute
                "Template does not have field, but had {sub-field} along with a {reason} and a sprious field named {missing}.",    // unknown substitute
        };

        ValidationFailureDetails failureDetails = new ValidationFailureDetails("id", "subType", false, false, false, "foo-bar");

        String[] results = new String[] {
                "The id, was missing and sub-field subType was mssing, too. Validation failed due to foo-bar", // pattern at end.
                "id, was missing and sub-field subType was mssing, too. Validation failed due to foo-bar.",    // pattern at start but not end.
                "The id, was missing and sub-field subType was mssing, too. Validation failed due to {missing}.",    // unknown substitute
                "Template does not have field, but had subType along with a foo-bar and a sprious field named {missing}.",    // unknown substitute
        };

        for (int i = 0; i < templates.length; i++) {
            String result = failureDetails.substituteVariables(templates[i]);
            assertEquals(results[i], result);
        }
    }
}