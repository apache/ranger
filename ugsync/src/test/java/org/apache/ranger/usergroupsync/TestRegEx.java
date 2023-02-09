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

package org.apache.ranger.usergroupsync;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class TestRegEx {
	protected String userNameBaseProperty = "ranger.usersync.mapping.username.regex";
    protected String groupNameBaseProperty = "ranger.usersync.mapping.groupname.regex";
    protected String mappingSeparator = "/";
    protected RegEx userNameRegEx = null;
    protected RegEx groupNameRegEx = null;
    List<String> userRegexPatterns = null;
    List<String> groupRegexPatterns = null;

	@Before
	public void setUp() throws Exception {
		userNameRegEx = new RegEx();
        //userNameRegEx.init(userNameBaseProperty);
        userRegexPatterns = new ArrayList<String>();
        groupNameRegEx = new RegEx();
        //groupNameRegEx.init(groupNameBaseProperty);
        groupRegexPatterns = new ArrayList<String>();
	}

	@Test
    public void testUserNameTransform() throws Throwable {
        userRegexPatterns.add("s/\\s/_/");
        userNameRegEx.populateReplacementPatterns(userNameBaseProperty, userRegexPatterns, mappingSeparator);
        assertEquals("test_user", userNameRegEx.transform("test user"));
    }

    @Test
    public void testGroupNameTransform() throws Throwable {
        groupRegexPatterns.add("s/\\s/_/g");
        groupRegexPatterns.add("s/_/\\$/g");
        groupNameRegEx.populateReplacementPatterns(groupNameBaseProperty, groupRegexPatterns, mappingSeparator);
        assertEquals("ldap$grp", groupNameRegEx.transform("ldap grp"));
    }

    @Test
    public void testEmptyTransform() {
        assertEquals("test user", userNameRegEx.transform("test user"));
        assertEquals("ldap grp", groupNameRegEx.transform("ldap grp"));
    }

    @Test
    public void testTransform() throws Throwable {
        userRegexPatterns.add("s/\\s/_/g");
        groupRegexPatterns.add("s/\\s/_/g");
        userNameRegEx.populateReplacementPatterns(userNameBaseProperty, userRegexPatterns, mappingSeparator);
        groupNameRegEx.populateReplacementPatterns(groupNameBaseProperty, groupRegexPatterns, mappingSeparator);
        assertEquals("test_user", userNameRegEx.transform("test user"));
        assertEquals("ldap_grp", groupNameRegEx.transform("ldap grp"));
    }

    @Test
    public void testTransform1() throws Throwable {
        userRegexPatterns.add("s/\\\\/ /g");
        userRegexPatterns.add("s//_/g");
        userNameRegEx.populateReplacementPatterns(userNameBaseProperty, userRegexPatterns, mappingSeparator);
        groupRegexPatterns.add("s/\\s/\\$/g");
        groupRegexPatterns.add("s/\\s");
        groupRegexPatterns.add("s/\\$//g");
        groupNameRegEx.populateReplacementPatterns(groupNameBaseProperty, groupRegexPatterns, mappingSeparator);
        assertEquals("test user", userNameRegEx.transform("test\\user"));
        assertEquals("ldapgrp", groupNameRegEx.transform("ldap grp"));
    }

    @Test
    public void testTransformWithSeparators() throws Throwable {
        String[] separators = {"%", "#", "&", "!", "@", "-", "~", "=", ",", " "};
        for (String separator : separators) {
            userRegexPatterns = new ArrayList<String>();
            userRegexPatterns.add(String.format("s%sdark%sDE/dark%sg", separator, separator, separator));
            userNameRegEx.populateReplacementPatterns(userNameBaseProperty, userRegexPatterns, separator);
            assertEquals("DE/dark_knight_admin", userNameRegEx.transform("dark_knight_admin"));
        }
    }

    @Test
    public void testUsernamePrefix() throws Throwable {
        // appends PR/ to the beginning
        String separator = "#";
        userRegexPatterns = Collections.singletonList("s#^(.*)#PR/$1#g");
        userNameRegEx.populateReplacementPatterns(userNameBaseProperty, userRegexPatterns, separator);
        assertEquals("PR/mew_two", userNameRegEx.transform("mew_two"));
        assertEquals("PR/dragoon", userNameRegEx.transform("dragoon"));
        assertEquals("PR/pikachu", userNameRegEx.transform("pikachu"));
        assertEquals("PR/dialga", userNameRegEx.transform("dialga"));
    }

    @Test
    public void testUsernameSuffix() throws Throwable {
        // appends _ty to the end
        String separator = "#";
        userRegexPatterns = Collections.singletonList("s#^(.*)#$1_ty#g");
        userNameRegEx.populateReplacementPatterns(userNameBaseProperty, userRegexPatterns, separator);
        assertEquals("mew_ty", userNameRegEx.transform("mew"));
        assertEquals("onix_ty", userNameRegEx.transform("onix"));
    }

}
