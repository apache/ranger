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

package org.apache.ranger.plugin.conditionevaluator;

import org.apache.ranger.authorization.utils.TestStringUtil;
import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerRequestScriptEvaluator;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ScriptEngineUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngine;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RangerRequestScriptEvaluatorTest {
    final ScriptEngine scriptEngine = ScriptEngineUtil.createScriptEngine(null);

    @Test
    public void testRequestAttributes() {
        RangerTag                    tagPII    = new RangerTag("PII", Collections.singletonMap("attr1", "PII_value"));
        RangerTag                    tagPCI    = new RangerTag("PCI", Collections.singletonMap("attr1", "PCI_value"));
        RangerAccessRequest          request   = createRequest("test-user", new HashSet<>(Arrays.asList("test-group1", "test-group2")), new HashSet<>(Arrays.asList("test-role1", "test-role2")), Arrays.asList(tagPII, tagPCI));
        RangerRequestScriptEvaluator evaluator = new RangerRequestScriptEvaluator(request, scriptEngine);

        Assertions.assertEquals("test-group1,test-group2", evaluator.evaluateScript("UG_NAMES_CSV"), "test: UG_NAMES_CSV");
        Assertions.assertEquals("test-role1,test-role2", evaluator.evaluateScript("UR_NAMES_CSV"), "test: UR_NAMES_CSV");
        Assertions.assertEquals("PCI,PII", evaluator.evaluateScript("TAG_NAMES_CSV"), "test: TAG_NAMES_CSV");
        Assertions.assertEquals("state", evaluator.evaluateScript("USER_ATTR_NAMES_CSV"), "test: USER_ATTR_NAMES_CSV");
        Assertions.assertEquals("dept,site", evaluator.evaluateScript("UG_ATTR_NAMES_CSV"), "test: UG_ATTR_NAMES_CSV");
        Assertions.assertEquals("attr1", evaluator.evaluateScript("TAG_ATTR_NAMES_CSV"), "test: TAG_ATTR_NAMES_CSV");
        Assertions.assertEquals("ENGG,PROD", evaluator.evaluateScript("GET_UG_ATTR_CSV('dept')"), "test: GET_UG_ATTR_CSV('dept')");
        Assertions.assertEquals("10,20", evaluator.evaluateScript("GET_UG_ATTR_CSV('site')"), "test: GET_UG_ATTR_CSV('site')");
        Assertions.assertEquals("PCI_value,PII_value", evaluator.evaluateScript("GET_TAG_ATTR_CSV('attr1')"), "test: GET_TAG_ATTR_CSV('attr1')");

        Assertions.assertEquals("'test-group1','test-group2'", evaluator.evaluateScript("UG_NAMES_Q_CSV"), "test: UG_NAMES_Q_CSV");
        Assertions.assertEquals("'test-role1','test-role2'", evaluator.evaluateScript("UR_NAMES_Q_CSV"), "test: UR_NAMES_Q_CSV");
        Assertions.assertEquals("'PCI','PII'", evaluator.evaluateScript("TAG_NAMES_Q_CSV"), "test: TAG_NAMES_Q_CSV");
        Assertions.assertEquals("'state'", evaluator.evaluateScript("USER_ATTR_NAMES_Q_CSV"), "test: USER_ATTR_NAMES_Q_CSV");
        Assertions.assertEquals("'dept','site'", evaluator.evaluateScript("UG_ATTR_NAMES_Q_CSV"), "test: UG_ATTR_NAMES_Q_CSV");
        Assertions.assertEquals("'attr1'", evaluator.evaluateScript("TAG_ATTR_NAMES_Q_CSV"), "test: TAG_ATTR_NAMES_Q_CSV");
        Assertions.assertEquals("'ENGG','PROD'", evaluator.evaluateScript("GET_UG_ATTR_Q_CSV('dept')"), "test: GET_UG_ATTR_Q_CSV('dept')");
        Assertions.assertEquals("'10','20'", evaluator.evaluateScript("GET_UG_ATTR_Q_CSV('site')"), "test: GET_UG_ATTR_Q_CSV('site')");
        Assertions.assertEquals("'PCI_value','PII_value'", evaluator.evaluateScript("GET_TAG_ATTR_Q_CSV('attr1')"), "test: GET_TAG_ATTR_Q_CSV('attr1')");

        Assertions.assertTrue((Boolean) evaluator.evaluateScript("USER._name == 'test-user'"), "test: USER._name is 'test-user'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("HAS_USER_ATTR('state')"), "test: HAS_USER_ATTR(state)");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("HAS_USER_ATTR('notExists')"), "test: HAS_USER_ATTR(notExists)");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("USER['state'] == 'CA'"), "test: USER['state'] is 'CA'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("USER.state == 'CA'"), "test: USER.state is 'CA'");

        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_IN_GROUP('test-group1')"), "test: IS_IN_GROUP(test-group1)");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_IN_GROUP('test-group2')"), "test: IS_IN_GROUP(test-group2)");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("IS_IN_GROUP('notExists')"), "test: IS_IN_GROUP(notExists)");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_IN_ANY_GROUP"), "test: IS_IN_ANY_GROUP");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("IS_NOT_IN_ANY_GROUP"), "test: IS_NOT_IN_ANY_GROUP");

        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UG['test-group1'].dept == 'ENGG'"), "test: UG['test-group1'].dept is 'ENGG'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UG['test-group1'].site == 10"), "test: UG['test-group1'].site is 10");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UG['test-group2'].dept == 'PROD'"), "test: UG['test-group2'].dept is 'PROD'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UG['test-group2'].site == 20"), "test: UG['test-group2'].site is 20");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UG['test-group3'] == null"), "test: UG['test-group3'] is null");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UG['test-group1'].notExists == null"), "test: UG['test-group1'].notExists is null");

        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_IN_ROLE('test-role1')"), "test: IS_IN_ROLE(test-role1)");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_IN_ROLE('test-role2')"), "test: IS_IN_ROLE(test-role2)");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("IS_IN_ROLE('notExists')"), "test: IS_IN_ROLE(notExists)");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_IN_ANY_ROLE"), "test: IS_IN_ANY_ROLE");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("IS_NOT_IN_ANY_ROLE"), "test: IS_NOT_IN_ANY_ROLE");

        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UGA.sVal['dept'] == 'ENGG'"), "test: UGA.sVal['dept'] is 'ENGG'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UGA.sVal['site'] == 10"), "test: UGA.sVal['site'] is 10");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UGA.sVal['notExists'] == null"), "test: UGA.sVal['notExists'] is null");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("J(UGA.mVal['dept']) == '[\"ENGG\",\"PROD\"]'"), "test: UGA.mVal['dept'] is [\"ENGG\", \"PROD\"]");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("J(UGA.mVal['site']) == '[\"10\",\"20\"]'"), "test: UGA.mVal['site'] is [10, 20]");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UGA.mVal['notExists'] == null"), "test: UGA.mVal['notExists'] is null");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UGA.mVal['dept'].indexOf('ENGG') != -1"), "test: UGA.mVal['dept'] has 'ENGG'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UGA.mVal['dept'].indexOf('PROD') != -1"), "test: UGA.mVal['dept'] has 'PROD'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("UGA.mVal['dept'].indexOf('EXEC') == -1"), "test: UGA.mVal['dept'] doesn't have 'EXEC'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("HAS_UG_ATTR('dept')"), "test: HAS_UG_ATTR(dept)");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("HAS_UG_ATTR('site')"), "test: HAS_UG_ATTR(site)");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("HAS_UG_ATTR('notExists')"), "test: HAS_UG_ATTR(notExists)");

        Assertions.assertTrue((Boolean) evaluator.evaluateScript("REQ.accessType == 'select'"), "test: REQ.accessTyp is 'select'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("REQ.action == 'query'"), "test: REQ.action is 'query'");

        Assertions.assertTrue((Boolean) evaluator.evaluateScript("RES._ownerUser == 'testUser'"), "test: RES._ownerUser is 'testUser'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("RES.database == 'db1'"), "test: RES.database is 'db1'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("RES.table == 'tbl1'"), "test: RES.table is 'tbl1'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("RES.column == 'col1'"), "test: RES.column is 'col1'");

        Assertions.assertTrue((Boolean) evaluator.evaluateScript("TAG._type == 'PII'"), "test: TAG._type is 'PII'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("TAG.attr1 == 'PII_value'"), "test: TAG.attr1 is 'PII_value'");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("Object.keys(TAGS).length == 2"), "test: TAGS.length is 2");
        Assertions.assertEquals("PII_value", evaluator.evaluateScript("TAGS['PII'].attr1"), "test: TAG PII has attr1=PII_value");
        Assertions.assertEquals("PCI_value", evaluator.evaluateScript("TAGS['PCI'].attr1"), "test: TAG PCI has attr1=PCI_value");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("TAGS['PII'].notExists == undefined"), "test: TAG PII doesn't have PII.notExists");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("HAS_TAG_ATTR('attr1')"), "test: HAS_TAG_ATTR(attr1)");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("HAS_TAG_ATTR('notExists')"), "test: HAS_TAG_ATTR(notExists)");

        Assertions.assertTrue((Boolean) evaluator.evaluateScript("TAGNAMES.length == 2"), "test: TAGNAMES.length is 2");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("HAS_TAG('PII')"), "test: HAS_TAG(PII)");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("HAS_TAG('PCI')"), "test: HAS_TAG(PCI)");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("HAS_TAG('notExists')"), "test: HAS_TAG(notExists)");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("HAS_ANY_TAG"), "test: HAS_ANY_TAG");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("HAS_NO_TAG"), "test: HAS_NO_TAG");

        Assertions.assertEquals("PCI,PII", evaluator.evaluateScript("GET_TAG_NAMES()"), "GET_TAG_NAMES()");
        Assertions.assertEquals("PCI,PII", evaluator.evaluateScript("GET_TAG_NAMES(null)"), "GET_TAG_NAMES(null)");
        Assertions.assertEquals("PCI|PII", evaluator.evaluateScript("GET_TAG_NAMES(null, '|')"), "GET_TAG_NAMES(null, '|')");
        Assertions.assertEquals("PCIPII", evaluator.evaluateScript("GET_TAG_NAMES(null, null)"), "GET_TAG_NAMES(null, null)");

        Assertions.assertEquals("'PCI','PII'", evaluator.evaluateScript("GET_TAG_NAMES_Q()"), "GET_TAG_NAMES_Q()");
        Assertions.assertEquals("'PCI','PII'", evaluator.evaluateScript("GET_TAG_NAMES_Q(null)"), "GET_TAG_NAMES_Q(null)");
        Assertions.assertEquals("'PCI'|'PII'", evaluator.evaluateScript("GET_TAG_NAMES_Q(null, '|')"), "GET_TAG_NAMES_Q(null, '|')");
        Assertions.assertEquals("'PCI''PII'", evaluator.evaluateScript("GET_TAG_NAMES_Q(null, null)"), "GET_TAG_NAMES_Q(null, null)");
        Assertions.assertEquals("PCI|PII", evaluator.evaluateScript("GET_TAG_NAMES_Q(null, '|', null)"), "GET_TAG_NAMES_Q(null, '|', null)");
        Assertions.assertEquals("{PCI},{PII}", evaluator.evaluateScript("GET_TAG_NAMES_Q(null, ',', '{', '}')"), "GET_TAG_NAMES_Q(null, ',', '{', '}')");

        Assertions.assertEquals("attr1", evaluator.evaluateScript("GET_TAG_ATTR_NAMES()"), "GET_TAG_ATTR_NAMES()");
        Assertions.assertEquals("attr1", evaluator.evaluateScript("GET_TAG_ATTR_NAMES(null)"), "GET_TAG_ATTR_NAMES(null)");
        Assertions.assertEquals("attr1", evaluator.evaluateScript("GET_TAG_ATTR_NAMES(null, '|')"), "GET_TAG_ATTR_NAMES(null, '|',)");
        Assertions.assertEquals("attr1", evaluator.evaluateScript("GET_TAG_ATTR_NAMES(null, null)"), "GET_TAG_ATTR_NAMES(null, null)");

        Assertions.assertEquals("'attr1'", evaluator.evaluateScript("GET_TAG_ATTR_NAMES_Q()"), "GET_TAG_ATTR_NAMES_Q()");
        Assertions.assertEquals("'attr1'", evaluator.evaluateScript("GET_TAG_ATTR_NAMES_Q(null)"), "GET_TAG_ATTR_NAMES_Q(null)");
        Assertions.assertEquals("'attr1'", evaluator.evaluateScript("GET_TAG_ATTR_NAMES_Q(null, '|')"), "GET_TAG_ATTR_NAMES_Q(null, '|')");
        Assertions.assertEquals("'attr1'", evaluator.evaluateScript("GET_TAG_ATTR_NAMES_Q(null, null)"), "GET_TAG_ATTR_NAMES_Q(null, null)");
        Assertions.assertEquals("attr1", evaluator.evaluateScript("GET_TAG_ATTR_NAMES_Q(null, '|', null)"), "GET_TAG_ATTR_NAMES_Q(null, '|', null)");
        Assertions.assertEquals("{attr1}", evaluator.evaluateScript("GET_TAG_ATTR_NAMES_Q(null, ',', '{', '}')"), "GET_TAG_ATTR_NAMES_Q(null, ',', '{', '}')");

        Assertions.assertEquals("PCI_value,PII_value", evaluator.evaluateScript("GET_TAG_ATTR('attr1')"), "GET_TAG_ATTR('attr1')");
        Assertions.assertEquals("PCI_value,PII_value", evaluator.evaluateScript("GET_TAG_ATTR('attr1', null)"), "GET_TAG_ATTR('attr1', null)");
        Assertions.assertEquals("PCI_value|PII_value", evaluator.evaluateScript("GET_TAG_ATTR('attr1', null, '|')"), "GET_TAG_ATTR('attr1', null, '|')");
        Assertions.assertEquals("PCI_valuePII_value", evaluator.evaluateScript("GET_TAG_ATTR('attr1', null, null)"), "GET_TAG_ATTR('attr1', null, null)");

        Assertions.assertEquals("'PCI_value','PII_value'", evaluator.evaluateScript("GET_TAG_ATTR_Q('attr1')"), "GET_TAG_ATTR_Q('attr1')");
        Assertions.assertEquals("'PCI_value','PII_value'", evaluator.evaluateScript("GET_TAG_ATTR_Q('attr1', null)"), "GET_TAG_ATTR_Q('attr1', null)");
        Assertions.assertEquals("'PCI_value''PII_value'", evaluator.evaluateScript("GET_TAG_ATTR_Q('attr1', null, null)"), "GET_TAG_ATTR_Q('attr1', null, null)");
        Assertions.assertEquals("'PCI_value'|'PII_value'", evaluator.evaluateScript("GET_TAG_ATTR_Q('attr1', null, '|')"), "GET_TAG_ATTR_Q('attr1', null, '|')");
        Assertions.assertEquals("PCI_value,PII_value", evaluator.evaluateScript("GET_TAG_ATTR_Q('attr1', null, ',', null)"), "GET_TAG_ATTR_Q('attr1', null, ',', null)");
        Assertions.assertEquals("{PCI_value},{PII_value}", evaluator.evaluateScript("GET_TAG_ATTR_Q('attr1', null, ',', '{', '}')"), "GET_TAG_ATTR_Q('attr1', null, ',', '{', '}')");

        Assertions.assertEquals("test-group1,test-group2", evaluator.evaluateScript("GET_UG_NAMES()"), "GET_UG_NAMES()");
        Assertions.assertEquals("test-group1,test-group2", evaluator.evaluateScript("GET_UG_NAMES(null)"), "GET_UG_NAMES(null)");
        Assertions.assertEquals("test-group1|test-group2", evaluator.evaluateScript("GET_UG_NAMES(null, '|')"), "GET_UG_NAMES(null, '|')");
        Assertions.assertEquals("test-group1test-group2", evaluator.evaluateScript("GET_UG_NAMES(null, null)"), "GET_UG_NAMES(null, null)");

        Assertions.assertEquals("'test-group1','test-group2'", evaluator.evaluateScript("GET_UG_NAMES_Q()"), "GET_UG_NAMES_Q()");
        Assertions.assertEquals("'test-group1','test-group2'", evaluator.evaluateScript("GET_UG_NAMES_Q(null)"), "GET_UG_NAMES_Q(null)");
        Assertions.assertEquals("'test-group1''test-group2'", evaluator.evaluateScript("GET_UG_NAMES_Q(null, null)"), "GET_UG_NAMES_Q(null, null)");
        Assertions.assertEquals("'test-group1'|'test-group2'", evaluator.evaluateScript("GET_UG_NAMES_Q(null, '|')"), "GET_UG_NAMES_Q(null, '|')");
        Assertions.assertEquals("test-group1,test-group2", evaluator.evaluateScript("GET_UG_NAMES_Q(null, ',', null)"), "GET_UG_NAMES_Q(null, ',', null)");
        Assertions.assertEquals("{test-group1},{test-group2}", evaluator.evaluateScript("GET_UG_NAMES_Q(null, ',', '{', '}')"), "GET_UG_NAMES_Q(null, ',', '{', '}')");

        Assertions.assertEquals("dept,site", evaluator.evaluateScript("GET_UG_ATTR_NAMES()"), "GET_UG_ATTR_NAMES()");
        Assertions.assertEquals("dept,site", evaluator.evaluateScript("GET_UG_ATTR_NAMES(null)"), "GET_UG_ATTR_NAMES(null)");
        Assertions.assertEquals("dept|site", evaluator.evaluateScript("GET_UG_ATTR_NAMES(null, '|')"), "GET_UG_ATTR_NAMES(null, '|')");
        Assertions.assertEquals("deptsite", evaluator.evaluateScript("GET_UG_ATTR_NAMES(null, null)"), "GET_UG_ATTR_NAMES(null, null)");

        Assertions.assertEquals("'dept','site'", evaluator.evaluateScript("GET_UG_ATTR_NAMES_Q()"), "GET_UG_ATTR_NAMES_Q()");
        Assertions.assertEquals("'dept','site'", evaluator.evaluateScript("GET_UG_ATTR_NAMES_Q(null)"), "GET_UG_ATTR_NAMES_Q(null)");
        Assertions.assertEquals("'dept''site'", evaluator.evaluateScript("GET_UG_ATTR_NAMES_Q(null, null)"), "GET_UG_ATTR_NAMES_Q(null, null)");
        Assertions.assertEquals("'dept'|'site'", evaluator.evaluateScript("GET_UG_ATTR_NAMES_Q(null, '|')"), "GET_UG_ATTR_NAMES_Q(null, '|')");
        Assertions.assertEquals("dept,site", evaluator.evaluateScript("GET_UG_ATTR_NAMES_Q(null, ',', null)"), "GET_UG_ATTR_NAMES_Q(null, ',', null)");
        Assertions.assertEquals("{dept},{site}", evaluator.evaluateScript("GET_UG_ATTR_NAMES_Q(null, ',', '{', '}')"), "GET_UG_ATTR_NAMES_Q(null, ',', '{', '}')");

        Assertions.assertEquals("ENGG,PROD", evaluator.evaluateScript("GET_UG_ATTR('dept')"), "GET_UG_ATTR('dept')");
        Assertions.assertEquals("ENGG,PROD", evaluator.evaluateScript("GET_UG_ATTR('dept', null)"), "GET_UG_ATTR('dept', null)");
        Assertions.assertEquals("ENGG|PROD", evaluator.evaluateScript("GET_UG_ATTR('dept', null, '|')"), "GET_UG_ATTR('dept', null, '|')");
        Assertions.assertEquals("ENGGPROD", evaluator.evaluateScript("GET_UG_ATTR('dept', null, null)"), "GET_UG_ATTR('dept', null, null)");

        Assertions.assertEquals("'ENGG','PROD'", evaluator.evaluateScript("GET_UG_ATTR_Q('dept')"), "GET_UG_ATTR_Q('dept')");
        Assertions.assertEquals("'ENGG','PROD'", evaluator.evaluateScript("GET_UG_ATTR_Q('dept', null)"), "GET_UG_ATTR_Q('dept', null)");
        Assertions.assertEquals("'ENGG''PROD'", evaluator.evaluateScript("GET_UG_ATTR_Q('dept', null, null)"), "GET_UG_ATTR_Q('dept', null, null)");
        Assertions.assertEquals("'ENGG'|'PROD'", evaluator.evaluateScript("GET_UG_ATTR_Q('dept', null, '|')"), "GET_UG_ATTR_Q('dept', null, '|')");
        Assertions.assertEquals("ENGG,PROD", evaluator.evaluateScript("GET_UG_ATTR_Q('dept', null, ',', null)"), "GET_UG_ATTR_Q('dept', null, ',', null)");
        Assertions.assertEquals("{ENGG},{PROD}", evaluator.evaluateScript("GET_UG_ATTR_Q('dept', null, ',', '{', '}')"), "GET_UG_ATTR_Q('dept', null, ',', '{', '}')");

        Assertions.assertEquals("10,20", evaluator.evaluateScript("GET_UG_ATTR('site')"), "GET_UG_ATTR('site')");
        Assertions.assertEquals("10,20", evaluator.evaluateScript("GET_UG_ATTR('site', null)"), "GET_UG_ATTR('site', null)");
        Assertions.assertEquals("10|20", evaluator.evaluateScript("GET_UG_ATTR('site', null, '|')"), "GET_UG_ATTR('site', null, '|')");
        Assertions.assertEquals("1020", evaluator.evaluateScript("GET_UG_ATTR('site', null, null)"), "GET_UG_ATTR('site', null, null)");

        Assertions.assertEquals("'10','20'", evaluator.evaluateScript("GET_UG_ATTR_Q('site')"), "GET_UG_ATTR_Q('site')");
        Assertions.assertEquals("'10','20'", evaluator.evaluateScript("GET_UG_ATTR_Q('site', null)"), "GET_UG_ATTR_Q('site', null)");
        Assertions.assertEquals("'10''20'", evaluator.evaluateScript("GET_UG_ATTR_Q('site', null, null)"), "GET_UG_ATTR_Q('site', null, null)");
        Assertions.assertEquals("'10'|'20'", evaluator.evaluateScript("GET_UG_ATTR_Q('site', null, '|')"), "GET_UG_ATTR_Q('site', null, '|')");
        Assertions.assertEquals("10,20", evaluator.evaluateScript("GET_UG_ATTR_Q('site', null, ',', null)"), "GET_UG_ATTR_Q('site', null, ',', null)");
        Assertions.assertEquals("{10},{20}", evaluator.evaluateScript("GET_UG_ATTR_Q('site', null, ',', '{', '}')"), "GET_UG_ATTR_Q('site', null, ',', '{', '}')");

        Assertions.assertEquals("test-role1,test-role2", evaluator.evaluateScript("GET_UR_NAMES()"), "GET_UR_NAMES()");
        Assertions.assertEquals("test-role1,test-role2", evaluator.evaluateScript("GET_UR_NAMES(null)"), "GET_UR_NAMES(null)");
        Assertions.assertEquals("test-role1|test-role2", evaluator.evaluateScript("GET_UR_NAMES(null, '|')"), "GET_UR_NAMES(null, '|')");
        Assertions.assertEquals("test-role1test-role2", evaluator.evaluateScript("GET_UR_NAMES(null, null)"), "GET_UR_NAMES(null, null)");

        Assertions.assertEquals("'test-role1','test-role2'", evaluator.evaluateScript("GET_UR_NAMES_Q()"), "GET_UR_NAMES_Q()");
        Assertions.assertEquals("'test-role1','test-role2'", evaluator.evaluateScript("GET_UR_NAMES_Q(null)"), "GET_UR_NAMES_Q(null)");
        Assertions.assertEquals("'test-role1''test-role2'", evaluator.evaluateScript("GET_UR_NAMES_Q(null, null)"), "GET_UR_NAMES_Q(null, null)");
        Assertions.assertEquals("'test-role1'|'test-role2'", evaluator.evaluateScript("GET_UR_NAMES_Q(null, '|')"), "GET_UR_NAMES_Q(null, '|')");
        Assertions.assertEquals("test-role1,test-role2", evaluator.evaluateScript("GET_UR_NAMES_Q(null, ',', null)"), "GET_UR_NAMES_Q(null, ',', null)");
        Assertions.assertEquals("{test-role1},{test-role2}", evaluator.evaluateScript("GET_UR_NAMES_Q(null, ',', '{', '}')"), "GET_UR_NAMES_Q(null, ',', '{', '}')");

        Assertions.assertEquals("state", evaluator.evaluateScript("GET_USER_ATTR_NAMES()"), "GET_USER_ATTR_NAMES()");
        Assertions.assertEquals("state", evaluator.evaluateScript("GET_USER_ATTR_NAMES(null)"), "GET_USER_ATTR_NAMES(null)");
        Assertions.assertEquals("state", evaluator.evaluateScript("GET_USER_ATTR_NAMES(null, '|')"), "GET_USER_ATTR_NAMES(null, '|')");
        Assertions.assertEquals("state", evaluator.evaluateScript("GET_USER_ATTR_NAMES(null, null)"), "GET_USER_ATTR_NAMES(null, null)");

        Assertions.assertEquals("'state'", evaluator.evaluateScript("GET_USER_ATTR_NAMES_Q()"), "GET_USER_ATTR_NAMES_Q()");
        Assertions.assertEquals("'state'", evaluator.evaluateScript("GET_USER_ATTR_NAMES_Q(null)"), "GET_USER_ATTR_NAMES_Q(null)");
        Assertions.assertEquals("'state'", evaluator.evaluateScript("GET_USER_ATTR_NAMES_Q(null, null)"), "GET_USER_ATTR_NAMES_Q(null, null)");
        Assertions.assertEquals("'state'", evaluator.evaluateScript("GET_USER_ATTR_NAMES_Q(null, '|')"), "GET_USER_ATTR_NAMES_Q(null, '|')");
        Assertions.assertEquals("state", evaluator.evaluateScript("GET_USER_ATTR_NAMES_Q(null, ',', null)"), "GET_USER_ATTR_NAMES_Q(null, ',', null)");
        Assertions.assertEquals("{state}", evaluator.evaluateScript("GET_USER_ATTR_NAMES_Q(null, ',', '{', '}')"), "GET_USER_ATTR_NAMES_Q(null, ',', '{', '}')");

        Assertions.assertEquals("CA", evaluator.evaluateScript("GET_USER_ATTR('state')"), "GET_USER_ATTR('state')");
        Assertions.assertEquals("CA", evaluator.evaluateScript("GET_USER_ATTR('state', null)"), "GET_USER_ATTR('state', null)");
        Assertions.assertEquals("CA", evaluator.evaluateScript("GET_USER_ATTR('state', null, '|')"), "GET_USER_ATTR('state', null, '|')");
        Assertions.assertEquals("CA", evaluator.evaluateScript("GET_USER_ATTR('state', null, null)"), "GET_USER_ATTR('state', null, null)");

        Assertions.assertEquals("'CA'", evaluator.evaluateScript("GET_USER_ATTR_Q('state')"), "GET_USER_ATTR_Q('state')");
        Assertions.assertEquals("'CA'", evaluator.evaluateScript("GET_USER_ATTR_Q('state', null)"), "GET_USER_ATTR_Q('state', null)");
        Assertions.assertEquals("'CA'", evaluator.evaluateScript("GET_USER_ATTR_Q('state', null, null)"), "GET_USER_ATTR_Q('state', null, null)");
        Assertions.assertEquals("'CA'", evaluator.evaluateScript("GET_USER_ATTR_Q('state', null, '|')"), "GET_USER_ATTR_Q('state', null, '|')");
        Assertions.assertEquals("CA", evaluator.evaluateScript("GET_USER_ATTR_Q('state', null, ',', null)"), "GET_USER_ATTR_Q('state', null, ',', null)");
        Assertions.assertEquals("{CA}", evaluator.evaluateScript("GET_USER_ATTR_Q('state', null, ',', '{', '}')"), "GET_USER_ATTR_Q('state', null, ',', '{', '}')");
    }

    @Test
    public void testNonExistentValues() {
        RangerAccessRequest          request   = createRequest("test-user", Collections.emptySet(), Collections.emptySet(), Collections.emptyList());
        RangerRequestScriptEvaluator evaluator = new RangerRequestScriptEvaluator(request, scriptEngine);

        // empty TAG names
        Assertions.assertEquals("", evaluator.evaluateScript("GET_TAG_NAMES()"), "GET_TAG_NAMES()");
        Assertions.assertEquals("", evaluator.evaluateScript("GET_TAG_NAMES(null)"), "GET_TAG_NAMES(null)");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_TAG_NAMES('empty')"), "GET_TAG_NAMES('empty')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_TAG_NAMES('empty', '|')"), "GET_TAG_NAMES('empty', '|')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_TAG_NAMES('empty', null)"), "GET_TAG_NAMES('empty', null)");

        // empty TAG names
        Assertions.assertEquals("", evaluator.evaluateScript("GET_TAG_NAMES_Q()"), "GET_TAG_NAMES_Q()");
        Assertions.assertEquals("", evaluator.evaluateScript("GET_TAG_NAMES_Q(null)"), "GET_TAG_NAMES_Q(null)");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_TAG_NAMES_Q('empty')"), "GET_TAG_NAMES_Q('empty')");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_TAG_NAMES_Q('empty', ',')"), "GET_TAG_NAMES_Q('empty', ',')");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_TAG_NAMES_Q('empty', '|')"), "GET_TAG_NAMES_Q('empty', '|', null)");
        Assertions.assertEquals("{empty}", evaluator.evaluateScript("GET_TAG_NAMES_Q('empty', ',', '{', '}')"), "GET_TAG_NAMES_Q('empty', ',', '{', '}')");

        // empty UG names
        Assertions.assertEquals("", evaluator.evaluateScript("GET_UG_NAMES()"), "GET_UG_NAMES()");
        Assertions.assertEquals("", evaluator.evaluateScript("GET_UG_NAMES(null)"), "GET_UG_NAMES(null)");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_UG_NAMES('empty')"), "GET_UG_NAMES('empty')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_UG_NAMES('empty', '|')"), "GET_UG_NAMES('empty', '|')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_UG_NAMES('empty', null)"), "GET_UG_NAMES('empty', null)");

        // empty UG names
        Assertions.assertEquals("", evaluator.evaluateScript("GET_UG_NAMES_Q()"), "GET_UG_NAMES_Q()");
        Assertions.assertEquals("", evaluator.evaluateScript("GET_UG_NAMES_Q(null)"), "GET_UG_NAMES_Q(null)");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_UG_NAMES_Q('empty')"), "GET_UG_NAMES_Q('empty')");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_UG_NAMES_Q('empty', ',')"), "GET_UG_NAMES_Q('empty', ',')");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_UG_NAMES_Q('empty', '|')"), "GET_UG_NAMES_Q('empty', '|', null)");
        Assertions.assertEquals("{empty}", evaluator.evaluateScript("GET_UG_NAMES_Q('empty', ',', '{', '}')"), "GET_UG_NAMES_Q('empty', ',', '{', '}')");

        // empty UR names
        Assertions.assertEquals("", evaluator.evaluateScript("GET_UR_NAMES()"), "GET_UR_NAMES()");
        Assertions.assertEquals("", evaluator.evaluateScript("GET_UR_NAMES(null)"), "GET_UR_NAMES(null)");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_UR_NAMES('empty')"), "GET_UR_NAMES('empty')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_UR_NAMES('empty', '|')"), "GET_UR_NAMES('empty', '|')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_UR_NAMES('empty', null)"), "GET_UR_NAMES('empty', null)");

        // empty UR names
        Assertions.assertEquals("", evaluator.evaluateScript("GET_UR_NAMES_Q()"), "GET_UR_NAMES_Q()");
        Assertions.assertEquals("", evaluator.evaluateScript("GET_UR_NAMES_Q(null)"), "GET_UR_NAMES_Q(null)");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_UR_NAMES_Q('empty')"), "GET_UR_NAMES_Q('empty')");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_UR_NAMES_Q('empty', ',')"), "GET_UR_NAMES_Q('empty', ',')");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_UR_NAMES_Q('empty', '|')"), "GET_UR_NAMES_Q('empty', '|', null)");
        Assertions.assertEquals("{empty}", evaluator.evaluateScript("GET_UR_NAMES_Q('empty', ',', '{', '}')"), "GET_UR_NAMES_Q('empty', ',', '{', '}')");

        // non-existent attribute
        Assertions.assertEquals("", evaluator.evaluateScript("GET_TAG_ATTR('noattr')"), "GET_TAG_ATTR('noattr')");
        Assertions.assertEquals("", evaluator.evaluateScript("GET_TAG_ATTR('noattr', null)"), "GET_TAG_ATTR('noattr', null)");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_TAG_ATTR('noattr', 'empty')"), "GET_TAG_ATTR('noattr', 'empty')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_TAG_ATTR('noattr', 'empty', '|')"), "GET_TAG_ATTR('noattr', 'empty', '|')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_TAG_ATTR('noattr', 'empty', null)"), "GET_TAG_ATTR('noattr', 'empty', null)");

        // non-existent attribute
        Assertions.assertEquals("", evaluator.evaluateScript("GET_TAG_ATTR_Q('noattr')"), "GET_TAG_ATTR_Q('noattr')");
        Assertions.assertEquals("", evaluator.evaluateScript("GET_TAG_ATTR_Q('noattr', null)"), "GET_TAG_ATTR_Q('noattr', null)");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_TAG_ATTR_Q('noattr', 'empty')"), "GET_TAG_ATTR_Q('noattr', 'empty')");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_TAG_ATTR_Q('noattr', 'empty', ',')"), "GET_TAG_ATTR_Q('noattr', 'empty', ',')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_TAG_ATTR_Q('noattr', 'empty', '|', null)"), "GET_TAG_ATTR_Q('noattr', 'empty', '|', null)");
        Assertions.assertEquals("{empty}", evaluator.evaluateScript("GET_TAG_ATTR_Q('noattr', 'empty', ',', '{', '}')"), "GET_TAG_ATTR_Q('noattr', 'empty', ',', '{', '}')");

        // non-existent attribute
        Assertions.assertEquals("", evaluator.evaluateScript("GET_UG_ATTR('noattr')"), "GET_UG_ATTR('noattr')");
        Assertions.assertEquals("", evaluator.evaluateScript("GET_UG_ATTR('noattr', null)"), "GET_UG_ATTR('noattr', null)");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_UG_ATTR('noattr', 'empty', '|')"), "GET_UG_ATTR('noattr', 'empty', '|')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_UG_ATTR('noattr', 'empty', null)"), "GET_UG_ATTR('noattr', 'empty', null)");

        // non-existent attribute
        Assertions.assertEquals("", evaluator.evaluateScript("GET_UG_ATTR_Q('noattr')"), "GET_UG_ATTR_Q('noattr')");
        Assertions.assertEquals("", evaluator.evaluateScript("GET_UG_ATTR_Q('noattr', null)"), "GET_UG_ATTR_Q('noattr', null)");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_UG_ATTR_Q('noattr', 'empty', null)"), "GET_UG_ATTR_Q('noattr', 'empty', null)");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_UG_ATTR_Q('noattr', 'empty', '|')"), "GET_UG_ATTR_Q('noattr', 'empty', '|')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_UG_ATTR_Q('noattr', 'empty', ',', null)"), "GET_UG_ATTR_Q('noattr', 'empty', ',', null)");
        Assertions.assertEquals("{empty}", evaluator.evaluateScript("GET_UG_ATTR_Q('noattr', 'empty', ',', '{', '}')"), "GET_UG_ATTR_Q('noattr', 'empty', ',', '{', '}')");

        // non-existent attribute
        Assertions.assertEquals("", evaluator.evaluateScript("GET_USER_ATTR('noattr')"), "GET_USER_ATTR('noattr')");
        Assertions.assertEquals("", evaluator.evaluateScript("GET_USER_ATTR('noattr', null)"), "GET_USER_ATTR('noattr', null)");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_USER_ATTR('noattr', 'empty', '|')"), "GET_USER_ATTR('noattr', 'empty', '|')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_USER_ATTR('noattr', 'empty', null)"), "GET_USER_ATTR('noattr', 'empty', null)");

        // non-existent attribute
        Assertions.assertEquals("", evaluator.evaluateScript("GET_USER_ATTR_Q('noattr')"), "GET_USER_ATTR_Q('noattr')");
        Assertions.assertEquals("", evaluator.evaluateScript("GET_USER_ATTR_Q('noattr', null)"), "GET_USER_ATTR_Q('noattr', null)");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_USER_ATTR_Q('noattr', 'empty', null)"), "GET_USER_ATTR_Q('noattr', 'empty', null)");
        Assertions.assertEquals("'empty'", evaluator.evaluateScript("GET_USER_ATTR_Q('noattr', 'empty', '|')"), "GET_USER_ATTR_Q('noattr', 'empty', '|')");
        Assertions.assertEquals("empty", evaluator.evaluateScript("GET_USER_ATTR_Q('noattr', 'empty', ',', null)"), "GET_USER_ATTR_Q('noattr', 'empty', ',', null)");
        Assertions.assertEquals("{empty}", evaluator.evaluateScript("GET_USER_ATTR_Q('noattr', 'empty', ',', '{', '}')"), "GET_USER_ATTR_Q('noattr', 'empty', ',', '{', '}')");
    }

    @Test
    public void testIntersectsIncludes() {
        RangerTag                    tagPartners = new RangerTag("PARTNERS", Collections.singletonMap("names", "partner-1,partner-2"));
        RangerTag                    tagDepts    = new RangerTag("DEPTS", Collections.singletonMap("names", "ENGG,SALES"));
        RangerAccessRequest          request     = createRequest("test-user2", Collections.singleton("test-group2"), Collections.singleton("test-role2"), Arrays.asList(tagPartners, tagDepts));
        RangerRequestScriptEvaluator evaluator   = new RangerRequestScriptEvaluator(request, scriptEngine);

        Assertions.assertTrue((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].intersects(['sales'])"), "test: ['sales', 'mktg', 'products'].intersects(['sales'])");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].intersects(['mktg'])"), "test: ['sales', 'mktg', 'products'].intersects(['mktg'])");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].intersects(['products'])"), "test: ['sales', 'mktg', 'products'].intersects(['products'])");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].intersects(['sales', 'engineering'])"), "test: ['sales', 'mktg', 'products'].intersects(['sales', 'engineering'])");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].intersects(['mktg', 'engineering'])"), "test: ['sales', 'mktg', 'products'].intersects(['mktg', 'engineering'])");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].intersects(['products', 'engineering'])"), "test: ['sales', 'mktg', 'products'].intersects(['products', 'engineering'])");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].intersects(['engineering', 'hr', 'sales'])"), "test: ['sales', 'mktg', 'products'].intersects(['engineering', 'hr', 'sales'])");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].intersects(['engineering'])"), "test: ['sales', 'mktg', 'products'].intersects(['engineering'])");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].intersects([])"), "test: ['sales', 'mktg', 'products'].intersects([])");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].intersects(null)"), "test: ['sales', 'mktg', 'products'].intersects(null)");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("[].intersects(['engineering'])"), "test: [].intersects(['engineering'])");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("[].intersects([])"), "test: [].intersects([])");
        /*
         TAGS.PARTNERS.names = partner-1,partner-2
         USER.partners       = partner-1,partner-2,partners-3
         */
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("HAS_USER_ATTR('partners') && TAGS.PARTNERS.names.split(',').intersects(USER.partners.split(','))"), "test: TAGS.PARTNERS.names.split(',').intersects(USER.partners.split(','))");

        Assertions.assertTrue((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].includes('sales')"), "test: ['sales', 'mktg', 'products'].includes('sales')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].includes('mktg')"), "test: ['sales', 'mktg', 'products'].includes('mktg')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].includes('products')"), "test: ['sales', 'mktg', 'products'].includes('products')");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].includes('engineering')"), "test: ['sales', 'mktg', 'products'].includes(['engineering'])");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].includes('')"), "test: ['sales', 'mktg', 'products'].includes('')");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("['sales', 'mktg', 'products'].includes(null)"), "test: ['sales', 'mktg', 'products'].includes(null)");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("[].includes('engineering')"), "test: [].includes('engineering')");
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("[].includes([])"), "test: [].includes([])");
        /*
         TAGS.DEPTS.names = ENGG,SALES
         USER.dept        = ENGG
         */
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("TAGS.DEPTS.names.split(',').includes(USER.dept)"), "test: TAGS.DEPTS.names.split(',').includes(USER.dept)");

        // switch context to user test-user3, who has different attribute values for partners and dept
        request   = createRequest("test-user3", Collections.singleton("test-group3"), Collections.singleton("test-role3"), Arrays.asList(tagPartners, tagDepts));
        evaluator = new RangerRequestScriptEvaluator(request, scriptEngine);

        /*
         TAGS.PARTNERS.names = partner-1,partner-2
         USER.partners       = partner-3
         */
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("HAS_USER_ATTR('partners') && TAGS.PARTNERS.names.split(',').intersects(USER.partners.split(','))"), "test: TAGS.PARTNERS.names.split(',').intersects(USER.partners.split(','))");

        /*
         TAGS.DEPTS.names = ENGG,SALES
         USER.dept        = MKTG
         */
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("TAGS.DEPTS.names.split(',').includes(USER.dept)"), "test: TAGS.DEPTS.names.split(',').includes(USER.dept)");

        // switch context to user test-user4, who doesn't have attribute partners and dept
        request   = createRequest("test-user4", Collections.singleton("test-group4"), Collections.singleton("test-role4"), Arrays.asList(tagPartners, tagDepts));
        evaluator = new RangerRequestScriptEvaluator(request, scriptEngine);

        /*
         TAGS.PARTNERS.names = partner-1,partner-2
         USER.partners       = null
         */
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("HAS_USER_ATTR('partners') && TAGS.PARTNERS.names.split(',').intersects(USER.partners.split(','))"), "test: TAGS.PARTNERS.names.split(',').intersects(USER.partners.split(','))");

        /*
         TAGS.DEPTS.names = ENGG,SALES
         USER.dept        = null
         */
        Assertions.assertFalse((Boolean) evaluator.evaluateScript("TAGS.DEPTS.names.split(',').includes(USER.dept)"), "test: TAGS.DEPTS.names.split(',').includes(USER.dept)");
    }

    @Test
    public void testBlockJavaClassReferences() {
        RangerAccessRequest          request   = createRequest("test-user", Collections.emptySet(), Collections.emptySet(), Collections.emptyList());
        RangerRequestScriptEvaluator evaluator = new RangerRequestScriptEvaluator(request, scriptEngine, false);

        String fileName = "/tmp/ctest1-" + System.currentTimeMillis();

        String[] scripts = new String[] {
                "java.lang.System.out.println(\"test\");",
                "java.lang.Runtime.getRuntime().exec(\"bash\");",
                "var newBindings=loadWithNewGlobal({'script':'this','name':'ctest'});this.context.setBindings(newBindings,100);var newEngine = this.__noSuchProperty__('engine');var e=newEngine.getFactory().getScriptEngine('-Dnashorn.args=--no-java=False');e.eval('java.lang.Runtime.getRuntime().exec(\"touch /tmp/ctest1\")')",
                "engine.eval('malicious code')",
                "var str = new java.lang.String('test'); str.length()",
                "var file = new java.io.File('" + fileName +  "'); file.createNewFile()",
        };

        for (String script : scripts) {
            Assertions.assertNull(evaluator.evaluateScript(script), "test: " + script);
        }

        File testFile = new File(fileName);
        Assertions.assertFalse(testFile.exists(), fileName + ": file should not have been created");
    }

    @Test
    public void testIsTimeMacros() {
        RangerAccessRequest          request   = createRequest("test-user", Collections.emptySet(), Collections.emptySet(), Collections.emptyList());
        RangerRequestScriptEvaluator evaluator = new RangerRequestScriptEvaluator(request, scriptEngine, false);

        // Date
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_AFTER('2020/01/01')"), "test: IS_ACCESS_TIME_AFTER('2020/01/01')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_AFTER('2020/01/01', 'GMT')"), "test: IS_ACCESS_TIME_AFTER('2020/01/01', 'GMT')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_BEFORE('2100/01/01')"), "test: IS_ACCESS_TIME_BEFORE('2100/01/01')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_BEFORE('2100/01/01', 'GMT')"), "test: IS_ACCESS_TIME_BEFORE('2100/01/01', 'GMT')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_BETWEEN('2010/01/01', '2100/01/01')"), "test: IS_ACCESS_TIME_BETWEEN('2010/01/01', '2100/01/01')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_BETWEEN('2010/01/01', '2100/01/01', 'GMT')"), "test: IS_ACCESS_TIME_BETWEEN('2010/01/01', '2100/01/01', 'GMT')");

        // Date hh:mm
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_AFTER('2020/01/01 15:00')"), "test: IS_ACCESS_TIME_AFTER('2020/01/01 15:00')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_AFTER('2020/01/01 15:00', 'GMT')"), "test: IS_ACCESS_TIME_AFTER('2020/01/01 15:00', 'GMT')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_BEFORE('2100/01/01 15:00')"), "test: IS_ACCESS_TIME_BEFORE('2100/01/01 15:00')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_BEFORE('2100/01/01 15:00', 'GMT')"), "test: IS_ACCESS_TIME_BEFORE('2100/01/01 15:00', 'GMT')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_BETWEEN('2010/01/01 15:00', '2100/01/01 15:00')"), "test: IS_ACCESS_TIME_BETWEEN('2010/01/01 15:00', '2100/01/01 15:00')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_BETWEEN('2010/01/01 15:00', '2100/01/01 15:00', 'GMT')"), "test: IS_ACCESS_TIME_BETWEEN('2010/01/01 15:00', '2100/01/01 15:00', 'GMT')");

        // Date hh:mm:ss
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_AFTER('2020/01/01 15:00:42')"), "test: IS_ACCESS_TIME_AFTER('2020/01/01 15:00:42')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_AFTER('2020/01/01 15:00:42', 'GMT')"), "test: IS_ACCESS_TIME_AFTER('2020/01/01 15:00:42', 'GMT')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_BEFORE('2100/01/01 15:00:42')"), "test: IS_ACCESS_TIME_BEFORE('2100/01/01 15:00:42')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_BEFORE('2100/01/01 15:00:42', 'GMT')"), "test: IS_ACCESS_TIME_BEFORE('2100/01/01 15:00:42', 'GMT')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_BETWEEN('2010/01/01 15:00:42', '2100/01/01 15:00:42')"), "test: IS_ACCESS_TIME_BETWEEN('2010/01/01 15:00:42', '2100/01/01 15:00:42')");
        Assertions.assertTrue((Boolean) evaluator.evaluateScript("IS_ACCESS_TIME_BETWEEN('2010/01/01 15:00:42', '2100/01/01 15:00:42', 'GMT')"), "test: IS_ACCESS_TIME_BETWEEN('2010/01/01 15:00:42', '2100/01/01 15:00:42', 'GMT')");
    }

    @Test
    public void testMultipleTagInstancesOfType() {
        List<RangerTag> tags = Arrays.asList(new RangerTag("PII", Collections.singletonMap("type", "email")),
                new RangerTag("PII", Collections.singletonMap("type", "phone")),
                new RangerTag("PII", Collections.emptyMap()),
                new RangerTag("PCI", Collections.singletonMap("kind", "pan")),
                new RangerTag("PCI", Collections.singletonMap("kind", "sad")),
                new RangerTag("PCI", null));
        RangerAccessRequest          request   = createRequest("test-user", Collections.emptySet(), Collections.emptySet(), tags);
        RangerRequestScriptEvaluator evaluator = new RangerRequestScriptEvaluator(request, scriptEngine);

        Object[][] tests = new Object[][] {
                {"TAG_NAMES_CSV", "PCI,PII"},
                {"TAG_ATTR_NAMES_CSV", "kind,type"},
                {"ctx.getAttributeValueForAllMatchingTags('PII', 'type')", Arrays.asList("email", "phone")},
                {"ctx.getAttributeValueForAllMatchingTags('PCI', 'kind')", Arrays.asList("pan", "sad")},
                {"ctx.getAttributeValueForAllMatchingTags('PII', 'kind')", Collections.emptyList()},
                {"ctx.getAttributeValueForAllMatchingTags('PCI', 'type')", Collections.emptyList()},
                {"ctx.getAttributeValueForAllMatchingTags('notag', 'noattr')", Collections.emptyList()},
                {"ctx.getAttributeValueForAllMatchingTags('notag', null)", null},
                {"ctx.getAttributeValueForAllMatchingTags(null, 'noattr')", null},
                {"ctx.getAttributeValueForAllMatchingTags(null, noull)", null}
        };

        for (Object[] test : tests) {
            String script   = (String) test[0];
            Object expected = test[1];
            Object actual   = evaluator.evaluateScript(script);

            Assertions.assertEquals(expected, actual, "test: " + script);
        }
    }

    @Test
    public void testGetAllTagTypes() {
        RangerAccessRequest          request   = createRequest("test-user", Collections.emptySet(), Collections.emptySet(), Collections.emptyList());
        RangerRequestScriptEvaluator evaluator = new RangerRequestScriptEvaluator(request, scriptEngine, false);

        Assertions.assertEquals(Collections.emptySet(), evaluator.evaluateScript("ctx.getAllTagTypes()"));

        request   = createRequest("test-user", Collections.emptySet(), Collections.emptySet(), Collections.singletonList(new RangerTag("PII", Collections.emptyMap())));
        evaluator = new RangerRequestScriptEvaluator(request, scriptEngine, false);

        Assertions.assertEquals(Collections.singleton("PII"), evaluator.evaluateScript("ctx.getAllTagTypes()"));

        request   = createRequest("test-user", Collections.emptySet(), Collections.emptySet(), Arrays.asList(new RangerTag("PII", Collections.emptyMap()), new RangerTag("PCI", Collections.emptyMap())));
        evaluator = new RangerRequestScriptEvaluator(request, scriptEngine, false);

        Assertions.assertEquals(new HashSet<>(Arrays.asList("PCI", "PII")), evaluator.evaluateScript("ctx.getAllTagTypes()"));
    }

    RangerAccessRequest createRequest(String userName, Set<String> userGroups, Set<String> userRoles, List<RangerTag> resourceTags) {
        RangerAccessResource resource = mock(RangerAccessResource.class);

        Map<String, Object> resourceMap = new HashMap<>();

        resourceMap.put("database", "db1");
        resourceMap.put("table", "tbl1");
        resourceMap.put("column", "col1");

        when(resource.getAsString()).thenReturn("db1/tbl1/col1");
        when(resource.getOwnerUser()).thenReturn("testUser");
        when(resource.getAsMap()).thenReturn(resourceMap);
        when(resource.getReadOnlyCopy()).thenReturn(resource);

        RangerAccessRequestImpl request = new RangerAccessRequestImpl();

        request.setResource(resource);
        request.setResourceMatchingScope(RangerAccessRequest.ResourceMatchingScope.SELF);
        request.setAccessType("select");
        request.setAction("query");
        request.setUser(userName);
        request.setUserGroups(userGroups);
        request.setUserRoles(userRoles);

        RangerAccessRequestUtil.setCurrentResourceInContext(request.getContext(), resource);

        if (resourceTags != null) {
            Set<RangerTagForEval> rangerTagForEvals = new HashSet<>();
            RangerTagForEval      currentTag        = null;

            for (RangerTag tag : resourceTags) {
                RangerTagForEval tagForEval = new RangerTagForEval(tag, RangerPolicyResourceMatcher.MatchType.SELF);

                rangerTagForEvals.add(tagForEval);

                if (currentTag == null) {
                    currentTag = tagForEval;
                }
            }

            RangerAccessRequestUtil.setRequestTagsInContext(request.getContext(), rangerTagForEvals);
            RangerAccessRequestUtil.setCurrentTagInContext(request.getContext(), currentTag);
        } else {
            RangerAccessRequestUtil.setRequestTagsInContext(request.getContext(), null);
        }

        RangerUserStore userStore = mock(RangerUserStore.class);

        RangerAccessRequestUtil.setRequestUserStoreInContext(request.getContext(), userStore);

        Map<String, Map<String, String>> userAttrMapping  = new HashMap<>();
        Map<String, Map<String, String>> groupAttrMapping = new HashMap<>();

        userAttrMapping.put("test-user", TestStringUtil.mapFromStrings("state", "CA"));
        userAttrMapping.put("test-user2", TestStringUtil.mapFromStrings("partners", "partner-1,partner-2,partner-3", "dept", "ENGG"));
        userAttrMapping.put("test-user3", TestStringUtil.mapFromStrings("partners", "partner-3", "dept", "MKTG"));
        groupAttrMapping.put("test-group1", TestStringUtil.mapFromStrings("dept", "ENGG", "site", "10"));
        groupAttrMapping.put("test-group2", TestStringUtil.mapFromStrings("dept", "PROD", "site", "20"));
        groupAttrMapping.put("test-group3", TestStringUtil.mapFromStrings("dept", "SALES", "site", "30"));

        when(userStore.getUserAttrMapping()).thenReturn(userAttrMapping);
        when(userStore.getGroupAttrMapping()).thenReturn(groupAttrMapping);

        return request;
    }
}
