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

package org.apache.ranger.plugin.model;

import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class TestRangerPolicy {
    @Test
    public void test_01_Policy_SetListMethods() {
        RangerPolicy           policy         = new RangerPolicy();
        List<RangerPolicyItem> policyItemList = getList(new RangerPolicyItem());

        Assertions.assertEquals(0, policy.getPolicyItems().size(), "RangerPolicy.getPolicyItems()");
        policy.addPolicyItem(new RangerPolicyItem());
        Assertions.assertEquals(1, policy.getPolicyItems().size(), "RangerPolicy.addPolicyItem()");
        policy.setPolicyItems(policyItemList);
        Assertions.assertEquals(policyItemList.size(), policy.getPolicyItems().size(), "RangerPolicy.setPolicyItems()");

        Assertions.assertEquals(0, policy.getDenyPolicyItems().size(), "RangerPolicy.getDenyPolicyItems()");
        policy.addDenyPolicyItem(new RangerPolicyItem());
        Assertions.assertEquals(1, policy.getDenyPolicyItems().size(), "RangerPolicy.addDenyPolicyItem()");
        policy.setDenyPolicyItems(policyItemList);
        Assertions.assertEquals(policyItemList.size(), policy.getDenyPolicyItems().size(), "RangerPolicy.setDenyPolicyItems()");

        Assertions.assertEquals(0, policy.getAllowExceptions().size(), "RangerPolicy.getAllowExceptions()");
        policy.addAllowException(new RangerPolicyItem());
        Assertions.assertEquals(1, policy.getAllowExceptions().size(), "RangerPolicy.addAllowException()");
        policy.setAllowExceptions(policyItemList);
        Assertions.assertEquals(policyItemList.size(), policy.getAllowExceptions().size(), "RangerPolicy.setAllowExceptions()");

        Assertions.assertEquals(0, policy.getDenyExceptions().size(), "RangerPolicy.getDenyExceptions()");
        policy.addDenyException(new RangerPolicyItem());
        Assertions.assertEquals(1, policy.getDenyExceptions().size(), "RangerPolicy.addDenyException()");
        policy.setDenyExceptions(policyItemList);
        Assertions.assertEquals(policyItemList.size(), policy.getDenyExceptions().size(), "RangerPolicy.setDenyExceptions()");
    }

    @Test
    public void test_02_PolicyItem_SetListMethods() {
        RangerPolicyItem                policyItem = new RangerPolicyItem();
        List<RangerPolicyItemAccess>    accesses   = getList(new RangerPolicyItemAccess());
        List<String>                    users      = getList("user");
        List<String>                    groups     = getList("group");
        List<RangerPolicyItemCondition> conditions = getList(new RangerPolicyItemCondition());

        Assertions.assertEquals(0, policyItem.getAccesses().size(), "RangerPolicyItem.getAccesses()");
        policyItem.addAccess(new RangerPolicyItemAccess());
        Assertions.assertEquals(1, policyItem.getAccesses().size(), "RangerPolicyItem.addAccess()");
        policyItem.setAccesses(accesses);
        Assertions.assertEquals(accesses.size(), policyItem.getAccesses().size(), "RangerPolicyItem.setAccesses()");

        Assertions.assertEquals(0, policyItem.getUsers().size(), "RangerPolicyItem.getUsers()");
        policyItem.addUser(new String());
        Assertions.assertEquals(1, policyItem.getUsers().size(), "RangerPolicyItem.addUser()");
        policyItem.setUsers(users);
        Assertions.assertEquals(users.size(), policyItem.getUsers().size(), "RangerPolicyItem.setUsers()");

        Assertions.assertEquals(0, policyItem.getGroups().size(), "RangerPolicyItem.getGroups()");
        policyItem.addGroup(new String());
        Assertions.assertEquals(1, policyItem.getGroups().size(), "RangerPolicyItem.addGroup()");
        policyItem.setGroups(groups);
        Assertions.assertEquals(groups.size(), policyItem.getGroups().size(), "RangerPolicyItem.setGroups()");

        Assertions.assertEquals(0, policyItem.getConditions().size(), "RangerPolicyItem.getConditions()");
        policyItem.addCondition(new RangerPolicyItemCondition());
        Assertions.assertEquals(1, policyItem.getConditions().size(), "RangerPolicyItem.addCondition()");
        policyItem.setConditions(conditions);
        Assertions.assertEquals(conditions.size(), policyItem.getConditions().size(), "RangerPolicyItem.setConditions()");
    }

    @Test
    public void test_03_PolicyResource_SetListMethods() {
        RangerPolicyResource policyResource = new RangerPolicyResource();
        List<String>         values         = getList("value");

        Assertions.assertEquals(0, policyResource.getValues().size(), "RangerPolicyResource.getValues()");
        policyResource.addValue(new String());
        Assertions.assertEquals(1, policyResource.getValues().size(), "RangerPolicyResource.addValue()");
        policyResource.setValues(values);
        Assertions.assertEquals(values.size(), policyResource.getValues().size(), "RangerPolicyResource.setValues()");
    }

    @Test
    public void test_04_PolicyItemCondition_SetListMethods() {
        RangerPolicyItemCondition policyItemCondition = new RangerPolicyItemCondition();
        List<String>              values              = getList("value");

        Assertions.assertEquals(0, policyItemCondition.getValues().size(), "RangerPolicyItemCondition.getValues()");
        policyItemCondition.addValue(new String());
        Assertions.assertEquals(1, policyItemCondition.getValues().size(), "RangerPolicyItemCondition.addValue()");
        policyItemCondition.setValues(values);
        Assertions.assertEquals(values.size(), policyItemCondition.getValues().size(), "RangerPolicyItemCondition.setValues()");
    }

    private <T> List<T> getList(T value) {
        List<T> ret = new ArrayList<>();

        int count = getRandomNumber(10);
        for (int i = 0; i < count; i++) {
            ret.add(value);
        }

        return ret;
    }

    private int getRandomNumber(int maxValue) {
        return (int) (Math.random() * maxValue);
    }
}
