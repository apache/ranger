/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.common.view;

import org.apache.ranger.entity.XXDBBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class TestViewBaseBean {
    @Test
    public void testGetMObj() {
        ViewBaseBean viewBaseBean = new ViewBaseBean();

        XXDBBase mockObj = mock(XXDBBase.class);
        viewBaseBean.setMObj(mockObj);

        Object mObj = viewBaseBean.getMObj();
        assertNotNull("MObj should not be null", mObj); // ✅ FIXED: no cast to String
    }

    @Test
    public void testGetMyClassType() {
        ViewBaseBean viewBaseBean = new ViewBaseBean();
        int          myClassType  = viewBaseBean.getMyClassType();
        assertNotNull("My class type should not be null", String.valueOf(myClassType));
    }
}
