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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestVList {
    @Test
    public void testToString() {
        TestVListImpl vList = new TestVListImpl(Collections.singletonList("test"));
        vList.setStartIndex(1);
        vList.setPageSize(10);
        vList.setTotalCount(100);
        vList.setResultSize(1);
        vList.setSortType("asc");
        vList.setSortBy("name");

        String str = vList.toString();
        assertNotNull(str, "String representation should not be null");
        System.out.println("VList toString(): " + str);
    }

    static class TestVListImpl extends VList {
        private final List<String> data;

        public TestVListImpl(List<String> data) {
            super(data);
            this.data = data;
        }

        @Override
        public int getListSize() {
            return data != null ? data.size() : 0;
        }

        @Override
        public List<?> getList() {
            return data;
        }
    }
}
