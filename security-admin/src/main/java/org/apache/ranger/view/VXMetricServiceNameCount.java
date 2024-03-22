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
package org.apache.ranger.view;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
public class VXMetricServiceNameCount implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        protected Map<String, Map<String, Long>> serviceBasedCountList = new HashMap<String,Map<String, Long>>();
        protected Long totalCount;

        /**
         * Default constructor. This will set all the attributes to default value.
         */
        public VXMetricServiceNameCount() {
        }

        /**
         * @return the serviceBasedCountList
         */
        public Map<String, Map<String, Long>> getServiceBasedCountList() {
                return serviceBasedCountList;
        }

        /**
         * @param servicesforPolicyType the serviceBasedCountList to set
         */
        public void setServiceBasedCountList(Map<String, Map<String, Long>> servicesforPolicyType) {
                this.serviceBasedCountList = servicesforPolicyType;
        }

        /**
         * @return the totalCount
         */
        public Long getTotalCount() {
                return totalCount;
        }

        /**
         * @param totalCount the totalCount to set
         */
        public void setTotalCount(Long totalCount) {
                this.totalCount = totalCount;
        }

        @Override
        public String toString() {
                return "VXMetricServiceNameCount={total_count=" + totalCount +", services="
                        + serviceBasedCountList +"}";
        }
}
