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

package org.apache.ranger.services.kms.client.json.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class KMSSchedulerResponse implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private KMSScheduler scheduler = null;

    public KMSScheduler getScheduler() { return scheduler; }

    public List<String> getQueueNames() {
    	List<String> ret = new ArrayList<String>();

    	if(scheduler != null) {
    		scheduler.collectQueueNames(ret);
    	}

    	return ret;
    }


    @JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class KMSScheduler implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private KMSSchedulerInfo schedulerInfo = null;

        public KMSSchedulerInfo getSchedulerInfo() { return schedulerInfo; }

        public void collectQueueNames(List<String> queueNames) {
        	if(schedulerInfo != null) {
        		schedulerInfo.collectQueueNames(queueNames, null);
        	}
        }
    }

    @JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class KMSSchedulerInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String     queueName = null;
        private KMSQueues queues    = null;

        public String getQueueName() { return queueName; }

        public KMSQueues getQueues() { return queues; }

        public void collectQueueNames(List<String> queueNames, String parentQueueName) {
        	if(queueName != null) {
        		String queueFqdn = parentQueueName == null ? queueName : parentQueueName + "." + queueName;

        		queueNames.add(queueFqdn);

            	if(queues != null) {
            		queues.collectQueueNames(queueNames, queueFqdn);
            	}
        	}
        }
    }

    @JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class KMSQueues implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private List<KMSSchedulerInfo> queue = null;

        public List<KMSSchedulerInfo> getQueue() { return queue; }

        public void collectQueueNames(List<String> queueNames, String parentQueueName) {
        	if(queue != null) {
        		for(KMSSchedulerInfo schedulerInfo : queue) {
        			schedulerInfo.collectQueueNames(queueNames, parentQueueName);
        		}
        	}
        }
    }
}
