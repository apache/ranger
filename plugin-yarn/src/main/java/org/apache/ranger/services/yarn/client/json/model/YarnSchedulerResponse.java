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

package org.apache.ranger.services.yarn.client.json.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class YarnSchedulerResponse implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private static final String FAIR_SCHEDULER                = "fairScheduler";
    private static final String CAPACITY_SCHEDULER            = "capacityScheduler";
    private static final String CAPACITY_SCHEDULER_LEAF_QUEUE = "capacitySchedulerLeafQueueInfo";

    private YarnScheduler scheduler;

    public YarnScheduler getScheduler() {
        return scheduler;
    }

    public List<String> getQueueNames() {
        List<String> ret = new ArrayList<String>();

        if (scheduler != null) {
            scheduler.collectQueueNames(ret);
        }

        return ret;
    }

    @JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class YarnScheduler implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private YarnSchedulerInfo schedulerInfo;

        public YarnSchedulerInfo getSchedulerInfo() {
            return schedulerInfo;
        }

        public void collectQueueNames(List<String> queueNames) {
            if (schedulerInfo != null) {
                schedulerInfo.collectQueueNames(queueNames, null);
            }
        }
    }

    @JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class YarnSchedulerInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String     type; // support fair schedule
        private RootQueue  rootQueue;
        private String     queueName;
        private YarnQueues queues;

        public String getType() {
            return type;
        }

        public  RootQueue getRootQueue() {
            return rootQueue;
        }

        public String getQueueName() {
            return queueName;
        }

        public YarnQueues getQueues() {
            return queues;
        }

        public void collectQueueNames(List<String> queueNames, String parentQueueName) {
            if (type != null) {
                if (type.equals(FAIR_SCHEDULER)) {
                    if (rootQueue != null) {
                        rootQueue.collectQueueNames(queueNames);
                    }
                } else if (type.equals(CAPACITY_SCHEDULER) || type.equals(CAPACITY_SCHEDULER_LEAF_QUEUE)) {
                    if(queueName != null) {
                        String queueFqdn = parentQueueName == null ? queueName : parentQueueName + "." + queueName;

                        queueNames.add(queueFqdn);

                        if (queues != null) {
                            queues.collectQueueNames(queueNames, queueFqdn);
                        }
                    }
                }
            }
        }
    }

    @JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class YarnQueues implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private List<YarnSchedulerInfo> queue;

        public List<YarnSchedulerInfo> getQueue() {
            return queue;
        }

        public void collectQueueNames(List<String> queueNames, String parentQueueName) {
            if (queue != null) {
                for (YarnSchedulerInfo schedulerInfo : queue) {
                    schedulerInfo.collectQueueNames(queueNames, parentQueueName);
                }
            }
        }
    }

    @JsonAutoDetect(getterVisibility= JsonAutoDetect.Visibility.NONE, setterVisibility= JsonAutoDetect.Visibility.NONE, fieldVisibility= JsonAutoDetect.Visibility.ANY)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RootQueue implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String      queueName;
        private ChildQueues childQueues;

        public String getQueueName() {
            return queueName;
        }

        public ChildQueues getChildQueues() {
            return childQueues;
        }

        public void collectQueueNames(List<String> queueNames) {
            if (queueName != null){
                queueNames.add(queueName);

                if (childQueues != null) {
                    childQueues.collectQueueNames(queueNames);
                }
            }
        }
    }

    @JsonAutoDetect(getterVisibility= JsonAutoDetect.Visibility.NONE, setterVisibility= JsonAutoDetect.Visibility.NONE, fieldVisibility= JsonAutoDetect.Visibility.ANY)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class ChildQueues implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private List<RootQueue> queue;

        public List<RootQueue> getQueue() {
            return queue;
        }

        public void collectQueueNames(List<String> queueNames) {
            if( queue != null) {
                for (RootQueue queue : queue) {
                    queue.collectQueueNames(queueNames);
                }
            }
        }
    }
}
