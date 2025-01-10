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

package org.apache.ranger.services.kafka.client;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ServiceKafkaClient {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceKafkaClient.class);

    private static final String errMessage             = " You can still save the repository and start creating policies, but you would not be able to use autocomplete for resource names. Check server logs for more info.";
    private static final String TOPIC_KEY              = "topic";
    private static final String KEY_SASL_MECHANISM     = "sasl.mechanism";
    private static final String KEY_SASL_JAAS_CONFIG   = "sasl.jaas.config";
    private static final String KEY_KAFKA_KEYTAB       = "kafka.keytab";
    private static final String KEY_KAFKA_PRINCIPAL    = "kafka.principal";
    private static final String JAAS_KRB5_MODULE       = "com.sun.security.auth.module.Krb5LoginModule required";
    private static final String JAAS_USE_KEYTAB        = "useKeyTab=true";
    private static final String JAAS_KEYTAB            = "keyTab=\"";
    private static final String JAAS_STOKE_KEY         = "storeKey=true";
    private static final String JAAS_SERVICE_NAME      = "serviceName=kafka";
    private static final String JAAS_USER_TICKET_CACHE = "useTicketCache=false";
    private static final String JAAS_PRINCIPAL         = "principal=\"";
    private static final long   LOOKUP_TIMEOUT_SEC     = 5;

    String              serviceName;
    Map<String, String> configs;

    public ServiceKafkaClient(String serviceName, Map<String, String> configs) {
        this.serviceName = serviceName;
        this.configs     = configs;
    }

    public Map<String, Object> connectionTest() {
        String              errMsg       = errMessage;
        Map<String, Object> responseData = new HashMap<>();

        try {
            getTopicList(null);

            // If it doesn't throw exception, then assume the instance is reachable
            String successMsg = "ConnectionTest Successful";

            BaseClient.generateResponseDataMap(true, successMsg, successMsg, null, null, responseData);
        } catch (Exception e) {
            LOG.error("Error connecting to Kafka. kafkaClient = {}", this, e);

            String failureMsg = "Unable to connect to Kafka instance." + e.getMessage();

            BaseClient.generateResponseDataMap(false, failureMsg, failureMsg + errMsg, null, null, responseData);
        }

        return responseData;
    }

    public List<String> getResources(ResourceLookupContext context) {
        String                    userInput      = context.getUserInput();
        String                    resource       = context.getResourceName();
        Map<String, List<String>> resourceMap    = context.getResources();
        List<String>              resultList     = null;
        List<String>              topicList      = null;
        ResourceType              lookupResource = ResourceType.TOPIC;

        LOG.debug("<== getResources()  UserInput: \"{}\" resource : {} resourceMap: {}", userInput, resource, resourceMap);

        if (userInput != null && resource != null) {
            if (resourceMap != null && !resourceMap.isEmpty()) {
                topicList = resourceMap.get(TOPIC_KEY);
            }

            if (resource.trim().equalsIgnoreCase(TOPIC_KEY)) {
                lookupResource = ResourceType.TOPIC;
            }
        }

        if (userInput != null) {
            try {
                Callable<List<String>> callableObj    = null;
                final String           userInputFinal = userInput;
                final List<String>     finalTopicList = topicList;

                if (lookupResource == ResourceType.TOPIC) {
                    // get the topic list for given Input
                    callableObj = () -> {
                        List<String> retList = new ArrayList<>();

                        try {
                            List<String> list = getTopicList(finalTopicList);

                            if (userInputFinal != null && !userInputFinal.isEmpty()) {
                                for (String value : list) {
                                    if (value.startsWith(userInputFinal)) {
                                        retList.add(value);
                                    }
                                }
                            } else {
                                retList.addAll(list);
                            }
                        } catch (Exception ex) {
                            LOG.error("Error getting topic.", ex);
                        }

                        return retList;
                    };
                }

                // If we need to do lookup
                if (callableObj != null) {
                    synchronized (this) {
                        resultList = TimedEventUtil.timedTask(callableObj, LOOKUP_TIMEOUT_SEC, TimeUnit.SECONDS);
                    }
                }
            } catch (Exception e) {
                LOG.error("Unable to get hive resources.", e);
            }
        }

        return resultList;
    }

    @Override
    public String toString() {
        return "ServiceKafkaClient [serviceName = " + serviceName + ", configs = " + configs + "]";
    }

    private List<String> getTopicList(List<String> ignoreTopicList) throws Exception {
        List<String> ret               = new ArrayList<>();
        int          sessionTimeout    = 5000;
        int          connectionTimeout = 10000;
        AdminClient  adminClient       = null;

        try {
            Properties props = new Properties();

            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, configs.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
            props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, configs.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG));
            props.put(KEY_SASL_MECHANISM, configs.get(KEY_SASL_MECHANISM));
            props.put(KEY_SASL_JAAS_CONFIG, getJAASConfig(configs));
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, getIntProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, sessionTimeout));
            props.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, getIntProperty(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionTimeout));

            adminClient = KafkaAdminClient.create(props);

            ListTopicsResult listTopicsResult = adminClient.listTopics();

            if (listTopicsResult != null) {
                Collection<TopicListing> topicListings = listTopicsResult.listings().get();

                for (TopicListing topicListing : topicListings) {
                    String topicName = topicListing.name();

                    if (ignoreTopicList == null || !ignoreTopicList.contains(topicName)) {
                        ret.add(topicName);
                    }
                }
            }
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
        }

        return ret;
    }

    private Integer getIntProperty(String key, int defaultValue) {
        if (key == null) {
            return defaultValue;
        }

        String returnVal = configs.get(key);

        if (returnVal == null) {
            return defaultValue;
        }

        return Integer.valueOf(returnVal);
    }

    private String getJAASConfig(Map<String, String> configs) {
        String jaasConfig = new StringBuilder()
                .append(JAAS_KRB5_MODULE).append(" ")
                .append(JAAS_USE_KEYTAB).append(" ")
                .append(JAAS_KEYTAB).append(configs.get(KEY_KAFKA_KEYTAB)).append("\"").append(" ")
                .append(JAAS_STOKE_KEY).append(" ")
                .append(JAAS_USER_TICKET_CACHE).append(" ")
                .append(JAAS_SERVICE_NAME).append(" ")
                .append(JAAS_PRINCIPAL).append(configs.get(KEY_KAFKA_PRINCIPAL)).append("\";")
                .toString();

        LOG.debug("KafkaClient JAAS: {}", jaasConfig);

        return jaasConfig;
    }

    enum ResourceType {
        TOPIC
    }
}
