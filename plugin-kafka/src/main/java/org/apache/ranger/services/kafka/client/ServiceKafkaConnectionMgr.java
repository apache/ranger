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

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Map;

public class ServiceKafkaConnectionMgr {
    private static final String SEPARATOR           = ",";
    private static final String KEY_SASL_MECHANISM  = "sasl.mechanism";
    private static final String KEY_KAFKA_KEYTAB    = "kafka.keytab";
    private static final String KEY_KAFKA_PRINCIPAL = "kafka.principal";

    private ServiceKafkaConnectionMgr() {
        // to block instantiation
    }

    public static ServiceKafkaClient getKafkaClient(String serviceName, Map<String, String> configs) throws Exception {
        String error = getServiceConfigValidationErrors(configs);

        if (StringUtils.isNotBlank(error)) {
            error = "JAAS configuration missing or not correct in Ranger Kafka Service. " + error;
            throw new Exception(error);
        }
        return new ServiceKafkaClient(serviceName, configs);
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) throws Exception {
        ServiceKafkaClient serviceKafkaClient = getKafkaClient(serviceName, configs);
        return serviceKafkaClient.connectionTest();
    }

    private static String getServiceConfigValidationErrors(Map<String, String> configs) {
        StringBuilder ret = new StringBuilder();

        String bootstrapServers = configs.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
        String securityProtocol = configs.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG);
        String saslMechanism    = configs.get(KEY_SASL_MECHANISM);
        String kafkaKeytab      = configs.get(KEY_KAFKA_KEYTAB);
        String kafkaPrincipal   = configs.get(KEY_KAFKA_PRINCIPAL);

        if (StringUtils.isEmpty(bootstrapServers)) {
            ret.append(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
        }

        if (StringUtils.isEmpty(securityProtocol)) {
            if (StringUtils.isNotBlank(ret.toString())) {
                ret.append(SEPARATOR).append(AdminClientConfig.SECURITY_PROTOCOL_CONFIG);
            } else {
                ret.append(AdminClientConfig.SECURITY_PROTOCOL_CONFIG);
            }
        }

        if (StringUtils.isEmpty(saslMechanism)) {
            if (StringUtils.isNotBlank(ret.toString())) {
                ret.append(SEPARATOR).append(KEY_SASL_MECHANISM);
            } else {
                ret.append(KEY_SASL_MECHANISM);
            }
        }

        if (StringUtils.isEmpty(kafkaKeytab)) {
            if (StringUtils.isNotBlank(ret.toString())) {
                ret.append(SEPARATOR).append(KEY_KAFKA_KEYTAB);
            } else {
                ret.append(KEY_KAFKA_KEYTAB);
            }
        }

        if (StringUtils.isEmpty(kafkaPrincipal)) {
            if (StringUtils.isNotBlank(ret.toString())) {
                ret.append(SEPARATOR).append(KEY_KAFKA_PRINCIPAL);
            } else {
                ret.append(KEY_KAFKA_PRINCIPAL);
            }
        }
        return ret.toString();
    }
}
