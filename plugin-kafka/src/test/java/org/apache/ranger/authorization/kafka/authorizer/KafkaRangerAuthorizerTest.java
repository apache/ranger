/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.authorization.kafka.authorizer;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Some;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

/**
 * A simple test that starts a Kafka broker, creates "test" and "dev" topics, sends a message to them and consumes it. We also plug in a
 * CustomAuthorizer that enforces some authorization rules:
 * <p>
 * - The "IT" group can do anything
 * - The "public" group can "read/describe/write" on the "test" topic.
 * - The "public" group can only "read/describe" on the "dev" topic, but not write.
 * <p>
 * In addition we have a TAG based policy, which grants "read/describe" access to the "public" group to the "messages" topic (which is associated
 * with the tag called "MessagesTag". A "kafka_topic" entity was created in Apache Atlas + then associated with the "MessagesTag". This was
 * then imported into Ranger using the TagSyncService. The policies were then downloaded locally and saved for testing off-line.
 * <p>
 * Policies available from admin via:
 * <p>
 * http://localhost:6080/service/plugins/policies/download/cl1_kafka
 */
public class KafkaRangerAuthorizerTest {
    private static final String userWithComma      = "CN=superUser,O=Apache";
    private static final String userWithSpace      = "CN=super user";
    private static final String nonSuperUser       = "CN=nonSuperUser";
    private static final String invalidPrincipal   = "CN=invalidPrincipal";
    private static final String CLIENT_KEYSTORE_PW = "keystorepass";
    private static final String CLIENT_KEY_PW      = "keypass";

    private static final Map<String, String> keyStorePaths = new HashMap<>();
    private static final Properties          adminProps    = new Properties();

    private static KafkaServer   kafkaServer;
    private static TestingServer zkServer;
    private static int           port;
    private static String        serviceKeystorePath;
    private static String        clientKeystorePath;
    private static String        truststorePath;
    private static Path          tempDir;

    @BeforeAll
    public static void setup() throws Exception {
        // Create keys
        String   serviceDN = "CN=localhost,O=Apache,L=Dublin,ST=Leinster,C=IE";
        String   clientDN  = "CN=localhost,O=Apache,L=Dublin,ST=Leinster,C=IE";
        KeyStore keystore  = KeyStore.getInstance(KeyStore.getDefaultType()); // Create a truststore

        keystore.load(null, "security".toCharArray());

        serviceKeystorePath = KafkaTestUtils.createAndStoreKey(serviceDN, serviceDN, BigInteger.valueOf(30), "sspass", "myservicekey", "skpass", keystore);
        clientKeystorePath  = KafkaTestUtils.createAndStoreKey(clientDN, clientDN, BigInteger.valueOf(31), "cspass", "myclientkey", "ckpass", keystore);

        List<String> usersForSuperUserTest = Arrays.asList(userWithComma, userWithSpace, nonSuperUser, invalidPrincipal);

        for (String user : usersForSuperUserTest) {
            keyStorePaths.put(user, KafkaTestUtils.createAndStoreKey(user, user, BigInteger.valueOf(new Random().nextLong()), CLIENT_KEYSTORE_PW, "key_" + user, CLIENT_KEY_PW, keystore));
        }

        File truststoreFile = File.createTempFile("kafkatruststore", ".jks");

        try (OutputStream output = new FileOutputStream(truststoreFile)) {
            keystore.store(output, "security".toCharArray());
        }

        truststorePath = truststoreFile.getPath();
        zkServer       = new TestingServer();

        zkServer.start();

        // Get a random port
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            Assertions.assertNotNull(serverSocket);

            port = serverSocket.getLocalPort();

            Assertions.assertTrue(port > 0);
        } catch (IOException e) {
            throw new RuntimeException("Local socket port not available", e);
        }

        tempDir = Files.createTempDirectory("kafka");

        final Properties props = new Properties();

        props.put("broker.id", String.valueOf(1));
        props.put("host.name", "localhost");
        props.put("port", String.valueOf(port));
        props.put("log.dir", tempDir.toString());
        props.put("zookeeper.connect", zkServer.getConnectString());
        props.put("replica.socket.timeout.ms", "1500");
        props.put("controlled.shutdown.enable", Boolean.TRUE.toString());
        // Enable SSL
        props.put("listeners", "SSL://localhost:" + port);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, serviceKeystorePath);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "sspass");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "skpass");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "security");
        props.put("security.inter.broker.protocol", "SSL");
        props.put("ssl.client.auth", "required");
        props.put("offsets.topic.replication.factor", (short) 1);
        props.put("offsets.topic.num.partitions", 1);

        // Plug in Apache Ranger authorizer
        props.put("authorizer.class.name", "org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer");

        // User with space should be valid super user
        // Space between username and separator should be trimmed, also comma should be valid character
        // User key is case sensitive, this user should not be super user
        // Invalid principal string (not in the User:name format) should not be super user
        String superUsersConfig = String.format("User:%s;User:%s  ;user:%s;%s", userWithSpace, userWithComma, nonSuperUser, invalidPrincipal);

        props.put("super.users", superUsersConfig);

        // Create users for testing
        UserGroupInformation.createUserForTesting(clientDN, new String[] {"public"});
        UserGroupInformation.createUserForTesting(serviceDN, new String[] {"IT"});

        KafkaConfig config = new KafkaConfig(props);

        kafkaServer = new KafkaServer(config, Time.SYSTEM, new Some<>("KafkaRangerAuthorizerTest"), false);

        kafkaServer.startup();

        adminProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + port);
        adminProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        // ssl
        adminProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, serviceKeystorePath);
        adminProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "sspass");
        adminProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "skpass");
        adminProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
        adminProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "security");

        KafkaTestUtils.createSomeTopics(adminProps);
    }

    @AfterAll
    public static void cleanup() throws Exception {
        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }

        if (zkServer != null) {
            zkServer.stop();
        }

        for (String keyStorePath : keyStorePaths.values()) {
            File keystoreFile = new File(keyStorePath);

            if (keystoreFile.exists()) {
                FileUtils.forceDelete(keystoreFile);
            }
        }

        File clientKeystoreFile = new File(clientKeystorePath);
        if (clientKeystoreFile.exists()) {
            FileUtils.forceDelete(clientKeystoreFile);
        }

        File serviceKeystoreFile = new File(serviceKeystorePath);
        if (serviceKeystoreFile.exists()) {
            FileUtils.forceDelete(serviceKeystoreFile);
        }

        File truststoreFile = new File(truststorePath);
        if (truststoreFile.exists()) {
            FileUtils.forceDelete(truststoreFile);
        }

        if (tempDir != null) {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    // The "public" group can read from "test"
    @Test
    public void testAuthorizedRead() throws Exception {
        Properties producerProps = getProducerProps(serviceKeystorePath, "sspass", "skpass");
        Properties consumerProps = getConsumerProps(clientKeystorePath, "cspass", "ckpass");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Arrays.asList("test"));

            try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
                // Send a message
                producer.send(new ProducerRecord<>("test", "somekey", "somevalue"));
                producer.flush();
            }

            // Poll until we consume it
            ConsumerRecord<String, String> record = null;

            for (int i = 0; i < 1000; i++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (records.count() > 0) {
                    record = records.iterator().next();
                    break;
                }

                Thread.sleep(1000);
            }

            Assertions.assertNotNull(record);
            Assertions.assertEquals("somevalue", record.value());
        }
    }

    // The "IT" group can write to any topic
    @Test
    public void testAuthorizedWrite() throws Exception {
        Properties producerProps = getProducerProps(clientKeystorePath, "cspass", "ckpass");

        try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // Send a message
            Future<RecordMetadata> record = producer.send(new ProducerRecord<>("dev", "somekey", "somevalue"));

            producer.flush();
            record.get();
        }
    }

    // The "public" group can write to "test" but not "dev"
    @Test
    public void testUnauthorizedWrite() throws Exception {
        // Create the Producer
        Properties producerProps = new Properties();

        producerProps.put("bootstrap.servers", "localhost:" + port);
        producerProps.put("acks", "all");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        producerProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        producerProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientKeystorePath);
        producerProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "cspass");
        producerProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "ckpass");
        producerProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
        producerProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "security");

        try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // Send a message
            Future<RecordMetadata> record = producer.send(new ProducerRecord<>("test", "somekey", "somevalue"));

            producer.flush();
            record.get();

            try {
                record = producer.send(new ProducerRecord<>("dev", "somekey", "somevalue"));

                producer.flush();
                record.get();
            } catch (Exception ex) {
                Assertions.assertTrue(ex.getMessage().contains("Not authorized to access topics"));
            }
        }
    }

    // The "public" group can read from "messages"
    @Test
    public void testAuthorizedReadUsingTagPolicy() throws Exception {
        Properties producerProps = getProducerProps(serviceKeystorePath, "sspass", "skpass");
        Properties consumerProps = getConsumerProps(clientKeystorePath, "cspass", "ckpass");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Arrays.asList("messages"));

            try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
                // Send a message
                producer.send(new ProducerRecord<>("messages", "somekey", "somevalue"));
                producer.flush();
            }

            // Poll until we consume it
            ConsumerRecord<String, String> record = null;

            for (int i = 0; i < 1000; i++) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                if (records.count() > 0) {
                    record = records.iterator().next();
                    break;
                }

                Thread.sleep(1000);
            }

            Assertions.assertNotNull(record);
            Assertions.assertEquals("somevalue", record.value());
        }
    }

    // User with space should be valid super user
    @Test
    public void testSuperUserWithInternalSpace() throws Exception {
        String topic = "topic-user-space";

        testUserCanReadWrite(userWithSpace, topic);
    }

    // Space between username and separator should be trimmed, also comma should be valid character in user name
    @Test
    public void testSuperUserWithCommaAndTrailingSpaceTrimmed() throws Exception {
        String topic = "topic-user-comma";

        testUserCanReadWrite(userWithComma, topic);
    }

    // User key is case sensitive in the config, this user should not be super user as it was added with the wrong key
    @Test
    public void testUserWithWrongKeyShouldNotBeSuperUser() {
        String topic = "topic-non-super-user";

        testUserCanNotWrite(nonSuperUser, topic);
    }

    // Invalid principal string (not User:string) should not be super user
    @Test
    public void testInvalidPrincipalStringShouldNotBeSuperUser() {
        String topic = "topic-invalid-principal";

        testUserCanNotRead(invalidPrincipal, topic);
    }

    private void testUserCanReadWrite(String user, String topic) throws Exception {
        KafkaTestUtils.createTopic(adminProps, topic);

        Properties producerProps = getProducerProps(keyStorePaths.get(user), CLIENT_KEYSTORE_PW, CLIENT_KEY_PW);

        Properties consumerProps = getConsumerProps(keyStorePaths.get(user), CLIENT_KEYSTORE_PW, CLIENT_KEY_PW);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));

            try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
                // Send a message
                Future<RecordMetadata> record = producer.send(new ProducerRecord<>(topic, "somekey", "somevalue"));

                producer.flush();
                record.get();
            }

            // Poll until we consume it
            ConsumerRecord<String, String> record = null;
            for (int i = 0; i < 1000; i++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (records.count() > 0) {
                    record = records.iterator().next();
                    break;
                }

                Thread.sleep(1000);
            }

            Assertions.assertNotNull(record);
            Assertions.assertEquals("somevalue", record.value());
        }
    }

    private void testUserCanNotRead(String user, String topic) {
        KafkaTestUtils.createTopic(adminProps, topic);

        Properties producerProps = getProducerProps(serviceKeystorePath, "sspass", "skpass");
        Properties consumerProps = getConsumerProps(keyStorePaths.get(user), CLIENT_KEYSTORE_PW, CLIENT_KEY_PW);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
                // Send a message
                producer.send(new ProducerRecord<>(topic, "somekey", "somevalue"));
                producer.flush();
            }

            try {
                consumer.subscribe(Collections.singletonList(topic));
                for (int i = 0; i < 1000; i++) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    if (records.count() > 0) {
                        records.iterator().next();
                        break;
                    }

                    Thread.sleep(1000);
                }

                Assertions.fail(String.format("User \"%s\" should have been unauthorized to consume from topic \"%s\"", user, topic));
            } catch (Exception ex) {
                Assertions.assertTrue(ex.getMessage().contains("Not authorized to access topics"));
            }
        }
    }

    private void testUserCanNotWrite(String user, String topic) {
        KafkaTestUtils.createTopic(adminProps, topic);

        // Create the Producer
        Properties producerProps = getProducerProps(keyStorePaths.get(user), CLIENT_KEYSTORE_PW, CLIENT_KEY_PW);

        try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
            try {
                Future<RecordMetadata> record = producer.send(new ProducerRecord<>(topic, "somekey", "somevalue"));

                producer.flush();
                record.get();
            } catch (Exception ex) {
                Assertions.assertTrue(ex.getMessage().contains("Not authorized to access topics"));
            }
        }
    }

    private static Properties getProducerProps(String keyStorePath, String keyStorePw, String keyPw) {
        Properties producerProps = new Properties();

        producerProps.put("bootstrap.servers", "localhost:" + port);
        producerProps.put("acks", "all");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        producerProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        producerProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStorePath);
        producerProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePw);
        producerProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPw);
        producerProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
        producerProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "security");

        return producerProps;
    }

    private static Properties getConsumerProps(String keyStorePath, String keyStorePw, String keyPw) {
        Properties consumerProps = new Properties();

        consumerProps.put("bootstrap.servers", "localhost:" + port);
        consumerProps.put("group.id", "test");
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        consumerProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        consumerProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStorePath);
        consumerProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePw);
        consumerProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPw);
        consumerProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
        consumerProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "security");

        return consumerProps;
    }
}
