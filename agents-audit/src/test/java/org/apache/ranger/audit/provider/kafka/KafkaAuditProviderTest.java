package org.apache.ranger.audit.provider.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaAuditProviderTest {

    @Mock
    private Producer<String, String> mockProducer;

    private KafkaAuditProvider kafkaAuditProvider;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
        kafkaAuditProvider = new KafkaAuditProvider();
        kafkaAuditProvider.producer = mockProducer;
    }

    @Test
    public void testInit() {
        Properties props = new Properties();
        props.setProperty(KafkaAuditProvider.AUDIT_KAFKA_TOPIC_NAME, "test_topic");

        kafkaAuditProvider.init(props, null);

        assertNotNull(KafkaAuditProvider.producer, "Producer should be initialized");
        assertEquals("test_topic", KafkaAuditProvider.topic, "Topic should be set correctly");
    }

    @Test
    public void testBuildKafkaProducerProperties() throws Exception {
        Properties props = new Properties();

        props.setProperty("xasecure.audit.kafka.bootstrap.servers", "localhost:9092");
        props.setProperty("xasecure.audit.kafka.acks", "all");
        props.setProperty("xasecure.audit.kafka.key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("xasecure.audit.kafka.value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("xasecure.audit.kafka.compression.type", "gzip");
        props.setProperty("xasecure.audit.kafka.invalid.config", "shouldBeIgnored");

        Method method = KafkaAuditProvider.class.getDeclaredMethod("buildKafkaProducerProperties", Properties.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Object> kafkaProps = (Map<String, Object>) method.invoke(kafkaAuditProvider, props);

        assertEquals("localhost:9092", kafkaProps.get("bootstrap.servers"), "Bootstrap servers should be set correctly");
        assertEquals("all", kafkaProps.get("acks"), "Acks should be set correctly");
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", kafkaProps.get("key.serializer"), "Key serializer should be set correctly");
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", kafkaProps.get("value.serializer"), "Value serializer should be set correctly");
        assertEquals("gzip", kafkaProps.get("compression.type"), "Compression type should be set correctly");
        assertFalse(kafkaProps.containsKey("invalid.config"), "Invalid config should not be set");
    }

}