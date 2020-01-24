package org.apache.ranger.services.schema.registry.client.srclient;

import com.google.common.io.Resources;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer;
import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class DefaultSRClientTest {

    private static final String V1_API_PATH = "api/v1";

    private static LocalSchemaRegistryServer localSchemaRegistryServer;

    private static SRClient srClient;

    @BeforeClass
    public static void init() throws Exception {
        localSchemaRegistryServer =
                new LocalSchemaRegistryServer(getFilePath("ssl-schema-registry.yaml"));

        try {
            localSchemaRegistryServer.start();
        } catch (Exception e){
            localSchemaRegistryServer.stop();
            throw e;
        }

        SchemaMetadata schemaMetadata1 = new SchemaMetadata
                .Builder("Schema1")
                .type("avro")
                .schemaGroup("Group1")
                .description("description")
                .build();

        SchemaMetadata schemaMetadata2 = new SchemaMetadata
                .Builder("Schema2")
                .type("avro")
                .schemaGroup("Group2")
                .description("description")
                .build();

        SchemaMetadata schemaMetadata3 = new SchemaMetadata
                .Builder("Schema3")
                .type("avro")
                .schemaGroup("Group3")
                .description("description")
                .build();

        SchemaRegistryClient client = getClient("ssl-schema-registry-client.yaml");

        client.registerSchemaMetadata(schemaMetadata1);
        client.registerSchemaMetadata(schemaMetadata2);
        client.registerSchemaMetadata(schemaMetadata3);

        SchemaVersion sv = new SchemaVersion(getSchema("schema-text3.avcs"),
                "Initial version of the schema");
        client.addSchemaVersion(schemaMetadata3, sv);

        ///////////////////////////////////////////////
        Map<String, Object> conf = new HashMap<>();
        conf.put(SCHEMA_REGISTRY_URL.name(), "https://localhost:" + localSchemaRegistryServer.getLocalPort());
        String keyStorePath = "./src/test/resources/keystore.jks";
        String keyStorePassword = "password";
        String keyStoreType = "jks";

        String trustStorePath = "./src/test/resources/truststore.jks";
        String trustStorePassword = "password";
        String trustStoreType = "jks";
        conf.put("keyStorePath", keyStorePath);
        conf.put("keyStorePassword", keyStorePassword);
        conf.put("keyStoreType", keyStoreType);

        conf.put("trustStorePath", trustStorePath);
        conf.put("trustStorePassword", trustStorePassword);
        conf.put("trustStoreType", trustStoreType);

        srClient = new DefaultSRClient(conf);

    }

    private static String getSchema(String schemaFileName) throws Exception {
        try (FileInputStream fis = new FileInputStream(getFilePath(schemaFileName))) {
            org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
            return parser.parse(fis).toString();
        } catch (Exception e) {
            throw new Exception("Failed to read schema text from : "
                    + getFilePath(schemaFileName), e);
        }

    }

    private static String getFilePath(String serverYAMLFileName) throws URISyntaxException {
        return new File(Resources.getResource(serverYAMLFileName)
                .toURI())
                .getAbsolutePath();
    }

    private static SchemaRegistryClient getClient(String clientYAMLFileName) throws Exception {
        String registryURL = localSchemaRegistryServer.getLocalURL() + V1_API_PATH;
        Map<String, Object> conf = new HashMap<>();
        try (FileInputStream fis = new FileInputStream(getFilePath(clientYAMLFileName))) {
            conf = (Map<String, Object>) new Yaml().load(IOUtils.toString(fis, "UTF-8"));
            conf.put("schema.registry.url", registryURL);
        } catch(Exception e) {
            throw new Exception("Failed to export schema client configuration for yaml : " + getFilePath(clientYAMLFileName), e);
        }
        conf.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), registryURL);

        return new SchemaRegistryClient(conf);
    }

    @Test
    public void getSchemaGroups() {
        List<String> groups = srClient.getSchemaGroups();
        assertThat(groups.size(), is(3));
        assertTrue(groups.contains("Group1"));
        assertTrue(groups.contains("Group2"));
        assertTrue(groups.contains("Group3"));
    }

    @Test
    public void getSchemaNames() {
        List<String> groups = new ArrayList<>();
        groups.add("Group1");
        groups.add("Group2");
        List<String> schemas = srClient.getSchemaNames(groups);
        assertThat(schemas.size(), is(2));
        assertTrue(schemas.contains("Schema1"));
        assertTrue(schemas.contains("Schema2"));
    }

    @Test
    public void getSchemaBranches() {
        List<String> branches = srClient.getSchemaBranches("Schema1");
        assertTrue(branches.isEmpty());
        branches = srClient.getSchemaBranches("Schema3");
        assertThat(branches.size(), is(1));
        assertThat(branches.get(0), is("MASTER"));
    }

    @Test
    public void checkConnection() {
        try {
            srClient.checkConnection();
        } catch (Exception e) {
            fail("No Exception should be thrown");
        }
    }

    @Test(expected = Exception.class)
    public void checkConnection2() throws Exception {
        new DefaultSRClient(new HashMap<>()).checkConnection();
    }
}