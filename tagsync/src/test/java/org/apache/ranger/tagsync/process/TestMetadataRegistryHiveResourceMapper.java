package org.apache.ranger.tagsync.process;

import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.tagsync.source.metadataregistry.MetadataRegistryEntity;
import org.apache.ranger.tagsync.source.metadataregistry.MetadataRegistryHiveResourceMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMetadataRegistryHiveResourceMapper {
    private final MetadataRegistryHiveResourceMapper mapper = new MetadataRegistryHiveResourceMapper();

    @Test
    public void testHiveTableUrn() throws Exception {
        MetadataRegistryEntity entity = new MetadataRegistryEntity(
                "urn:hive:table::hms:prod:sales:orders",
                42L,
                MetadataRegistryHiveResourceMapper.ENTITY_TYPE_HIVE_TABLE,
                "",
                "orders",
                "Orders",
                "prod",
                "hms");

        RangerServiceResource resource = mapper.buildResource(entity);

        Assertions.assertEquals("prod_hive", resource.getServiceName());
        Assertions.assertEquals(
                "sales",
                resource.getResourceElements().get("database").getValues().get(0));
        Assertions.assertEquals(
                "orders",
                resource.getResourceElements().get("table").getValues().get(0));
    }

    @Test
    public void testHiveDatabaseUrn() throws Exception {
        MetadataRegistryEntity entity = new MetadataRegistryEntity(
                "urn:hive:database::hms:prod:sales",
                7L,
                MetadataRegistryHiveResourceMapper.ENTITY_TYPE_HIVE_DATABASE,
                "",
                "sales",
                "Sales",
                "prod",
                "hms");

        RangerServiceResource resource = mapper.buildResource(entity);

        Assertions.assertEquals("prod_hive", resource.getServiceName());
        Assertions.assertEquals(
                "sales",
                resource.getResourceElements().get("database").getValues().get(0));
    }

    @Test
    public void testHiveColumnFieldPath() throws Exception {
        MetadataRegistryEntity entity = new MetadataRegistryEntity(
                "urn:hive:table::hms:prod:sales:orders",
                42L,
                MetadataRegistryHiveResourceMapper.ENTITY_TYPE_HIVE_TABLE,
                "columns.email",
                "orders",
                "Orders",
                "prod",
                "hms");

        RangerServiceResource resource = mapper.buildResource(entity);

        Assertions.assertEquals(
                "email",
                resource.getResourceElements().get("column").getValues().get(0));
    }
}
