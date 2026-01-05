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

package org.apache.ranger.authorization.atlas.authorizer;

import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRangerAtlasAuthorizer {
    private static final String      USER_USER1                   = "user1";
    private static final Set<String> USER_USER1_GROUPS            = Collections.singleton("users");
    private static final String      USER_ADMIN1                  = "admin1";
    private static final Set<String> USER_ADMIN1_GROUPS           = Collections.singleton("admins");
    private static final String      USER_STEWARD1                = "data-steward1";
    private static final Set<String> USER_STEWARD1_GROUPS         = Collections.singleton("stewards");
    private static final String      USER_FINANCE_STEWARD1        = "finance-data-steward1";
    private static final Set<String> USER_FINANCE_STEWARD1_GROUPS = new HashSet<>(Arrays.asList("finance-stewards", "stewards"));

    private static final AtlasEntityDef         ENTITY_DEF_HIVE_TABLE           = new AtlasEntityDef("hive_table");
    private static final AtlasClassificationDef CLASSIFICATION_DEF_FINANCE      = new AtlasClassificationDef("FINANCE");
    private static final AtlasClassificationDef CLASSIFICATION_DEF_PII          = new AtlasClassificationDef("PII");
    private static final AtlasClassificationDef CLASSIFICATION_DEF_DATA_QUALITY = new AtlasClassificationDef("DATA_QUALITY");
    private static final AtlasEntityHeader      ENTITY_HIVE_TABLE_DB1_TBL1      = new AtlasEntityHeader(ENTITY_DEF_HIVE_TABLE.getName(), Collections.singletonMap("qualifiedName", "db1.tbl1@dev"));
    private static final AtlasEntityHeader      ENTITY_HIVE_TABLE_DB1_TBL2      = new AtlasEntityHeader(ENTITY_DEF_HIVE_TABLE.getName(), Collections.singletonMap("qualifiedName", "db1.tbl2@dev"));
    private static final AtlasClassification    CLASSIFICATION_FINANCE          = new AtlasClassification(CLASSIFICATION_DEF_FINANCE.getName());
    private static final AtlasClassification    CLASSIFICATION_PII              = new AtlasClassification(CLASSIFICATION_DEF_PII.getName());
    private static final AtlasClassification    CLASSIFICATION_DATA_QUALITY     = new AtlasClassification(CLASSIFICATION_DEF_DATA_QUALITY.getName());
    private static final String                 LABEL_APPROVED                  = "approved";
    private static final String                 BUSINESS_METADATA_DATA_PROVIDER = "data-provider";
    private static final String                 RELATIONSHIP_TYPE_CLONE_OF      = "cloneOf";

    private final AtlasTypeRegistry     typeRegistry;
    private final RangerAtlasAuthorizer authorizer;

    public TestRangerAtlasAuthorizer() {
        typeRegistry = new AtlasTypeRegistry();
        authorizer   = new RangerAtlasAuthorizer();

        authorizer.init();
    }

    @Test
    public void testTypeDefRead() {
        AtlasTypeAccessRequest request = new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_READ, ENTITY_DEF_HIVE_TABLE);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to read typedef %s", request.getUser(), ENTITY_DEF_HIVE_TABLE.getName())
                .isTrue();
    }

    @Test
    public void testTypeDefCreate() {
        AtlasTypeAccessRequest request = new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_CREATE, ENTITY_DEF_HIVE_TABLE);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to create typedef %s", request.getUser(), ENTITY_DEF_HIVE_TABLE.getName())
                .isFalse();

        request.setUser(USER_ADMIN1, USER_ADMIN1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to create typedef %s", request.getUser(), ENTITY_DEF_HIVE_TABLE.getName())
                .isTrue();
    }

    @Test
    public void testTypeDefUpdate() {
        AtlasTypeAccessRequest request = new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_UPDATE, ENTITY_DEF_HIVE_TABLE);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to update typedef %s", request.getUser(), ENTITY_DEF_HIVE_TABLE.getName())
                .isFalse();

        request.setUser(USER_ADMIN1, USER_ADMIN1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to update typedef %s", request.getUser(), ENTITY_DEF_HIVE_TABLE.getName())
                .isTrue();
    }

    @Test
    public void testTypeDefDelete() {
        AtlasTypeAccessRequest request = new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_DELETE, ENTITY_DEF_HIVE_TABLE);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to delete typedef %s", request.getUser(), ENTITY_DEF_HIVE_TABLE.getName())
                .isFalse();

        request.setUser(USER_ADMIN1, USER_ADMIN1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to delete typedef %s", request.getUser(), ENTITY_DEF_HIVE_TABLE.getName())
                .isTrue();
    }

    @Test
    public void testEntityRead() {
        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, ENTITY_HIVE_TABLE_DB1_TBL1);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to read entity %s", request.getUser(), request.getEntityId())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to read entity %s", request.getUser(), request.getEntityId())
                .isTrue();

        request.setUser(USER_ADMIN1, USER_ADMIN1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to read entity %s", request.getUser(), request.getEntityId())
                .isTrue();
    }

    @Test
    public void testEntityCreate() {
        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, ENTITY_HIVE_TABLE_DB1_TBL1);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to create entity %s", request.getUser(), request.getEntityId())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to create entity %s", request.getUser(), request.getEntityId())
                .isFalse();

        request.setUser(USER_ADMIN1, USER_ADMIN1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to create entity %s", request.getUser(), request.getEntityId())
                .isTrue();
    }

    @Test
    public void testEntityUpdate() {
        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, ENTITY_HIVE_TABLE_DB1_TBL1);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to update entity %s", request.getUser(), request.getEntityId())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to update entity %s", request.getUser(), request.getEntityId())
                .isFalse();

        request.setUser(USER_ADMIN1, USER_ADMIN1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to update entity %s", request.getUser(), request.getEntityId())
                .isTrue();
    }

    @Test
    public void testEntityDelete() {
        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_DELETE, ENTITY_HIVE_TABLE_DB1_TBL1);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to delete entity %s", request.getUser(), request.getEntityId())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to delete entity %s", request.getUser(), request.getEntityId())
                .isFalse();

        request.setUser(USER_ADMIN1, USER_ADMIN1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to delete entity %s", request.getUser(), request.getEntityId())
                .isTrue();
    }

    @Test
    public void testAddClassification() {
        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_ADD_CLASSIFICATION, ENTITY_HIVE_TABLE_DB1_TBL1, CLASSIFICATION_PII);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to add classification %s", request.getUser(), request.getClassification().getTypeName())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to add classification %s", request.getUser(), request.getClassification().getTypeName())
                .isTrue();
    }

    @Test
    public void testAddClassificationOnEntityHavingFinanceTag() {
        AtlasEntityHeader entityHavingFinanceTag = new AtlasEntityHeader(ENTITY_HIVE_TABLE_DB1_TBL1);

        entityHavingFinanceTag.setClassifications(Collections.singletonList(CLASSIFICATION_FINANCE));

        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_ADD_CLASSIFICATION, entityHavingFinanceTag, CLASSIFICATION_DATA_QUALITY);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to add classification %s to entity having classifications %s", request.getUser(), request.getClassification().getTypeName(), entityHavingFinanceTag.getClassifications())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to add classification %s to entity having classifications %s", request.getUser(), request.getClassification().getTypeName(), entityHavingFinanceTag.getClassifications())
                .isFalse();

        request.setUser(USER_FINANCE_STEWARD1, USER_FINANCE_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to add classification %s to entity having classifications %s", request.getUser(), request.getClassification().getTypeName(), entityHavingFinanceTag.getClassifications())
                .isTrue();
    }

    @Test
    public void testAddClassificationOnEntityHavingFinanceDataQualityTags() {
        AtlasEntityHeader entityHavingFinanceTag = new AtlasEntityHeader(ENTITY_HIVE_TABLE_DB1_TBL1);

        entityHavingFinanceTag.setClassifications(Arrays.asList(CLASSIFICATION_FINANCE, CLASSIFICATION_DATA_QUALITY));

        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_ADD_CLASSIFICATION, entityHavingFinanceTag, CLASSIFICATION_PII);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to add classification %s to entity having classifications %s", request.getUser(), request.getClassification().getTypeName(), entityHavingFinanceTag.getClassifications())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to add classification %s to entity having classifications %s", request.getUser(), request.getClassification().getTypeName(), entityHavingFinanceTag.getClassifications())
                .isFalse();

        request.setUser(USER_FINANCE_STEWARD1, USER_FINANCE_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to add classification %s to entity having classifications %s", request.getUser(), request.getClassification().getTypeName(), entityHavingFinanceTag.getClassifications())
                .isTrue();
    }

    @Test
    public void testUpdateClassification() {
        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE_CLASSIFICATION, ENTITY_HIVE_TABLE_DB1_TBL1, CLASSIFICATION_PII);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to update classification %s", request.getUser(), request.getClassification().getTypeName())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to update classification %s", request.getUser(), request.getClassification().getTypeName())
                .isTrue();
    }

    @Test
    public void testRemoveClassification() {
        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_REMOVE_CLASSIFICATION, ENTITY_HIVE_TABLE_DB1_TBL1, CLASSIFICATION_PII);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to remove classification %s", request.getUser(), request.getClassification().getTypeName())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to remove classification %s", request.getUser(), request.getClassification().getTypeName())
                .isTrue();
    }

    @Test
    public void testAddLabel() {
        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_ADD_LABEL, ENTITY_HIVE_TABLE_DB1_TBL1, null, null, LABEL_APPROVED, null);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to add label ", request.getUser(), request.getLabel())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to add label %s", request.getUser(), request.getLabel())
                .isTrue();
    }

    @Test
    public void testRemoveLabel() {
        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_REMOVE_LABEL, ENTITY_HIVE_TABLE_DB1_TBL1, null, null, LABEL_APPROVED, null);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to remove label %s", request.getUser(), request.getLabel())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to remove label %s", request.getUser(), request.getLabel())
                .isTrue();
    }

    @Test
    public void testUpdateBusinessMetadata() {
        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE_BUSINESS_METADATA, ENTITY_HIVE_TABLE_DB1_TBL1, null, null, null, BUSINESS_METADATA_DATA_PROVIDER, USER_USER1, USER_USER1_GROUPS);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to update business metadata %s", request.getUser(), request.getBusinessMetadata())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to update business metadata %s", request.getUser(), request.getBusinessMetadata())
                .isTrue();
    }

    @Test
    public void testImport() {
        AtlasAdminAccessRequest request = new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_IMPORT);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to import", request.getUser())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to import", request.getUser())
                .isFalse();

        request.setUser(USER_ADMIN1, USER_ADMIN1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to import", request.getUser())
                .isTrue();
    }

    @Test
    public void testExport() {
        AtlasAdminAccessRequest request = new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_EXPORT);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to export", request.getUser())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to export", request.getUser())
                .isFalse();

        request.setUser(USER_ADMIN1, USER_ADMIN1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to export", request.getUser())
                .isTrue();
    }

    @Test
    public void testPurge() {
        AtlasAdminAccessRequest request = new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_PURGE);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to purge", request.getUser())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to purge", request.getUser())
                .isFalse();

        request.setUser(USER_ADMIN1, USER_ADMIN1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to purge", request.getUser())
                .isTrue();
    }

    @Test
    public void testAudits() {
        AtlasAdminAccessRequest request = new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_AUDITS);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to admin audits", request.getUser())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to admin audits", request.getUser())
                .isFalse();

        request.setUser(USER_ADMIN1, USER_ADMIN1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to admin audits", request.getUser())
                .isTrue();
    }

    @Test
    public void testAddRelationship() {
        AtlasRelationshipAccessRequest request = new AtlasRelationshipAccessRequest(typeRegistry, AtlasPrivilege.RELATIONSHIP_ADD, RELATIONSHIP_TYPE_CLONE_OF, ENTITY_HIVE_TABLE_DB1_TBL1, ENTITY_HIVE_TABLE_DB1_TBL2);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to add relationship %s", request.getUser(), request.getRelationshipType())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to add relationship %s", request.getUser(), request.getRelationshipType()).isTrue();
    }

    @Test
    public void testUpdateRelationship() {
        AtlasRelationshipAccessRequest request = new AtlasRelationshipAccessRequest(typeRegistry, AtlasPrivilege.RELATIONSHIP_UPDATE, RELATIONSHIP_TYPE_CLONE_OF, ENTITY_HIVE_TABLE_DB1_TBL1, ENTITY_HIVE_TABLE_DB1_TBL2);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to update relationship %s", request.getUser(), request.getRelationshipType())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to update relationship %s", request.getUser(), request.getRelationshipType())
                .isTrue();
    }

    @Test
    public void testRemoveRelationship() {
        AtlasRelationshipAccessRequest request = new AtlasRelationshipAccessRequest(typeRegistry, AtlasPrivilege.RELATIONSHIP_REMOVE, RELATIONSHIP_TYPE_CLONE_OF, ENTITY_HIVE_TABLE_DB1_TBL1, ENTITY_HIVE_TABLE_DB1_TBL2);

        request.setUser(USER_USER1, USER_USER1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be denied to remove relationship %s", request.getUser(), request.getRelationshipType())
                .isFalse();

        request.setUser(USER_STEWARD1, USER_STEWARD1_GROUPS);

        assertThat(authorizer.isAccessAllowed(request))
                .as("%s should be allowed to remove relationship %s", request.getUser(), request.getRelationshipType()).isTrue();
    }
}
