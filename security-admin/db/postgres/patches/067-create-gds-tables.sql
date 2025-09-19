-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.


DROP TABLE IF EXISTS x_gds_dataset_policy_map;
DROP TABLE IF EXISTS x_gds_project_policy_map;
DROP TABLE IF EXISTS x_gds_dataset CASCADE;
DROP TABLE IF EXISTS x_gds_project CASCADE;
DROP TABLE IF EXISTS x_gds_data_share CASCADE;
DROP TABLE IF EXISTS x_gds_shared_resource CASCADE;
DROP TABLE IF EXISTS x_gds_data_share_in_dataset CASCADE;
DROP TABLE IF EXISTS x_gds_dataset_in_project CASCADE;

DROP SEQUENCE IF EXISTS x_gds_project_policy_map_seq;
DROP SEQUENCE IF EXISTS x_gds_dataset_policy_map_seq;
DROP SEQUENCE IF EXISTS X_GDS_DATASET_SEQ;
DROP SEQUENCE IF EXISTS X_GDS_PROJECT_SEQ;
DROP SEQUENCE IF EXISTS X_GDS_DATA_SHARE_SEQ;
DROP SEQUENCE IF EXISTS X_GDS_SHARED_RESOURCE_SEQ;
DROP SEQUENCE IF EXISTS X_GDS_DATA_SHARE_IN_DATASET_SEQ;
DROP SEQUENCE IF EXISTS X_GDS_DATASET_IN_PROJECT_SEQ;


CREATE SEQUENCE X_GDS_DATASET_SEQ;
CREATE TABLE x_gds_dataset (
    id              BIGINT       NOT NULL DEFAULT nextval('X_GDS_DATASET_SEQ'::regclass)
  , guid            VARCHAR(64)  NOT NULL
  , create_time     TIMESTAMP    NULL     DEFAULT NULL
  , update_time     TIMESTAMP    NULL     DEFAULT NULL
  , added_by_id     BIGINT       NULL     DEFAULT NULL
  , upd_by_id       BIGINT       NULL     DEFAULT NULL
  , version         BIGINT       NOT NULL DEFAULT 1
  , is_enabled      BOOLEAN      NOT NULL DEFAULT '1'
  , name            VARCHAR(512) NOT NULL
  , description     TEXT         NULL     DEFAULT NULL
  , acl             TEXT         NULL     DEFAULT NULL
  , terms_of_use    TEXT         NULL     DEFAULT NULL
  , options         TEXT         NULL     DEFAULT NULL
  , additional_info TEXT         NULL     DEFAULT NULL
  , PRIMARY KEY(id)
  , CONSTRAINT x_gds_dataset_UK_name UNIQUE(name)
  , CONSTRAINT x_gds_dataset_FK_added_by_id FOREIGN KEY(added_by_id) REFERENCES x_portal_user(id)
  , CONSTRAINT x_gds_dataset_FK_upd_by_id   FOREIGN KEY(upd_by_id)   REFERENCES x_portal_user(id)
);
CREATE INDEX x_gds_dataset_guid ON x_gds_dataset(guid);
commit;

CREATE SEQUENCE X_GDS_PROJECT_SEQ;
CREATE TABLE x_gds_project (
    id              BIGINT       NOT NULL DEFAULT nextval('X_GDS_PROJECT_SEQ'::regclass)
  , guid            VARCHAR(64)  NOT NULL
  , create_time     TIMESTAMP    NULL     DEFAULT NULL
  , update_time     TIMESTAMP    NULL     DEFAULT NULL
  , added_by_id     BIGINT       NULL     DEFAULT NULL
  , upd_by_id       BIGINT       NULL     DEFAULT NULL
  , version         BIGINT       NOT NULL DEFAULT 1
  , is_enabled      BOOLEAN      NOT NULL DEFAULT '1'
  , name            VARCHAR(512) NOT NULL
  , description     TEXT         NULL     DEFAULT NULL
  , acl             TEXT         NULL     DEFAULT NULL
  , terms_of_use    TEXT         NULL     DEFAULT NULL
  , options         TEXT         NULL     DEFAULT NULL
  , additional_info TEXT         NULL     DEFAULT NULL
  , PRIMARY KEY(id)
  , CONSTRAINT x_gds_project_UK_name UNIQUE(name)
  , CONSTRAINT x_gds_project_FK_added_by_id FOREIGN KEY(added_by_id) REFERENCES x_portal_user(id)
  , CONSTRAINT x_gds_project_FK_upd_by_id   FOREIGN KEY(upd_by_id)   REFERENCES x_portal_user(id)
);
CREATE INDEX x_gds_project_guid ON x_gds_project(guid);
commit;

CREATE SEQUENCE X_GDS_DATA_SHARE_SEQ;
CREATE TABLE x_gds_data_share(
    id                   BIGINT       NOT NULL DEFAULT nextval('X_GDS_DATA_SHARE_SEQ'::regclass)
  , guid                 VARCHAR(64)  NOT NULL
  , create_time          TIMESTAMP    NULL     DEFAULT NULL
  , update_time          TIMESTAMP    NULL     DEFAULT NULL
  , added_by_id          BIGINT       NULL     DEFAULT NULL
  , upd_by_id            BIGINT       NULL     DEFAULT NULL
  , version              BIGINT       NOT NULL DEFAULT 1
  , is_enabled           BOOLEAN      NOT NULL DEFAULT '1'
  , name                 VARCHAR(512) NOT NULL
  , description          TEXT         NULL     DEFAULT NULL
  , acl                  TEXT         NOT NULL
  , service_id           BIGINT       NOT NULL
  , zone_id              BIGINT       NOT NULL
  , condition_expr       TEXT         NULL
  , default_access_types TEXT         NULL
  , default_tag_masks    TEXT         NULL
  , terms_of_use         TEXT         NULL     DEFAULT NULL
  , options              TEXT         NULL     DEFAULT NULL
  , additional_info      TEXT         NULL     DEFAULT NULL
  , PRIMARY KEY(id)
  , CONSTRAINT x_gds_data_share_UK_name UNIQUE(service_id, zone_id, name)
  , CONSTRAINT x_gds_data_share_FK_added_by_id FOREIGN KEY(added_by_id) REFERENCES x_portal_user(id)
  , CONSTRAINT x_gds_data_share_FK_upd_by_id   FOREIGN KEY(upd_by_id)   REFERENCES x_portal_user(id)
  , CONSTRAINT x_gds_data_share_FK_service_id  FOREIGN KEY(service_id) REFERENCES x_service(id)
  , CONSTRAINT x_gds_data_share_FK_zone_id     FOREIGN KEY(zone_id)    REFERENCES x_security_zone(id)
);
CREATE INDEX x_gds_data_share_guid       ON x_gds_data_share(guid);
CREATE INDEX x_gds_data_share_service_id ON x_gds_data_share(service_id);
CREATE INDEX x_gds_data_share_zone_id    ON x_gds_data_share(zone_id);
commit;

CREATE SEQUENCE X_GDS_SHARED_RESOURCE_SEQ;
CREATE TABLE x_gds_shared_resource(
    id                   BIGINT       NOT NULL DEFAULT nextval('X_GDS_SHARED_RESOURCE_SEQ'::regclass)
  , guid                 VARCHAR(64)  NOT NULL
  , create_time          TIMESTAMP    NULL     DEFAULT NULL
  , update_time          TIMESTAMP    NULL     DEFAULT NULL
  , added_by_id          BIGINT       NULL     DEFAULT NULL
  , upd_by_id            BIGINT       NULL     DEFAULT NULL
  , version              BIGINT       NOT NULL DEFAULT 1
  , is_enabled           BOOLEAN      NOT NULL DEFAULT '1'
  , name                 VARCHAR(512) NOT NULL
  , description          TEXT         NULL     DEFAULT NULL
  , data_share_id        BIGINT       NOT NULL
  , resource_name        TEXT         NOT NULL
  , resource_signature   VARCHAR(128) NOT NULL
  , sub_resource        TEXT         NULL     DEFAULT NULL
  , sub_resource_type   TEXT         NULL     DEFAULT NULL
  , condition_expr       TEXT         NULL     DEFAULT NULL
  , access_types         TEXT         NULL     DEFAULT NULL
  , row_filter           TEXT         NULL     DEFAULT NULL
  , sub_resource_masks   TEXT         NULL     DEFAULT NULL
  , profiles             TEXT         NULL     DEFAULT NULL
  , options              TEXT         NULL     DEFAULT NULL
  , additional_info      TEXT         NULL     DEFAULT NULL
  , PRIMARY KEY(id)
  , CONSTRAINT x_gds_shared_resource_UK_name UNIQUE(data_share_id, name)
  , CONSTRAINT x_gds_shared_resource_UK_resource_signature UNIQUE(data_share_id, resource_signature)
  , CONSTRAINT x_gds_shared_resource_FK_added_by_id   FOREIGN KEY(added_by_id) REFERENCES x_portal_user(id)
  , CONSTRAINT x_gds_shared_resource_FK_upd_by_id     FOREIGN KEY(upd_by_id)   REFERENCES x_portal_user(id)
  , CONSTRAINT x_gds_shared_resource_FK_data_share_id FOREIGN KEY(data_share_id) REFERENCES x_gds_data_share(id)
);
CREATE INDEX x_gds_shared_resource_guid          ON x_gds_shared_resource(guid);
CREATE INDEX x_gds_shared_resource_data_share_id ON x_gds_shared_resource(data_share_id);
commit;

CREATE SEQUENCE X_GDS_DATA_SHARE_IN_DATASET_SEQ;
CREATE TABLE x_gds_data_share_in_dataset(
    id                   BIGINT       NOT NULL DEFAULT nextval('X_GDS_SHARED_RESOURCE_SEQ'::regclass)
  , guid                 VARCHAR(64)  NOT NULL
  , create_time          TIMESTAMP    NULL     DEFAULT NULL
  , update_time          TIMESTAMP    NULL     DEFAULT NULL
  , added_by_id          BIGINT       NULL     DEFAULT NULL
  , upd_by_id            BIGINT       NULL     DEFAULT NULL
  , version              BIGINT       NOT NULL DEFAULT 1
  , is_enabled           BOOLEAN      NOT NULL DEFAULT '1'
  , description          TEXT         NULL     DEFAULT NULL
  , data_share_id        BIGINT       NOT NULL
  , dataset_id           BIGINT       NOT NULL
  , status               SMALLINT     NOT NULL
  , validity_period      TEXT         NULL     DEFAULT NULL
  , profiles             TEXT         NULL     DEFAULT NULL
  , options              TEXT         NULL     DEFAULT NULL
  , additional_info      TEXT         NULL     DEFAULT NULL
  , approver_id          BIGINT       NULL     DEFAULT NULL
  , PRIMARY KEY(id)
  , CONSTRAINT x_gds_dshid_FK_added_by_id   FOREIGN KEY(added_by_id)   REFERENCES x_portal_user(id)
  , CONSTRAINT x_gds_dshid_FK_upd_by_id     FOREIGN KEY(upd_by_id)     REFERENCES x_portal_user(id)
  , CONSTRAINT x_gds_dshid_FK_data_share_id FOREIGN KEY(data_share_id) REFERENCES x_gds_data_share(id)
  , CONSTRAINT x_gds_dshid_FK_dataset_id    FOREIGN KEY(dataset_id)    REFERENCES x_gds_dataset(id)
  , CONSTRAINT x_gds_dshid_UK_data_share_id_dataset_id UNIQUE(data_share_id, dataset_id)
  , CONSTRAINT x_gds_dshid_FK_approver_id   FOREIGN KEY(approver_id)   REFERENCES x_portal_user(id)
);
CREATE INDEX x_gds_dshid_guid                     ON x_gds_data_share_in_dataset(guid);
CREATE INDEX x_gds_dshid_data_share_id            ON x_gds_data_share_in_dataset(data_share_id);
CREATE INDEX x_gds_dshid_dataset_id               ON x_gds_data_share_in_dataset(dataset_id);
CREATE INDEX x_gds_dshid_data_share_id_dataset_id ON x_gds_data_share_in_dataset(data_share_id, dataset_id);
commit;

CREATE SEQUENCE X_GDS_DATASET_IN_PROJECT_SEQ;
CREATE TABLE x_gds_dataset_in_project(
    id                   BIGINT       NOT NULL DEFAULT nextval('X_GDS_DATASET_IN_PROJECT_SEQ'::regclass)
  , guid                 VARCHAR(64)  NOT NULL
  , create_time          TIMESTAMP    NULL     DEFAULT NULL
  , update_time          TIMESTAMP    NULL     DEFAULT NULL
  , added_by_id          BIGINT       NULL     DEFAULT NULL
  , upd_by_id            BIGINT       NULL     DEFAULT NULL
  , version              BIGINT       NOT NULL DEFAULT 1
  , is_enabled           BOOLEAN      NOT NULL DEFAULT '1'
  , description          TEXT         NULL     DEFAULT NULL
  , dataset_id           BIGINT       NOT NULL
  , project_id           BIGINT       NOT NULL
  , status               SMALLINT     NOT NULL
  , validity_period      TEXT         NULL     DEFAULT NULL
  , profiles             TEXT         NULL     DEFAULT NULL
  , options              TEXT         NULL     DEFAULT NULL
  , additional_info      TEXT         NULL     DEFAULT NULL
  , approver_id          BIGINT       NULL     DEFAULT NULL
  , PRIMARY KEY(id)
  , CONSTRAINT x_gds_dip_FK_added_by_id FOREIGN KEY(added_by_id) REFERENCES x_portal_user(id)
  , CONSTRAINT x_gds_dip_FK_upd_by_id   FOREIGN KEY(upd_by_id)   REFERENCES x_portal_user(id)
  , CONSTRAINT x_gds_dip_FK_dataset_id  FOREIGN KEY(dataset_id)  REFERENCES x_gds_dataset(id)
  , CONSTRAINT x_gds_dip_FK_project_id  FOREIGN KEY(project_id)  REFERENCES x_gds_project(id)
  , CONSTRAINT x_gds_dip_UK_data_share_id_dataset_id UNIQUE(dataset_id, project_id)
  , CONSTRAINT x_gds_dip_FK_approver_id FOREIGN KEY(approver_id) REFERENCES x_portal_user(id)
);
CREATE INDEX x_gds_dip_guid       ON x_gds_dataset_in_project(guid);
CREATE INDEX x_gds_dip_dataset_id ON x_gds_dataset_in_project(dataset_id);
CREATE INDEX x_gds_dip_project_id ON x_gds_dataset_in_project(project_id);
commit;

CREATE SEQUENCE x_gds_dataset_policy_map_seq;
CREATE TABLE x_gds_dataset_policy_map(
    id         BIGINT NOT NULL DEFAULT nextval('x_gds_dataset_policy_map_seq'::regclass)
  , dataset_id BIGINT NOT NULL
  , policy_id  BIGINT NOT NULL
  , PRIMARY KEY(id)
  , CONSTRAINT x_gds_dpm_FK_dataset_id FOREIGN KEY(dataset_id) REFERENCES x_gds_dataset(id)
  , CONSTRAINT x_gds_dpm_FK_policy_id  FOREIGN KEY(policy_id)  REFERENCES x_policy(id)
  , CONSTRAINT x_gds_dpm_UK_dataset_id_policy_id UNIQUE(dataset_id, policy_id)
);
CREATE INDEX x_gds_dpm_dataset_id ON x_gds_dataset_policy_map(dataset_id);
CREATE INDEX x_gds_dpm_policy_id  ON x_gds_dataset_policy_map(policy_id);
commit;

CREATE SEQUENCE x_gds_project_policy_map_seq;
CREATE TABLE x_gds_project_policy_map(
    id         BIGINT NOT NULL DEFAULT nextval('x_gds_project_policy_map_seq'::regclass)
  , project_id BIGINT NOT NULL
  , policy_id  BIGINT NOT NULL
  , PRIMARY KEY(id)
  , CONSTRAINT x_gds_ppm_FK_project_id FOREIGN KEY(project_id) REFERENCES x_gds_project(id)
  , CONSTRAINT x_gds_ppm_FK_policy_id  FOREIGN KEY(policy_id)  REFERENCES x_policy(id)
  , CONSTRAINT x_gds_ppm_UK_project_id_policy_id UNIQUE(project_id, policy_id)
);
CREATE INDEX x_gds_ppm_project_id ON x_gds_project_policy_map(project_id);
CREATE INDEX x_gds_ppm_policy_id  ON x_gds_project_policy_map(policy_id);
commit;
