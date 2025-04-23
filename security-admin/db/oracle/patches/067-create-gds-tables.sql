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

CREATE OR REPLACE PROCEDURE spdropsequence(ObjName IN varchar2)
IS
v_counter integer;
BEGIN
    select count(*) into v_counter from user_sequences where sequence_name = upper(ObjName);
      if (v_counter > 0) then
        execute immediate 'DROP SEQUENCE ' || ObjName;
      end if;
END;/
/

call spdropsequence('X_GDS_DATASET_SEQ');
call spdropsequence('X_GDS_PROJECT_SEQ');
call spdropsequence('X_GDS_DATA_SHARE_SEQ');
call spdropsequence('X_GDS_SHARED_RESOURCE_SEQ');
call spdropsequence('X_GDS_DATA_SHARE_IN_DATASET_SEQ');
call spdropsequence('X_GDS_DATASET_IN_PROJECT_SEQ');
call spdropsequence('X_GDS_DATASET_POLICY_MAP_SEQ');
call spdropsequence('X_GDS_PROJECT_POLICY_MAP_SEQ');

commit;

CREATE SEQUENCE X_GDS_DATASET_SEQ               START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GDS_PROJECT_SEQ               START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GDS_DATA_SHARE_SEQ            START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GDS_SHARED_RESOURCE_SEQ       START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GDS_DATA_SHARE_IN_DATASET_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GDS_DATASET_IN_PROJECT_SEQ    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GDS_DATASET_POLICY_MAP_SEQ    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GDS_PROJECT_POLICY_MAP_SEQ    START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;


CREATE OR REPLACE PROCEDURE spdroptable(ObjName IN varchar2)
IS
v_counter integer;
BEGIN
    select count(*) into v_counter from user_tables where table_name = upper(ObjName);
     if (v_counter > 0) then
     execute immediate 'drop table ' || ObjName || ' cascade constraints';
     end if;
END;/
/

call spdroptable('x_gds_dataset');
call spdroptable('x_gds_project');
call spdroptable('x_gds_data_share');
call spdroptable('x_gds_shared_resource');
call spdroptable('x_gds_data_share_in_dataset');
call spdroptable('x_gds_dataset_in_project');
call spdroptable('x_gds_dataset_policy_map');
call spdroptable('x_gds_project_policy_map');


CREATE TABLE x_gds_dataset (
    id              NUMBER(20)    NOT NULL,
    guid            VARCHAR2(64)  DEFAULT NULL NULL,
    create_time     DATE          DEFAULT  NULL NULL,
    update_time     DATE          DEFAULT  NULL NULL,
    added_by_id     NUMBER(20)    DEFAULT NULL NULL,
    upd_by_id       NUMBER(20)    DEFAULT NULL NULL,
    version         NUMBER(20)    DEFAULT '1' NOT NULL,
    is_enabled      NUMBER(1)     DEFAULT '1' NOT NULL,
    name            VARCHAR2(512) DEFAULT NULL NULL,
    description     CLOB          DEFAULT NULL NULL,
    acl             CLOB          DEFAULT NULL NULL,
    terms_of_use    CLOB          DEFAULT NULL NULL,
    options         CLOB          DEFAULT NULL NULL,
    additional_info CLOB          DEFAULT NULL NULL,
    primary key (id),
    CONSTRAINT x_gds_dataset_UK_name UNIQUE(name),
    CONSTRAINT x_gds_dataset_FK_added_by_id FOREIGN KEY(added_by_id) REFERENCES x_portal_user(id),
    CONSTRAINT x_gds_dataset_FK_upd_by_id   FOREIGN KEY(upd_by_id)   REFERENCES x_portal_user(id)
);

CREATE INDEX x_gds_dataset_guid ON x_gds_dataset(guid);


CREATE TABLE x_gds_project (
    id              NUMBER(20)    NOT NULL,
    guid            VARCHAR2(64)  DEFAULT NULL NULL,
    create_time     DATE          DEFAULT NULL NULL,
    update_time     DATE          DEFAULT NULL NULL,
    added_by_id     NUMBER(20)    DEFAULT NULL NULL,
    upd_by_id       NUMBER(20)    DEFAULT NULL NULL,
    version         NUMBER(20)    DEFAULT '1' NOT NULL,
    is_enabled      NUMBER(1)     DEFAULT '1' NOT NULL,
    name            VARCHAR2(512) DEFAULT NULL NULL,
    description     CLOB          DEFAULT NULL NULL,
    acl             CLOB          DEFAULT NULL NULL,
    terms_of_use    CLOB          DEFAULT NULL NULL,
    options         CLOB          DEFAULT NULL NULL,
    additional_info CLOB          DEFAULT NULL NULL,
    PRIMARY KEY (id),
    CONSTRAINT x_gds_project_UK_name UNIQUE(name),
    CONSTRAINT x_gds_project_FK_added_by_id FOREIGN KEY(added_by_id) REFERENCES x_portal_user(id),
    CONSTRAINT x_gds_project_FK_upd_by_id   FOREIGN KEY(upd_by_id)   REFERENCES x_portal_user(id)
);

CREATE INDEX x_gds_project_guid ON x_gds_project(guid);


CREATE TABLE x_gds_data_share (
    id               NUMBER(20)     NOT NULL,
    guid             VARCHAR2(64)   DEFAULT NULL NULL,
    create_time      DATE           DEFAULT NULL NULL,
    update_time      DATE           DEFAULT NULL NULL,
    added_by_id      NUMBER(20)     DEFAULT NULL NULL,
    upd_by_id        NUMBER(20)     DEFAULT NULL NULL,
    version          NUMBER(20)     DEFAULT '1' NOT NULL,
    is_enabled       NUMBER(1)      DEFAULT '1' NOT NULL,
    name             VARCHAR2(512)  NOT NULL,
    description      CLOB           DEFAULT NULL NULL,
    acl              CLOB           NOT NULL,
    service_id           NUMBER(20) NOT NULL,
    zone_id              NUMBER(20) NOT NULL,
    condition_expr       CLOB       DEFAULT NULL NULL,
    default_access_types CLOB       DEFAULT NULL NULL,
    default_tag_masks    CLOB       DEFAULT NULL NULL,
    terms_of_use         CLOB       DEFAULT NULL NULL,
    options              CLOB       DEFAULT NULL NULL,
    additional_info      CLOB       DEFAULT NULL NULL,
    PRIMARY KEY (id),
    CONSTRAINT x_gds_data_share_UK_name UNIQUE(service_id, zone_id, name),
    CONSTRAINT x_gds_data_share_FK_added_by_id FOREIGN KEY(added_by_id) REFERENCES x_portal_user(id),
    CONSTRAINT x_gds_data_share_FK_upd_by_id   FOREIGN KEY(upd_by_id)   REFERENCES x_portal_user(id),
    CONSTRAINT x_gds_data_share_FK_service_id  FOREIGN KEY(service_id)  REFERENCES x_service(id),
    CONSTRAINT x_gds_data_share_FK_zone_id     FOREIGN KEY(zone_id)     REFERENCES x_security_zone(id)
);

CREATE INDEX x_gds_data_share_guid       ON x_gds_data_share(guid);
CREATE INDEX x_gds_data_share_service_id ON x_gds_data_share(service_id);
CREATE INDEX x_gds_data_share_zone_id    ON x_gds_data_share(zone_id);


CREATE TABLE x_gds_shared_resource (
    id                   NUMBER(20)    NOT NULL,
    guid                 VARCHAR2(64)  NOT NULL,
    create_time          DATE          DEFAULT NULL NULL,
    update_time          DATE          DEFAULT NULL NULL,
    added_by_id          NUMBER(20)    DEFAULT NULL NULL,
    upd_by_id            NUMBER(20)    DEFAULT NULL NULL,
    version              NUMBER(20)    DEFAULT '1' NOT NULL,
    is_enabled           NUMBER(1)     DEFAULT '1' NOT NULL,
    name                 VARCHAR2(512) NOT NULL,
    description          CLOB          DEFAULT NULL NULL,
    data_share_id        NUMBER(20)    NOT NULL,
    "resource"           CLOB          NOT NULL,
    resource_signature   VARCHAR2(128) NOT NULL,
    sub_resource         CLOB          DEFAULT NULL NULL,
    sub_resource_type    CLOB          DEFAULT NULL NULL,
    condition_expr       CLOB          DEFAULT NULL NULL,
    access_types         CLOB          DEFAULT NULL NULL,
    row_filter           CLOB          DEFAULT NULL NULL,
    sub_resource_masks   CLOB          DEFAULT NULL NULL,
    profiles             CLOB          DEFAULT NULL NULL,
    options              CLOB          DEFAULT NULL NULL,
    additional_info      CLOB          DEFAULT NULL NULL,
    PRIMARY KEY (id),
    CONSTRAINT x_gds_shared_resource_UK_name UNIQUE(data_share_id, name),
    CONSTRAINT x_gds_shared_resource_UK_resource_signature UNIQUE(data_share_id, resource_signature),
    CONSTRAINT x_gds_shared_resource_FK_added_by_id   FOREIGN KEY(added_by_id)   REFERENCES x_portal_user(id),
    CONSTRAINT x_gds_shared_resource_FK_upd_by_id     FOREIGN KEY(upd_by_id)     REFERENCES x_portal_user(id),
    CONSTRAINT x_gds_shared_resource_FK_data_share_id FOREIGN KEY(data_share_id) REFERENCES x_gds_data_share(id)
);

CREATE INDEX x_gds_shared_resource_guid          ON x_gds_shared_resource(guid);
CREATE INDEX x_gds_shared_resource_data_share_id ON x_gds_shared_resource(data_share_id);


CREATE TABLE x_gds_data_share_in_dataset (
    id                NUMBER(20)    NOT NULL,
    guid              VARCHAR2(64)  NOT NULL,
    create_time       DATE          DEFAULT NULL NULL,
    update_time       DATE          DEFAULT NULL NULL,
    added_by_id       NUMBER(20)    DEFAULT NULL NULL,
    upd_by_id         NUMBER(20)    DEFAULT NULL NULL,
    version           NUMBER(20)    DEFAULT '1' NOT NULL,
    is_enabled        NUMBER(1)     DEFAULT '1' NOT NULL,
    description       CLOB          DEFAULT NULL NULL,
    data_share_id     NUMBER(20)    NOT NULL,
    dataset_id        NUMBER(20)    NOT NULL,
    status            NUMBER(5)     NOT NULL,
    validity_period   CLOB          DEFAULT NULL NULL,
    profiles          CLOB          DEFAULT NULL NULL,
    options           CLOB          DEFAULT NULL NULL,
    additional_info   CLOB          DEFAULT NULL NULL,
    approver_id       NUMBER(20)    DEFAULT NULL NULL,
    PRIMARY KEY (id),
    CONSTRAINT x_gds_dshid_UK_data_share_id_dataset_id UNIQUE(data_share_id, dataset_id),
    CONSTRAINT x_gds_dshid_FK_added_by_id    FOREIGN KEY (added_by_id)    REFERENCES x_portal_user(id),
    CONSTRAINT x_gds_dshid_FK_upd_by_id      FOREIGN KEY (upd_by_id)      REFERENCES x_portal_user(id),
    CONSTRAINT x_gds_dshid_FK_data_share_id  FOREIGN KEY (data_share_id)  REFERENCES x_gds_data_share(id),
    CONSTRAINT x_gds_dshid_FK_dataset_id     FOREIGN KEY (dataset_id)     REFERENCES x_gds_dataset(id),
    CONSTRAINT x_gds_dshid_FK_approver_id    FOREIGN KEY (approver_id)    REFERENCES x_portal_user(id)
);

CREATE INDEX x_gds_dshid_guid                     ON x_gds_data_share_in_dataset(guid);
CREATE INDEX x_gds_dshid_data_share_id            ON x_gds_data_share_in_dataset(data_share_id);
CREATE INDEX x_gds_dshid_dataset_id               ON x_gds_data_share_in_dataset(dataset_id);


CREATE TABLE x_gds_dataset_in_project (
    id               NUMBER(20)    NOT NULL,
    guid             VARCHAR2(64)  NOT NULL,
    create_time      DATE          DEFAULT NULL NULL,
    update_time      DATE          DEFAULT NULL NULL,
    added_by_id      NUMBER(20)    DEFAULT NULL NULL,
    upd_by_id        NUMBER(20)    DEFAULT NULL NULL,
    version          NUMBER(20)    DEFAULT '1' NOT NULL,
    is_enabled       NUMBER(1)     DEFAULT '1' NOT NULL,
    description      CLOB          DEFAULT NULL NULL,
    dataset_id       NUMBER(20)    NOT NULL,
    project_id       NUMBER(20)    NOT NULL,
    status           NUMBER(5)     NOT NULL,
    validity_period  CLOB          DEFAULT NULL NULL,
    profiles         CLOB          DEFAULT NULL NULL,
    options          CLOB          DEFAULT NULL NULL,
    additional_info  CLOB          DEFAULT NULL NULL,
    approver_id      NUMBER(20)    DEFAULT NULL NULL,
    PRIMARY KEY (id),
    CONSTRAINT x_gds_dip_UK_data_share_id_dataset_id UNIQUE(dataset_id, project_id),
    CONSTRAINT x_gds_dip_FK_added_by_id  FOREIGN KEY (added_by_id)  REFERENCES x_portal_user(id),
    CONSTRAINT x_gds_dip_FK_upd_by_id    FOREIGN KEY (upd_by_id)    REFERENCES x_portal_user(id),
    CONSTRAINT x_gds_dip_FK_dataset_id   FOREIGN KEY (dataset_id)   REFERENCES x_gds_dataset(id),
    CONSTRAINT x_gds_dip_FK_project_id   FOREIGN KEY (project_id)   REFERENCES x_gds_project(id),
    CONSTRAINT x_gds_dip_FK_approver_id  FOREIGN KEY (approver_id)  REFERENCES x_portal_user(id)
);

CREATE INDEX x_gds_dip_guid       ON x_gds_dataset_in_project(guid);
CREATE INDEX x_gds_dip_dataset_id ON x_gds_dataset_in_project(dataset_id);
CREATE INDEX x_gds_dip_project_id ON x_gds_dataset_in_project(project_id);


CREATE TABLE x_gds_dataset_policy_map (
    id         NUMBER(20) NOT NULL,
    dataset_id NUMBER(20) NOT NULL,
    policy_id  NUMBER(20) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT x_gds_dpm_UK_dataset_id_policy_id UNIQUE (dataset_id, policy_id),
    CONSTRAINT x_gds_dpm_FK_dataset_id FOREIGN KEY (dataset_id) REFERENCES x_gds_dataset(id),
    CONSTRAINT x_gds_dpm_FK_policy_id  FOREIGN KEY (policy_id)  REFERENCES x_policy(id)
);

CREATE INDEX x_gds_dpm_dataset_id ON x_gds_dataset_policy_map(dataset_id);
CREATE INDEX x_gds_dpm_policy_id  ON x_gds_dataset_policy_map(policy_id);


CREATE TABLE x_gds_project_policy_map (
    id         NUMBER(20) NOT NULL,
    project_id NUMBER(20) NOT NULL,
    policy_id  NUMBER(20) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT x_gds_ppm_UK_project_id_policy_id UNIQUE (project_id, policy_id),
    CONSTRAINT x_gds_ppm_FK_project_id FOREIGN KEY (project_id) REFERENCES x_gds_project(id),
    CONSTRAINT x_gds_ppm_FK_policy_id  FOREIGN KEY (policy_id)  REFERENCES x_policy(id)
);

CREATE INDEX x_gds_ppm_project_id ON x_gds_project_policy_map(project_id);
CREATE INDEX x_gds_ppm_policy_id  ON x_gds_project_policy_map(policy_id);
