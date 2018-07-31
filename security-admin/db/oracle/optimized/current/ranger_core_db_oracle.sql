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

-- create sequences
CREATE SEQUENCE SEQ_GEN_IDENTITY START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_ACCESS_AUDIT_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_ASSET_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_AUDIT_MAP_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_AUTH_SESS_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_CRED_STORE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_DB_BASE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GROUP_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GROUP_GROUPS_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GROUP_USERS_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_PERM_MAP_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_EXPORT_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_PORTAL_USER_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_PORTAL_USER_ROLE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_RESOURCE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_TRX_LOG_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_USER_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE V_TRX_LOG_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE XA_ACCESS_AUDIT_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_SERVICE_DEF_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_SERVICE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_SERVICE_CONFIG_DEF_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_RESOURCE_DEF_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_ACCESS_TYPE_DEF_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_ACCESS_TYPE_DEF_GRANTS_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_CONDITION_DEF_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_ENUM_DEF_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_ENUM_ELEMENT_DEF_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_SERVICE_CONFIG_MAP_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_RESOURCE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_RESOURCE_MAP_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_ITEM_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_ITEM_ACCESS_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_ITEM_CONDITION_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_CONTEXT_ENRICHER_DEF_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_ITEM_USER_PERM_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_ITEM_GROUP_PERM_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_DATA_HIST_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_MODULES_MASTER_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_USER_MODULE_PERM_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GROUP_MODULE_PERM_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_TAG_DEF_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_TAG_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_SERVICE_RESOURCE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_SERVICE_RESOURCE_ELEMENT_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_TAG_ATTR_DEF_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_TAG_ATTR_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_TAG_RESOURCE_MAP_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_SERVICE_RES_EL_VAL_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_DATAMASK_TYPE_DEF_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_ITEM_DATAMASK_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_ITEM_ROWFILTER_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_SERVICE_VERSION_INFO_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_PLUGIN_INFO_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_LABEL_MAP_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_LABEL_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_UGSYNC_AUDIT_INFO_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
--CREATE SEQUENCE X_DB_VERSION_H_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
commit;


-- create tables
CREATE TABLE x_portal_user (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        first_name VARCHAR(256) DEFAULT NULL NULL ,
        last_name VARCHAR(256) DEFAULT NULL NULL ,
        pub_scr_name VARCHAR(2048) DEFAULT NULL NULL ,
        login_id VARCHAR(767) DEFAULT NULL NULL ,
        password VARCHAR(512) NOT NULL,
        email VARCHAR(512) DEFAULT NULL NULL ,
        status NUMBER(11) DEFAULT '0' NOT NULL ,
        user_src NUMBER(11) DEFAULT '0' NOT NULL ,
        notes VARCHAR(4000) DEFAULT NULL NULL ,
        PRIMARY KEY (id),
        CONSTRAINT x_portal_user_UK_login_id UNIQUE (login_id) ,
        CONSTRAINT x_portal_user_UK_email UNIQUE (email),
        CONSTRAINT x_portal_user_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_portal_user_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_portal_user_role (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE  DEFAULT NULL NULL ,
        added_by_id NUMBER(20)  DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        user_id NUMBER(20) NOT NULL ,
        user_role VARCHAR(128)  DEFAULT NULL NULL ,
        status NUMBER(11) DEFAULT 0 NOT NULL ,
        PRIMARY KEY (id),
        CONSTRAINT x_portal_user_role_FK_addedby FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_portal_user_role_FK_updby FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_portal_user_role_FK_user_id FOREIGN KEY (user_id) REFERENCES x_portal_user (id)
);

CREATE TABLE xa_access_audit (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        audit_type NUMBER(11) DEFAULT '0' NOT NULL ,
        access_result NUMBER(11) DEFAULT '0' NULL ,
        access_type VARCHAR(255) DEFAULT NULL NULL ,
        acl_enforcer VARCHAR(255) DEFAULT NULL NULL ,
        agent_id VARCHAR(255) DEFAULT NULL NULL ,
        client_ip VARCHAR(255) DEFAULT NULL NULL ,
        client_type VARCHAR(255) DEFAULT NULL NULL ,
        policy_id NUMBER(20) DEFAULT '0' NULL ,
        repo_name VARCHAR(255) DEFAULT NULL NULL ,
        repo_type NUMBER(11) DEFAULT '0' NULL,
        result_reason VARCHAR(255) DEFAULT NULL NULL ,
        session_id VARCHAR(255) DEFAULT NULL NULL ,
        event_time DATE DEFAULT NULL NULL ,
        request_user VARCHAR(255) DEFAULT NULL NULL ,
        action VARCHAR(2000) DEFAULT NULL NULL ,
        request_data VARCHAR(2000) DEFAULT NULL NULL ,
        resource_path VARCHAR(2000) DEFAULT NULL NULL ,
        resource_type VARCHAR(255) DEFAULT NULL NULL ,
        PRIMARY KEY (id)
);

CREATE TABLE x_asset (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        asset_name VARCHAR(1024) NOT NULL,
        descr VARCHAR(4000) DEFAULT NULL NULL,
        act_status NUMBER(11) DEFAULT '0' NOT NULL ,
        asset_type NUMBER(11)  DEFAULT '0' NOT NULL,
        config CLOB NULL,
        sup_native NUMBER(1) DEFAULT '0' NOT NULL,
        PRIMARY KEY (id),
        CONSTRAINT x_asset_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_asset_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_auth_sess (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        login_id VARCHAR(767) NOT NULL,
        user_id NUMBER(20) DEFAULT NULL NULL ,
        ext_sess_id VARCHAR(512) DEFAULT NULL NULL ,
        auth_time DATE NOT NULL,
        auth_status NUMBER(11) DEFAULT '0' NOT NULL ,
        auth_type NUMBER(11) DEFAULT '0' NOT NULL ,
        auth_provider NUMBER(11) DEFAULT '0' NOT NULL ,
        device_type NUMBER(11) DEFAULT '0' NOT NULL ,
        req_ip VARCHAR(48) NOT NULL,
        req_ua VARCHAR(1024) DEFAULT NULL NULL ,
        PRIMARY KEY (id),
        CONSTRAINT x_auth_sess_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_auth_sess_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_auth_sess_FK_user_id FOREIGN KEY (user_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_cred_store (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        store_name VARCHAR(1024) NOT NULL,
        descr VARCHAR(4000) NOT NULL,
        PRIMARY KEY (id),
        CONSTRAINT x_cred_store_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_cred_store_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_db_base (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        PRIMARY KEY (id),
        CONSTRAINT x_db_base_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_db_base_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE X_GROUP(
        ID NUMBER(20,0) NOT NULL ENABLE,
        CREATE_TIME DATE DEFAULT NULL,
        UPDATE_TIME DATE DEFAULT NULL,
        ADDED_BY_ID NUMBER(20,0) DEFAULT NULL,
        UPD_BY_ID NUMBER(20,0) DEFAULT NULL,
        GROUP_NAME VARCHAR2(1024) NOT NULL ENABLE,
        DESCR VARCHAR2(4000) DEFAULT NULL NULL,
        STATUS NUMBER(11,0) DEFAULT '0' NOT NULL ENABLE,
        GROUP_TYPE NUMBER(11,0) DEFAULT '0' NOT NULL ENABLE,
        CRED_STORE_ID NUMBER(20,0) DEFAULT NULL,
        group_src NUMBER(10) DEFAULT 0 NOT NULL,
        is_visible NUMBER(11) DEFAULT 1 NOT NULL,
        PRIMARY KEY (ID),
        CONSTRAINT x_group_UK_group_name UNIQUE (group_name),
        CONSTRAINT X_GROUP_FK_ADDED_BY_ID FOREIGN KEY (ADDED_BY_ID) REFERENCES X_PORTAL_USER (ID) ENABLE,
        CONSTRAINT X_GROUP_FK_CRED_STORE_ID FOREIGN KEY (CRED_STORE_ID) REFERENCES X_CRED_STORE (ID) ENABLE,
        CONSTRAINT X_GROUP_FK_UPD_BY_ID FOREIGN KEY (UPD_BY_ID) REFERENCES X_PORTAL_USER (ID) ENABLE
) ;

CREATE TABLE x_group_groups (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        group_name VARCHAR(1024) NOT NULL,
        p_group_id NUMBER(20) DEFAULT NULL NULL ,
        group_id NUMBER(20) DEFAULT NULL NULL ,
        PRIMARY KEY (id),
        CONSTRAINT x_group_groups_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_group_groups_FK_group_id FOREIGN KEY (group_id) REFERENCES x_group (id),
        CONSTRAINT x_group_groups_FK_p_group_id FOREIGN KEY (p_group_id) REFERENCES x_group (id),
        CONSTRAINT x_group_groups_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_user (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        user_name VARCHAR(767) NOT NULL,
        descr VARCHAR(4000) DEFAULT NULL  NULL,
        status NUMBER(11) DEFAULT '0' NOT NULL,
        cred_store_id NUMBER(20) DEFAULT NULL NULL ,
        is_visible NUMBER(11) DEFAULT 1 NOT NULL ,
        PRIMARY KEY (id),
        CONSTRAINT x_user_UK_user_name UNIQUE (user_name),
        CONSTRAINT x_user_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_user_FK_cred_store_id FOREIGN KEY (cred_store_id) REFERENCES x_cred_store (id),
        CONSTRAINT x_user_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_group_users (
        id NUMBER(20) NOT NULL ,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        group_name VARCHAR(767) NOT NULL,
        p_group_id NUMBER(20) DEFAULT NULL NULL ,
        user_id NUMBER(20) DEFAULT NULL NULL ,
        PRIMARY KEY (id),
        CONSTRAINT x_group_users_uk_uid_gname UNIQUE (user_id,group_name),
        CONSTRAINT x_group_users_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_group_users_FK_p_group_id FOREIGN KEY (p_group_id) REFERENCES x_group (id),
        CONSTRAINT x_group_users_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_group_users_FK_user_id FOREIGN KEY (user_id) REFERENCES x_user (id)
);

CREATE TABLE x_policy_export_audit (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        client_ip VARCHAR(255) NOT NULL,
        agent_id VARCHAR(255) DEFAULT NULL NULL ,
        req_epoch NUMBER(20) NOT NULL,
        last_updated DATE DEFAULT NULL NULL ,
        repository_name VARCHAR(1024) DEFAULT NULL NULL ,
        exported_json CLOB NULL,
        http_ret_code NUMBER(11) DEFAULT '0' NOT NULL ,
        cluster_name varchar(255) DEFAULT NULL NULL ,
        PRIMARY KEY (id),
        CONSTRAINT x_policy_export_audit_FK_added FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_policy_export_audit_FK_upd FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_resource (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        res_name VARCHAR(4000) DEFAULT NULL NULL ,
        descr VARCHAR(4000) DEFAULT NULL NULL ,
        res_type NUMBER(11) DEFAULT '0' NOT NULL ,
        asset_id NUMBER(20) NOT NULL,
        parent_id NUMBER(20) DEFAULT NULL NULL ,
        parent_path VARCHAR(4000) DEFAULT NULL NULL ,
        is_encrypt NUMBER(11) DEFAULT '0' NOT NULL ,
        is_recursive NUMBER(11) DEFAULT '0' NOT NULL ,
        res_group VARCHAR(1024) DEFAULT NULL NULL ,
        res_dbs CLOB NULL,
        res_tables CLOB NULL,
        res_col_fams CLOB NULL,
        res_cols CLOB NULL,
        res_udfs CLOB NULL,
        res_status NUMBER(11) DEFAULT '1' NOT NULL,
        table_type NUMBER(11) DEFAULT '0' NOT NULL,
        col_type NUMBER(11) DEFAULT '0' NOT NULL,
    policy_name VARCHAR(500)  DEFAULT NULL NULL,
    res_topologies CLOB DEFAULT NULL NULL,
    res_services CLOB DEFAULT NULL NULL,
        PRIMARY KEY (id),
        CONSTRAINT x_resource_UK_policy_name UNIQUE (policy_name),
        CONSTRAINT x_resource_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_resource_FK_asset_id FOREIGN KEY (asset_id) REFERENCES x_asset (id),
        CONSTRAINT x_resource_FK_parent_id FOREIGN KEY (parent_id) REFERENCES x_resource (id),
        CONSTRAINT x_resource_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);


CREATE TABLE x_trx_log (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        class_type NUMBER(11) DEFAULT '0' NOT NULL ,
        object_id NUMBER(20) DEFAULT NULL NULL ,
        parent_object_id NUMBER(20) DEFAULT NULL NULL ,
        parent_object_class_type NUMBER(11) DEFAULT '0' NOT NULL ,
        parent_object_name VARCHAR(1024) DEFAULT NULL NULL ,
        object_name VARCHAR(1024) DEFAULT NULL NULL ,
        attr_name VARCHAR(255) DEFAULT NULL NULL ,
        prev_val CLOB DEFAULT NULL NULL ,
        new_val CLOB DEFAULT NULL NULL ,
        trx_id VARCHAR(1024) DEFAULT NULL NULL ,
        action VARCHAR(255) DEFAULT NULL NULL ,
        sess_id VARCHAR(512) DEFAULT NULL NULL ,
        req_id VARCHAR(30) DEFAULT NULL NULL ,
        sess_type VARCHAR(30) DEFAULT NULL NULL ,
        PRIMARY KEY (id),
        CONSTRAINT x_trx_log_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_trx_log_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);
CREATE TABLE x_perm_map (
        id NUMBER(20) NOT NULL,
        create_time DATE DEFAULT NULL NULL ,
        update_time DATE DEFAULT NULL NULL ,
        added_by_id NUMBER(20) DEFAULT NULL NULL ,
        upd_by_id NUMBER(20) DEFAULT NULL NULL ,
        perm_group VARCHAR(1024) DEFAULT NULL NULL ,
        res_id NUMBER(20) DEFAULT NULL NULL ,
        group_id NUMBER(20) DEFAULT NULL NULL ,
        user_id NUMBER(20) DEFAULT NULL NULL ,
        perm_for NUMBER(11) DEFAULT '0' NOT NULL ,
        perm_type NUMBER(11) DEFAULT '0' NOT NULL ,
        is_recursive NUMBER(11) DEFAULT '0' NOT NULL ,
        is_wild_card NUMBER(1) DEFAULT '1' NOT NULL ,
        grant_revoke NUMBER(1) DEFAULT '1' NOT NULL ,
        ip_address CLOB DEFAULT NULL NULL ,
        PRIMARY KEY (id),
        CONSTRAINT x_perm_map_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_perm_map_FK_group_id FOREIGN KEY (group_id) REFERENCES x_group (id),
        CONSTRAINT x_perm_map_FK_res_id FOREIGN KEY (res_id) REFERENCES x_resource (id),
        CONSTRAINT x_perm_map_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id),
        CONSTRAINT x_perm_map_FK_user_id FOREIGN KEY (user_id) REFERENCES x_user (id)
);

CREATE TABLE X_AUDIT_MAP
(	ID NUMBER(20,0) NOT NULL ENABLE,
        CREATE_TIME DATE DEFAULT NULL,
        UPDATE_TIME DATE DEFAULT NULL,
        ADDED_BY_ID NUMBER(20,0) DEFAULT NULL,
        UPD_BY_ID NUMBER(20,0) DEFAULT NULL,
        RES_ID NUMBER(20,0) DEFAULT NULL,
        GROUP_ID NUMBER(20,0) DEFAULT NULL,
        USER_ID NUMBER(20,0) DEFAULT NULL,
        AUDIT_TYPE NUMBER(11,0) DEFAULT 0 NOT NULL ENABLE,
        PRIMARY KEY (ID),
        CONSTRAINT X_AUDIT_MAP_FK_ADDED_BY_ID FOREIGN KEY (ADDED_BY_ID) REFERENCES X_PORTAL_USER (ID) ENABLE,
        CONSTRAINT X_AUDIT_MAP_FK_GROUP_ID FOREIGN KEY (GROUP_ID) REFERENCES X_GROUP (ID) ENABLE,
        CONSTRAINT X_AUDIT_MAP_FK_RES_ID FOREIGN KEY (RES_ID) REFERENCES X_RESOURCE (ID) ENABLE,
        CONSTRAINT X_AUDIT_MAP_FK_UPD_BY_ID FOREIGN KEY (UPD_BY_ID) REFERENCES X_PORTAL_USER (ID) ENABLE,
        CONSTRAINT X_AUDIT_MAP_FK_USER_ID FOREIGN KEY (USER_ID) REFERENCES X_USER (ID) ENABLE
);

CREATE TABLE x_service_def (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
version NUMBER(20) DEFAULT NULL NULL,
name VARCHAR(1024) DEFAULT NULL NULL,
impl_class_name VARCHAR(1024) DEFAULT NULL NULL,
label VARCHAR(1024) DEFAULT NULL NULL,
description VARCHAR(1024) DEFAULT NULL NULL,
rb_key_label VARCHAR(1024) DEFAULT NULL NULL,
rb_key_description VARCHAR(1024) DEFAULT NULL NULL,
is_enabled NUMBER(1) DEFAULT '1' NULL,
def_options VARCHAR(1024) DEFAULT NULL NULL,
PRIMARY KEY (id),
CONSTRAINT x_service_def_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_service_def_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_service(
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
version NUMBER(20) DEFAULT NULL NULL,
type NUMBER(20) DEFAULT NULL NULL,
name varchar(255) DEFAULT NULL NULL,
policy_version NUMBER(20) DEFAULT NULL NULL,
policy_update_time DATE DEFAULT NULL NULL,
description VARCHAR(1024) DEFAULT NULL NULL,
is_enabled NUMBER(1) DEFAULT '0' NOT NULL,
tag_service NUMBER(20) DEFAULT NULL NULL,
tag_version NUMBER(20) DEFAULT 0 NOT NULL,
tag_update_time DATE DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_service_name UNIQUE (name),
CONSTRAINT x_service_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_service_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_service_FK_type FOREIGN KEY (type) REFERENCES x_service_def (id),
CONSTRAINT x_service_FK_tag_service FOREIGN KEY (tag_service) REFERENCES x_service(id)
);

CREATE TABLE x_policy (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
version NUMBER(20) DEFAULT NULL NULL,
service NUMBER(20) NOT NULL,
name VARCHAR(512) NOT NULL,
policy_type NUMBER(11) DEFAULT '0' NULL,
description VARCHAR(1024) DEFAULT NULL NULL,
resource_signature VARCHAR(128) DEFAULT NULL NULL,
is_enabled NUMBER(1) DEFAULT '0' NOT NULL,
is_audit_enabled NUMBER(1) DEFAULT '0' NOT NULL,
policy_options varchar(4000) DEFAULT NULL NULL,
policy_priority NUMBER(11) DEFAULT 0 NOT NULL,
primary key (id),
CONSTRAINT x_policy_UK_name_service UNIQUE (name,service),
CONSTRAINT x_policy_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_policy_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_policy_FK_service FOREIGN KEY (service) REFERENCES x_service (id)
);

CREATE TABLE x_service_config_def (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
def_id NUMBER(20) NOT NULL,
item_id NUMBER(20) NOT NULL,
name VARCHAR(1024) DEFAULT NULL NULL,
type VARCHAR(1024) DEFAULT NULL NULL,
sub_type VARCHAR(1024) DEFAULT NULL NULL,
is_mandatory NUMBER(1) DEFAULT '0' NOT NULL,
default_value VARCHAR(1024) DEFAULT NULL NULL,
validation_reg_ex VARCHAR(1024) DEFAULT NULL NULL,
validation_message VARCHAR(1024) DEFAULT NULL NULL,
ui_hint VARCHAR(1024) DEFAULT NULL NULL,
label VARCHAR(1024) DEFAULT NULL NULL,
description VARCHAR(1024) DEFAULT NULL NULL,
rb_key_label VARCHAR(1024) DEFAULT NULL NULL,
rb_key_description VARCHAR(1024) DEFAULT NULL NULL,
rb_key_validation_message VARCHAR(1024) DEFAULT NULL NULL,
sort_order NUMBER(3) DEFAULT '0' NULL,
primary key (id),
CONSTRAINT x_service_conf_def_FK_defid FOREIGN KEY (def_id) REFERENCES x_service_def (id),
CONSTRAINT x_service_conf_def_FK_added_by FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_service_conf_def_FK_upd_by FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_resource_def (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
def_id NUMBER(20) NOT NULL,
item_id NUMBER(20) NOT NULL,
name VARCHAR(1024) DEFAULT NULL NULL,
type VARCHAR(1024) DEFAULT NULL NULL,
res_level NUMBER(20) DEFAULT NULL NULL,
parent NUMBER(20) DEFAULT NULL NULL,
mandatory NUMBER(1) DEFAULT '0' NOT NULL,
look_up_supported NUMBER(1) DEFAULT '0' NOT NULL,
recursive_supported NUMBER(1) DEFAULT '0' NOT NULL,
excludes_supported NUMBER(1) DEFAULT '0' NOT NULL,
matcher VARCHAR(1024) DEFAULT NULL NULL,
matcher_options varchar(1024) DEFAULT NULL NULL,
validation_reg_ex VARCHAR(1024) DEFAULT NULL NULL,
validation_message VARCHAR(1024) DEFAULT NULL NULL,
ui_hint VARCHAR(1024) DEFAULT NULL NULL,
label VARCHAR(1024) DEFAULT NULL NULL,
description VARCHAR(1024) DEFAULT NULL NULL,
rb_key_label VARCHAR(1024) DEFAULT NULL NULL,
rb_key_description VARCHAR(1024) DEFAULT NULL NULL,
rb_key_validation_message VARCHAR(1024) DEFAULT NULL NULL,
sort_order NUMBER(3) DEFAULT '0' NULL,
datamask_options VARCHAR(1024) DEFAULT NULL NULL,
rowfilter_options VARCHAR(1024) DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_resource_def_FK_parent FOREIGN KEY (parent) REFERENCES x_resource_def (id),
CONSTRAINT x_resource_def_FK_defid FOREIGN KEY (def_id) REFERENCES x_service_def (id),
CONSTRAINT x_resource_def_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_resource_def_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_access_type_def (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
def_id NUMBER(20) NOT NULL,
item_id NUMBER(20) NOT NULL,
name VARCHAR(1024) DEFAULT NULL NULL,
label VARCHAR(1024) DEFAULT NULL NULL,
rb_key_label VARCHAR(1024) DEFAULT NULL NULL,
sort_order NUMBER(3) DEFAULT '0' NULL,
datamask_options VARCHAR(1024) DEFAULT NULL NULL,
rowfilter_options VARCHAR(1024) DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_access_type_def_FK_defid FOREIGN KEY (def_id) REFERENCES x_service_def (id),
CONSTRAINT x_access_type_def_FK_added_by FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_access_type_def_FK_upd_by FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_access_type_def_grants (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
atd_id NUMBER(20) NOT NULL,
implied_grant VARCHAR(1024) DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_atd_grants_FK_atdid FOREIGN KEY (atd_id) REFERENCES x_access_type_def (id),
CONSTRAINT x_atd_grants_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_atd_grants_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_policy_condition_def (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
def_id NUMBER(20) NOT NULL,
item_id NUMBER(20) NOT NULL,
name VARCHAR(1024) DEFAULT NULL NULL,
evaluator VARCHAR(1024) DEFAULT NULL NULL,
evaluator_options VARCHAR(1024) DEFAULT NULL NULL,
validation_reg_ex VARCHAR(1024) DEFAULT NULL NULL,
validation_message VARCHAR(1024) DEFAULT NULL NULL,
ui_hint VARCHAR(1024) DEFAULT NULL NULL,
label VARCHAR(1024) DEFAULT NULL NULL,
description VARCHAR(1024) DEFAULT NULL NULL,
rb_key_label VARCHAR(1024) DEFAULT NULL NULL,
rb_key_description VARCHAR(1024) DEFAULT NULL NULL,
rb_key_validation_message VARCHAR(1024) DEFAULT NULL NULL,
sort_order NUMBER(3) DEFAULT '0' NULL,
primary key (id),
CONSTRAINT x_policy_cond_def_FK_defid FOREIGN KEY (def_id) REFERENCES x_service_def (id),
CONSTRAINT x_policy_cond_def_FK_added_by FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_policy_cond_def_FK_upd_by FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_context_enricher_def(
id NUMBER(20) NOT NULL,
guid varchar(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL,
upd_by_id NUMBER(20) DEFAULT NULL,
def_id NUMBER(20) NOT NULL,
item_id NUMBER(20) NOT NULL,
name varchar(1024) DEFAULT NULL NULL,
enricher varchar(1024) DEFAULT NULL NULL,
enricher_options varchar(1024) DEFAULT NULL NULL,
sort_order NUMBER(3) DEFAULT '0' NULL,
primary key (id),
CONSTRAINT x_cont_enr_def_FK_defid FOREIGN KEY (def_id) REFERENCES x_service_def (id),
CONSTRAINT x_cont_enr_def_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_cont_enr_def_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_enum_def (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
def_id NUMBER(20) NOT NULL,
item_id NUMBER(20) NOT NULL,
name VARCHAR(1024) DEFAULT NULL NULL,
default_index NUMBER(20) DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_enum_def_FK_def_id FOREIGN KEY (def_id) REFERENCES x_service_def (id),
CONSTRAINT x_enum_def_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_enum_def_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_enum_element_def (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
enum_def_id NUMBER(20) NOT NULL,
item_id NUMBER(20) NOT NULL,
name VARCHAR(1024) DEFAULT NULL NULL,
label VARCHAR(1024) DEFAULT NULL NULL,
rb_key_label VARCHAR(1024) DEFAULT NULL NULL,
sort_order NUMBER(3) DEFAULT '0' NULL,
primary key (id),
CONSTRAINT x_enum_element_def_FK_defid FOREIGN KEY (enum_def_id) REFERENCES x_enum_def (id),
CONSTRAINT x_enum_element_def_FK_added_by FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_enum_element_def_FK_upd_by FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_service_config_map (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
service NUMBER(20) NOT NULL,
config_key VARCHAR(1024) DEFAULT NULL NULL,
config_value VARCHAR(4000) DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_service_conf_map_FK_service FOREIGN KEY (service) REFERENCES x_service (id),
CONSTRAINT x_service_conf_map_FK_added_by FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_service_conf_map_FK_upd_by FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_policy_resource (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
policy_id NUMBER(20) NOT NULL,
res_def_id NUMBER(20) NOT NULL,
is_excludes NUMBER(1) DEFAULT '0' NOT NULL,
is_recursive NUMBER(1) DEFAULT '0' NOT NULL,
primary key (id),
CONSTRAINT x_policy_res_FK_policy_id FOREIGN KEY (policy_id) REFERENCES x_policy (id),
CONSTRAINT x_policy_res_FK_res_def_id FOREIGN KEY (res_def_id) REFERENCES x_resource_def (id),
CONSTRAINT x_policy_res_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_policy_res_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_policy_resource_map (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
resource_id NUMBER(20) NOT NULL,
value VARCHAR(1024) DEFAULT NULL NULL,
sort_order NUMBER(10) DEFAULT '0' NULL,
primary key (id),
CONSTRAINT x_policy_res_map_FK_res_id FOREIGN KEY (resource_id) REFERENCES x_policy_resource (id),
CONSTRAINT x_policy_res_map_FK_added_by FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_policy_res_map_FK_upd_by FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_policy_item (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
policy_id NUMBER(20) NOT NULL,
delegate_admin NUMBER(1) DEFAULT '0' NOT NULL,
sort_order NUMBER(3) DEFAULT '0' NULL,
item_type NUMBER(10) DEFAULT 0 NOT NULL,
is_enabled NUMBER(1) DEFAULT 1 NOT NULL,
comments VARCHAR(255) DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_policy_item_FK_policy_id FOREIGN KEY (policy_id) REFERENCES x_policy (id),
CONSTRAINT x_policy_item_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_policy_item_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_policy_item_access (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
policy_item_id NUMBER(20) NOT NULL,
type NUMBER(20) NOT NULL,
is_allowed NUMBER(3) DEFAULT '0' NOT NULL,
sort_order NUMBER(3) DEFAULT '0' NULL,
primary key (id),
CONSTRAINT x_plc_item_access_FK_pi_id FOREIGN KEY (policy_item_id) REFERENCES x_policy_item (id),
CONSTRAINT x_plc_item_access_FK_atd_id FOREIGN KEY (type) REFERENCES x_access_type_def (id),
CONSTRAINT x_plc_item_access_FK_added_by FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_plc_item_access_FK_upd_by FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_policy_item_condition (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
policy_item_id NUMBER(20) NOT NULL,
type NUMBER(20) NOT NULL,
value VARCHAR(1024) DEFAULT NULL NULL,
sort_order NUMBER(3) DEFAULT '0' NULL,
primary key (id),
CONSTRAINT x_plc_item_cond_FK_pi_id FOREIGN KEY (policy_item_id) REFERENCES x_policy_item (id),
CONSTRAINT x_plc_item_cond_FK_pcd_id FOREIGN KEY (type) REFERENCES x_policy_condition_def (id),
CONSTRAINT x_plc_item_cond_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_plc_item_cond_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_policy_item_user_perm (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
policy_item_id NUMBER(20) NOT NULL,
user_id NUMBER(20) DEFAULT NULL NULL,
sort_order NUMBER(10) DEFAULT '0' NULL,
primary key (id),
CONSTRAINT x_plc_itm_usr_perm_FK_pi_id FOREIGN KEY (policy_item_id) REFERENCES x_policy_item (id),
CONSTRAINT x_plc_itm_usr_perm_FK_user_id FOREIGN KEY (user_id) REFERENCES x_user (id),
CONSTRAINT x_plc_itm_usr_perm_FK_added_by FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_plc_itm_usr_perm_FK_upd_by FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_policy_item_group_perm (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
policy_item_id NUMBER(20) NOT NULL,
group_id NUMBER(20) DEFAULT NULL NULL,
sort_order NUMBER(10) DEFAULT '0' NULL,
primary key (id),
CONSTRAINT x_plc_itm_grp_perm_FK_pi_id FOREIGN KEY (policy_item_id) REFERENCES x_policy_item (id),
CONSTRAINT x_plc_itm_grp_perm_FK_group_id FOREIGN KEY (group_id) REFERENCES x_group (id),
CONSTRAINT x_plc_itm_grp_perm_FK_added_by FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_plc_itm_grp_perm_FK_upd_by FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_data_hist (
id NUMBER(20) NOT NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
obj_guid VARCHAR(1024) NOT NULL,
obj_class_type NUMBER(11) NOT NULL,
obj_id NUMBER(20) NOT NULL,
obj_name VARCHAR(1024) NOT NULL,
version NUMBER(20) DEFAULT NULL NULL,
action VARCHAR(512)NOT NULL,
from_time DATE NOT NULL,
to_time DATE DEFAULT NULL NULL,
content CLOB NOT NULL,
primary key (id)
);

CREATE TABLE x_modules_master(
id NUMBER(20) NOT NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
module VARCHAR(1024) NOT NULL,
url VARCHAR(1024) DEFAULT NULL NULL,
PRIMARY KEY (id)
);

INSERT INTO x_modules_master VALUES(X_MODULES_MASTER_SEQ.NEXTVAL,SYSDATE,SYSDATE,1,1,'Resource Based Policies','');
INSERT INTO x_modules_master VALUES(X_MODULES_MASTER_SEQ.NEXTVAL,SYSDATE,SYSDATE,1,1,'Users/Groups','');
INSERT INTO x_modules_master VALUES(X_MODULES_MASTER_SEQ.NEXTVAL,SYSDATE,SYSDATE,1,1,'Reports','');
INSERT INTO x_modules_master VALUES(X_MODULES_MASTER_SEQ.NEXTVAL,SYSDATE,SYSDATE,1,1,'Audit','');
INSERT INTO x_modules_master VALUES(X_MODULES_MASTER_SEQ.NEXTVAL,SYSDATE,SYSDATE,1,1,'Key Manager','');

CREATE TABLE x_user_module_perm(
id NUMBER(20) NOT NULL,
user_id NUMBER(20) DEFAULT NULL NULL,
module_id NUMBER(20) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
is_allowed NUMBER(11) DEFAULT '1' NOT NULL ,
PRIMARY KEY (id),
CONSTRAINT x_user_module_perm_FK_moduleid FOREIGN KEY (module_id) REFERENCES x_modules_master(id),
CONSTRAINT x_user_module_perm_FK_userid FOREIGN KEY (user_id) REFERENCES x_portal_user(id)
);

CREATE TABLE x_group_module_perm(
id NUMBER(20) NOT NULL,
group_id NUMBER(20) DEFAULT NULL NULL,
module_id NUMBER(20) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
is_allowed NUMBER(11) DEFAULT '1' NOT NULL ,
PRIMARY KEY (id),
CONSTRAINT x_grp_module_perm_FK_module_id FOREIGN KEY (module_id) REFERENCES x_modules_master(id),
CONSTRAINT x_grp_module_perm_FK_group_id FOREIGN KEY (group_id) REFERENCES x_group(id)
);

CREATE TABLE x_tag_def (
id NUMBER(20) NOT NULL,
guid VARCHAR(64) NOT NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
version NUMBER(20) DEFAULT NULL NULL,
name VARCHAR(255) NOT NULL,
source VARCHAR(128) DEFAULT NULL NULL,
is_enabled NUMBER(1) DEFAULT '0' NOT NULL,
PRIMARY KEY (id),
CONSTRAINT x_tag_def_UK_guid UNIQUE (guid),
CONSTRAINT x_tag_def_UK_name UNIQUE (name),
CONSTRAINT x_tag_def_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_tag_def_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_tag(
id NUMBER(20) NOT NULL,
guid VARCHAR(64) NOT NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
version NUMBER(20) DEFAULT NULL NULL,
type NUMBER(20) NOT NULL,
owned_by NUMBER(6) DEFAULT 0 NOT NULL,
policy_options varchar(4000) DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_tag_UK_guid UNIQUE (guid),
CONSTRAINT x_tag_FK_type FOREIGN KEY (type) REFERENCES x_tag_def (id),
CONSTRAINT x_tag_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_tag_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_service_resource(
id NUMBER(20) NOT NULL,
guid VARCHAR(64) NOT NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
version NUMBER(20) DEFAULT NULL NULL,
service_id NUMBER(20) NOT NULL,
resource_signature VARCHAR(128) DEFAULT NULL NULL,
is_enabled NUMBER(1) DEFAULT '1' NOT NULL,
primary key (id),
CONSTRAINT x_service_res_UK_guid UNIQUE (guid),
CONSTRAINT x_service_res_FK_service_id FOREIGN KEY (service_id) REFERENCES x_service (id),
CONSTRAINT x_service_res_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_service_res_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_service_resource_element(
id NUMBER(20) NOT NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
res_id NUMBER(20) NOT NULL,
res_def_id NUMBER(20) NOT NULL,
is_excludes NUMBER(1) DEFAULT '0' NOT NULL,
is_recursive NUMBER(1) DEFAULT '0' NOT NULL,
primary key (id),
CONSTRAINT x_srvc_res_el_FK_res_def_id FOREIGN KEY (res_def_id) REFERENCES x_resource_def (id),
CONSTRAINT x_srvc_res_el_FK_res_id FOREIGN KEY (res_id) REFERENCES x_service_resource (id),
CONSTRAINT x_srvc_res_el_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_srvc_res_el_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_tag_attr_def(
id NUMBER(20) NOT NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
tag_def_id NUMBER(20) NOT NULL,
name VARCHAR(255) NOT NULL,
type VARCHAR(50) NOT NULL,
primary key (id),
CONSTRAINT x_tag_attr_def_FK_tag_def_id FOREIGN KEY (tag_def_id) REFERENCES x_tag_def (id),
CONSTRAINT x_tag_attr_def_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_tag_attr_def_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_tag_attr(
id NUMBER(20) NOT NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
tag_id NUMBER(20) NOT NULL,
name VARCHAR(255) NOT NULL,
value VARCHAR(512) DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_tag_attr_FK_tag_id FOREIGN KEY (tag_id) REFERENCES x_tag (id),
CONSTRAINT x_tag_attr_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_tag_attr_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_tag_resource_map(
id NUMBER(20) NOT NULL,
guid VARCHAR(64) NOT NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
tag_id NUMBER(20) NOT NULL,
res_id NUMBER(20) NOT NULL,
primary key (id),
CONSTRAINT x_tag_res_map_UK_guid UNIQUE (guid),
CONSTRAINT x_tag_res_map_FK_tag_id FOREIGN KEY (tag_id) REFERENCES x_tag (id),
CONSTRAINT x_tag_res_map_FK_res_id FOREIGN KEY (res_id) REFERENCES x_service_resource (id),
CONSTRAINT x_tag_res_map_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_tag_res_map_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_service_resource_element_val(
id NUMBER(20) NOT NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
res_element_id NUMBER(20) NOT NULL,
value VARCHAR(1024) NOT NULL,
sort_order NUMBER(3) DEFAULT '0' NULL,
primary key (id),
CONSTRAINT x_srvc_res_el_val_FK_res_el_id FOREIGN KEY (res_element_id) REFERENCES x_service_resource_element (id),
CONSTRAINT x_srvc_res_el_val_FK_add_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_srvc_res_el_val_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);
INSERT INTO x_modules_master VALUES(X_MODULES_MASTER_SEQ.NEXTVAL,SYSDATE,SYSDATE,1,1,'Tag Based Policies','');
commit;

CREATE TABLE x_datamask_type_def (
id NUMBER(20) NOT NULL,
guid VARCHAR(64) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
def_id NUMBER(20) NOT NULL,
item_id NUMBER(20) NOT NULL,
name VARCHAR(1024) NOT NULL,
label VARCHAR(1024) NOT NULL,
description VARCHAR(1024) DEFAULT NULL NULL,
transformer VARCHAR(1024) DEFAULT NULL NULL,
datamask_options VARCHAR(1024) DEFAULT NULL NULL,
rb_key_label VARCHAR(1024) DEFAULT NULL NULL,
rb_key_description VARCHAR(1024) DEFAULT NULL NULL,
sort_order NUMBER(3) DEFAULT '0' NULL,
primary key (id),
CONSTRAINT x_dm_type_def_FK_def_id FOREIGN KEY (def_id) REFERENCES x_service_def(id),
CONSTRAINT x_dm_type_def_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user(id),
CONSTRAINT x_dm_type_def_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user(id)
);

CREATE TABLE x_policy_item_datamask(
id NUMBER(20) NOT NULL,
guid VARCHAR(64) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
policy_item_id NUMBER(20) NOT NULL,
type NUMBER(20) NOT NULL,
condition_expr VARCHAR(1024) DEFAULT NULL NULL,
value_expr VARCHAR(1024) DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_plc_item_dm_FK_plc_item_id FOREIGN KEY (policy_item_id) REFERENCES x_policy_item(id),
CONSTRAINT x_plc_item_dm_FK_type FOREIGN KEY (type) REFERENCES x_datamask_type_def(id),
CONSTRAINT x_plc_item_dm_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user(id),
CONSTRAINT x_plc_item_dm_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user(id)
);

CREATE TABLE x_policy_item_rowfilter(
id NUMBER(20) NOT NULL,
guid VARCHAR(64) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
policy_item_id NUMBER(20) NOT NULL,
filter_expr VARCHAR(1024) DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_plc_item_rf_FK_plc_item_id FOREIGN KEY (policy_item_id) REFERENCES x_policy_item (id),
CONSTRAINT x_plc_item_rf_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_plc_item_rf_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_service_version_info(
id NUMBER(20) NOT NULL,
service_id NUMBER(20) NOT NULL,
policy_version NUMBER(20) DEFAULT 0 NOT NULL,
policy_update_time DATE DEFAULT NULL NULL,
tag_version NUMBER(20) DEFAULT 0 NOT NULL,
tag_update_time DATE DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_svc_ver_info_FK_service_id FOREIGN KEY (service_id) REFERENCES x_service(id)
);

CREATE TABLE x_plugin_info(
id NUMBER(20) NOT NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
service_name VARCHAR(255) NOT NULL,
app_type VARCHAR(128) NOT NULL,
host_name VARCHAR(255) NOT NULL,
ip_address VARCHAR(64) NOT NULL,
info VARCHAR(1024) NOT NULL,
PRIMARY KEY (id),
CONSTRAINT x_plugin_info_UK UNIQUE (service_name, host_name, app_type)
);

CREATE TABLE x_policy_label (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
label_name VARCHAR(512) DEFAULT NULL,
primary key (id),
CONSTRAINT x_pl_UK_label_name UNIQUE (label_name),
CONSTRAINT x_pl_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_pl_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_policy_label_map (
id NUMBER(20) NOT NULL,
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
policy_id NUMBER(20) DEFAULT NULL,
policy_label_id NUMBER(20) DEFAULT NULL,
primary key (id),
CONSTRAINT x_plmap_uk_pid_plid UNIQUE (policy_id,policy_label_id),
CONSTRAINT x_plmap_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_plmap_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_plmap_FK_policy_id FOREIGN KEY (policy_id) REFERENCES x_policy (id),
CONSTRAINT x_plmap_FK_policy_label_id FOREIGN KEY (policy_label_id) REFERENCES x_policy_label (id)
);
commit;

CREATE TABLE x_ugsync_audit_info(
id NUMBER(20) NOT NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
event_time DATE DEFAULT NULL NULL,
user_name VARCHAR(255) NOT  NULL,
sync_source VARCHAR(128) NOT NULL,
no_of_new_users NUMBER(20) NOT NULL,
no_of_new_groups NUMBER(20) NOT NULL,
no_of_modified_users NUMBER(20) NOT NULL,
no_of_modified_groups NUMBER(20) NOT NULL,
sync_source_info VARCHAR(4000) NOT NULL,
session_id VARCHAR(255) DEFAULT NULL,
 PRIMARY KEY (id)
);
commit;

CREATE VIEW vx_trx_log AS select x_trx_log.id AS id,x_trx_log.create_time AS create_time,x_trx_log.update_time AS update_time,x_trx_log.added_by_id AS added_by_id,x_trx_log.upd_by_id AS upd_by_id,x_trx_log.class_type AS class_type,x_trx_log.object_id AS object_id,x_trx_log.parent_object_id AS parent_object_id,x_trx_log.parent_object_class_type AS parent_object_class_type,x_trx_log.attr_name AS attr_name,x_trx_log.parent_object_name AS parent_object_name,x_trx_log.object_name AS object_name,x_trx_log.prev_val AS prev_val,x_trx_log.new_val AS new_val,x_trx_log.trx_id AS trx_id,x_trx_log.action AS action,x_trx_log.sess_id AS sess_id,x_trx_log.req_id AS req_id,x_trx_log.sess_type AS sess_type from x_trx_log  where id in(select min(x_trx_log.id) from x_trx_log group by x_trx_log.trx_id);
commit;

CREATE INDEX xa_access_audit_added_by_id ON  xa_access_audit(added_by_id);
CREATE INDEX xa_access_audit_upd_by_id ON  xa_access_audit(upd_by_id);
CREATE INDEX xa_access_audit_cr_time ON  xa_access_audit(create_time);
CREATE INDEX xa_access_audit_up_time ON  xa_access_audit(update_time);
CREATE INDEX xa_access_audit_event_time ON  xa_access_audit(event_time);
CREATE INDEX x_asset_FK_added_by_id ON  x_asset(added_by_id);
CREATE INDEX x_asset_FK_upd_by_id ON  x_asset(upd_by_id);
CREATE INDEX x_asset_cr_time ON x_asset (create_time);
CREATE INDEX x_asset_up_time ON x_asset (update_time);
CREATE INDEX x_audit_map_FK_added_by_id ON x_audit_map (added_by_id);
CREATE INDEX x_audit_map_FK_upd_by_id ON x_audit_map (upd_by_id);
CREATE INDEX x_audit_map_FK_res_id ON  x_audit_map(res_id);
CREATE INDEX x_audit_map_FK_group_id ON x_audit_map (group_id);
CREATE INDEX x_audit_map_FK_user_id ON  x_audit_map(user_id);
CREATE INDEX x_audit_map_cr_time ON  x_audit_map(create_time);
CREATE INDEX x_audit_map_up_time ON x_audit_map (update_time);
CREATE INDEX x_auth_sess_FK_added_by_id ON x_auth_sess (added_by_id);
CREATE INDEX x_auth_sess_FK_upd_by_id ON x_auth_sess (upd_by_id);
CREATE INDEX x_auth_sess_FK_user_id ON x_auth_sess (user_id);
CREATE INDEX x_auth_sess_cr_time ON x_auth_sess (create_time);
CREATE INDEX x_auth_sess_up_time ON x_auth_sess (update_time);
CREATE INDEX x_cred_store_FK_added_by_id ON x_cred_store (added_by_id);
CREATE INDEX x_cred_store_FK_upd_by_id ON x_cred_store (upd_by_id);
CREATE INDEX x_cred_store_cr_time ON x_cred_store (create_time);
CREATE INDEX x_cred_store_up_time ON x_cred_store (update_time);
CREATE INDEX x_db_base_FK_added_by_id ON x_db_base (added_by_id);
CREATE INDEX x_db_base_FK_upd_by_id ON x_db_base (upd_by_id);
CREATE INDEX x_db_base_cr_time ON x_db_base (create_time);
CREATE INDEX x_db_base_up_time ON  x_db_base(update_time);
CREATE INDEX x_group_FK_added_by_id ON x_group (added_by_id);
CREATE INDEX x_group_FK_upd_by_id ON x_group (upd_by_id);
CREATE INDEX x_group_FK_cred_store_id ON x_group (cred_store_id);
CREATE INDEX x_group_cr_time ON x_group (create_time);
CREATE INDEX x_group_up_time ON x_group (update_time);
CREATE INDEX x_group_groups_FK_added_by_id ON x_group_groups (added_by_id);
CREATE INDEX x_group_groups_FK_upd_by_id ON  x_group_groups(upd_by_id);
CREATE INDEX x_group_groups_FK_p_group_id ON x_group_groups (p_group_id);
CREATE INDEX x_group_groups_FK_group_id ON  x_group_groups(group_id);
CREATE INDEX x_group_groups_cr_time ON x_group_groups (create_time);
CREATE INDEX x_group_groups_up_time ON x_group_groups (update_time);
CREATE INDEX x_group_users_FK_added_by_id ON x_group_users (added_by_id);
CREATE INDEX x_group_users_FK_upd_by_id ON  x_group_users(upd_by_id);
CREATE INDEX x_group_users_FK_p_group_id ON x_group_users (p_group_id);
CREATE INDEX x_group_users_FK_user_id ON x_group_users (user_id);
CREATE INDEX x_group_users_cr_time ON  x_group_users(create_time);
CREATE INDEX x_group_users_up_time ON  x_group_users(update_time);
CREATE INDEX x_perm_map_FK_added_by_id ON x_perm_map (added_by_id);
CREATE INDEX x_perm_map_FK_upd_by_id ON x_perm_map (upd_by_id);
CREATE INDEX x_perm_map_FK_res_id ON  x_perm_map(res_id);
CREATE INDEX x_perm_map_FK_group_id ON  x_perm_map(group_id);
CREATE INDEX x_perm_map_FK_user_id ON  x_perm_map(user_id);
CREATE INDEX x_perm_map_cr_time ON x_perm_map (create_time);
CREATE INDEX x_perm_map_up_time ON  x_perm_map(update_time);
CREATE INDEX x_policy_export_audit_FK_added ON x_policy_export_audit (added_by_id);
CREATE INDEX x_policy_export_audit_FK_upd ON x_policy_export_audit (upd_by_id);
CREATE INDEX x_policy_export_audit_cr_time ON x_policy_export_audit (create_time);
CREATE INDEX x_policy_export_audit_up_time ON  x_policy_export_audit(update_time);
CREATE INDEX x_portal_user_FK_added_by_id ON x_portal_user (added_by_id);
CREATE INDEX x_portal_user_FK_upd_by_id ON x_portal_user (upd_by_id);
CREATE INDEX x_portal_user_cr_time ON  x_portal_user(create_time);
CREATE INDEX x_portal_user_up_time ON x_portal_user (update_time);
CREATE INDEX x_portal_user_name ON  x_portal_user(first_name);
CREATE INDEX x_portal_user_role_FK_added ON  x_portal_user_role(added_by_id);
CREATE INDEX x_portal_user_role_FK_upd ON  x_portal_user_role(upd_by_id);
CREATE INDEX x_portal_user_role_FK_user_id ON  x_portal_user_role(user_id);
CREATE INDEX x_portal_user_role_cr_time ON  x_portal_user_role(create_time);
CREATE INDEX x_portal_user_role_up_time ON x_portal_user_role (update_time);
CREATE INDEX x_resource_FK_added_by_id ON  x_resource(added_by_id);
CREATE INDEX x_resource_FK_upd_by_id ON x_resource(upd_by_id);
CREATE INDEX x_resource_FK_asset_id ON x_resource (asset_id);
CREATE INDEX x_resource_FK_parent_id ON x_resource (parent_id);
CREATE INDEX x_resource_cr_time ON  x_resource(create_time);
CREATE INDEX x_resource_up_time ON x_resource (update_time);
CREATE INDEX x_trx_log_FK_added_by_id ON x_trx_log (added_by_id);
CREATE INDEX x_trx_log_FK_upd_by_id ON  x_trx_log(upd_by_id);
CREATE INDEX x_trx_log_cr_time ON x_trx_log (create_time);
CREATE INDEX x_trx_log_up_time ON x_trx_log (update_time);
CREATE INDEX x_user_FK_added_by_id ON x_user (added_by_id);
CREATE INDEX x_user_FK_upd_by_id ON x_user (upd_by_id);
CREATE INDEX x_user_FK_cred_store_id ON x_user (cred_store_id);
CREATE INDEX x_user_cr_time ON x_user (create_time);
CREATE INDEX x_user_up_time ON  x_user(update_time);
CREATE INDEX x_service_def_added_by_id ON x_service_def(added_by_id);
CREATE INDEX x_service_def_upd_by_id ON x_service_def(upd_by_id);
CREATE INDEX x_service_def_cr_time ON x_service_def(create_time);
CREATE INDEX x_service_def_up_time ON x_service_def(update_time);
CREATE INDEX x_service_added_by_id ON x_service(added_by_id);
CREATE INDEX x_service_upd_by_id ON x_service(upd_by_id);
CREATE INDEX x_service_cr_time ON x_service(create_time);
CREATE INDEX x_service_up_time ON x_service(update_time);
CREATE INDEX x_service_type ON x_service(type);
CREATE INDEX x_policy_added_by_id ON x_policy(added_by_id);
CREATE INDEX x_policy_upd_by_id ON x_policy(upd_by_id);
CREATE INDEX x_policy_cr_time ON x_policy(create_time);
CREATE INDEX x_policy_up_time ON x_policy(update_time);
CREATE INDEX x_policy_service ON x_policy(service);
CREATE INDEX x_resource_def_parent ON x_resource_def(parent);
CREATE INDEX x_policy_resource_signature ON x_policy(resource_signature);
CREATE INDEX x_usr_module_perm_idx_moduleid ON x_user_module_perm(module_id);
CREATE INDEX x_usr_module_perm_idx_userid ON x_user_module_perm(user_id);
CREATE INDEX x_grp_module_perm_idx_groupid ON x_group_module_perm(group_id);
CREATE INDEX x_grp_module_perm_idx_moduleid ON x_group_module_perm(module_id);
CREATE INDEX x_tag_def_IDX_added_by_id ON x_tag_def(added_by_id);
CREATE INDEX x_tag_def_IDX_upd_by_id ON x_tag_def(upd_by_id);
CREATE INDEX x_tag_IDX_type ON x_tag(type);
CREATE INDEX x_tag_IDX_added_by_id ON x_tag(added_by_id);
CREATE INDEX x_tag_IDX_upd_by_id ON x_tag(upd_by_id);
CREATE INDEX x_service_res_IDX_added_by_id ON x_service_resource(added_by_id);
CREATE INDEX x_service_res_IDX_upd_by_id ON x_service_resource(upd_by_id);
CREATE INDEX x_srvc_res_el_IDX_added_by_id ON x_service_resource_element(added_by_id);
CREATE INDEX x_srvc_res_el_IDX_upd_by_id ON x_service_resource_element(upd_by_id);
CREATE INDEX x_tag_attr_def_IDX_tag_def_id ON x_tag_attr_def(tag_def_id);
CREATE INDEX x_tag_attr_def_IDX_added_by_id ON x_tag_attr_def(added_by_id);
CREATE INDEX x_tag_attr_def_IDX_upd_by_id ON x_tag_attr_def(upd_by_id);
CREATE INDEX x_tag_attr_IDX_tag_id ON x_tag_attr(tag_id);
CREATE INDEX x_tag_attr_IDX_added_by_id ON x_tag_attr(added_by_id);
CREATE INDEX x_tag_attr_IDX_upd_by_id ON x_tag_attr(upd_by_id);
CREATE INDEX x_tag_res_map_IDX_tag_id ON x_tag_resource_map(tag_id);
CREATE INDEX x_tag_res_map_IDX_res_id ON x_tag_resource_map(res_id);
CREATE INDEX x_tag_res_map_IDX_added_by_id ON x_tag_resource_map(added_by_id);
CREATE INDEX x_tag_res_map_IDX_upd_by_id ON x_tag_resource_map(upd_by_id);
CREATE INDEX x_srvc_res_el_val_IDX_resel_id ON x_service_resource_element_val(res_element_id);
CREATE INDEX x_srvc_res_el_val_IDX_addby_id ON x_service_resource_element_val(added_by_id);
CREATE INDEX x_srvc_res_el_val_IDX_updby_id ON x_service_resource_element_val(upd_by_id);
CREATE INDEX x_service_conf_def_IDX_defid ON x_service_config_def(def_id);
CREATE INDEX x_resource_def_IDX_def_id ON x_resource_def(def_id);
CREATE INDEX x_access_type_def_IDX_def_id ON x_access_type_def(def_id);
CREATE INDEX x_atd_grants_IDX_atdid ON x_access_type_def_grants(atd_id);
CREATE INDEX x_cont_enr_def_IDX_defid ON x_context_enricher_def(def_id);
CREATE INDEX x_enum_def_IDX_def_id ON x_enum_def(def_id);
CREATE INDEX x_enum_element_def_IDX_defid ON x_enum_element_def(enum_def_id);
CREATE INDEX x_service_conf_map_IDX_service ON x_service_config_map(service);
CREATE INDEX x_policy_res_IDX_policy_id ON x_policy_resource(policy_id);
CREATE INDEX x_policy_res_IDX_res_def_id ON x_policy_resource(res_def_id);
CREATE INDEX x_policy_res_map_IDX_res_id ON x_policy_resource_map(resource_id);
CREATE INDEX x_policy_item_IDX_policy_id ON x_policy_item(policy_id);
CREATE INDEX x_plc_item_access_IDX_pi_id ON x_policy_item_access(policy_item_id);
CREATE INDEX x_plc_item_access_IDX_type ON x_policy_item_access(type);
CREATE INDEX x_plc_item_cond_IDX_pi_id ON x_policy_item_condition(policy_item_id);
CREATE INDEX x_plc_item_cond_IDX_type ON x_policy_item_condition(type);
CREATE INDEX x_plc_itm_usr_perm_IDX_pi_id ON x_policy_item_user_perm(policy_item_id);
CREATE INDEX x_plc_itm_usr_perm_IDX_user_id ON x_policy_item_user_perm(user_id);
CREATE INDEX x_plc_itm_grp_perm_IDX_pi_id ON x_policy_item_group_perm(policy_item_id);
CREATE INDEX x_plc_itm_grp_perm_IDX_grp_id ON x_policy_item_group_perm(group_id);
CREATE INDEX x_srvc_res_IDX_service_id ON x_service_resource(service_id);
CREATE INDEX x_srvc_res_el_IDX_res_def_id ON x_service_resource_element(res_id);
CREATE INDEX x_srvc_res_el_IDX_res_id ON x_service_resource_element(res_def_id);
CREATE INDEX x_dm_type_def_IDX_def_id ON x_datamask_type_def(def_id);
CREATE INDEX x_plc_item_dm_IDX_plc_item_id ON x_policy_item_datamask(policy_item_id);
CREATE INDEX x_plc_item_rf_IDX_plc_item_id ON x_policy_item_rowfilter(policy_item_id);
CREATE INDEX x_svc_ver_info_IDX_service_id ON x_service_version_info(service_id);
CREATE INDEX x_plugin_info_IDX_service_name ON x_plugin_info(service_name);
CREATE INDEX x_plugin_info_IDX_host_name ON x_plugin_info(host_name);
CREATE INDEX x_ugsync_audit_info_etime ON x_ugsync_audit_info(event_time);
CREATE INDEX x_ugsync_audit_info_sync_src ON x_ugsync_audit_info(sync_source);
CREATE INDEX x_ugsync_audit_info_uname ON x_ugsync_audit_info(user_name);
commit;

insert into x_portal_user (id,CREATE_TIME, UPDATE_TIME,FIRST_NAME, LAST_NAME, PUB_SCR_NAME, LOGIN_ID, PASSWORD, EMAIL, STATUS) values (X_PORTAL_USER_SEQ.NEXTVAL, SYSDATE, SYSDATE, 'Admin', '', 'Admin', 'admin', 'ceb4f32325eda6142bd65215f4c0f371', '', 1);
insert into x_portal_user_role (id, CREATE_TIME, UPDATE_TIME, USER_ID, USER_ROLE, STATUS) values (X_PORTAL_USER_ROLE_SEQ.NEXTVAL, SYSDATE, SYSDATE, 1, 'ROLE_SYS_ADMIN', 1);
insert into x_user (id,CREATE_TIME, UPDATE_TIME,user_name, status,descr) values (X_USER_SEQ.NEXTVAL, SYSDATE, SYSDATE,'admin', 0,'Administrator');
INSERT INTO x_group (ID,ADDED_BY_ID, CREATE_TIME, DESCR, GROUP_TYPE, GROUP_NAME, STATUS, UPDATE_TIME, UPD_BY_ID) VALUES (X_GROUP_SEQ.nextval,1, sys_extract_utc(systimestamp), 'public group', 0, 'public', 0, sys_extract_utc(systimestamp), 1);


INSERT INTO x_portal_user(ID,CREATE_TIME,UPDATE_TIME,FIRST_NAME,LAST_NAME,PUB_SCR_NAME,LOGIN_ID,PASSWORD,EMAIL,STATUS,USER_SRC) VALUES (X_PORTAL_USER_SEQ.nextval,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),'rangerusersync',NULL,'rangerusersync','rangerusersync','70b8374d3dfe0325aaa5002a688c7e3b','rangerusersync',1,0);
INSERT INTO x_portal_user_role(id,create_time,update_time,user_id,user_role,status) VALUES (X_PORTAL_USER_ROLE_SEQ.nextval,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),2,'ROLE_SYS_ADMIN',1);
INSERT INTO x_user(id,create_time,update_time,user_name,descr,status) values (X_USER_SEQ.nextval,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),'rangerusersync','rangerusersync',0);

INSERT INTO x_portal_user(ID,CREATE_TIME,UPDATE_TIME,FIRST_NAME,LAST_NAME,PUB_SCR_NAME,LOGIN_ID,PASSWORD,EMAIL,STATUS,USER_SRC) VALUES(X_PORTAL_USER_SEQ.nextval,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),'keyadmin',NULL,'keyadmin','keyadmin','a05f34d2dce2b4688fa82e82a89ba958','keyadmin',1,0);
INSERT INTO x_portal_user_role(id,create_time,update_time,user_id,user_role,status) VALUES(X_PORTAL_USER_ROLE_SEQ.nextval,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),3,'ROLE_KEY_ADMIN',1);
INSERT INTO x_user(id,create_time,update_time,user_name,descr,status) values(X_USER_SEQ.nextval,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),'keyadmin','keyadmin',0);

INSERT INTO x_portal_user(ID,CREATE_TIME,UPDATE_TIME,FIRST_NAME,LAST_NAME,PUB_SCR_NAME,LOGIN_ID,PASSWORD,EMAIL,STATUS,USER_SRC) VALUES(X_PORTAL_USER_SEQ.nextval,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),'rangertagsync',NULL,'rangertagsync','rangertagsync','f5820e1229418dcf2575908f2c493da5','rangertagsync',1,0);
INSERT INTO x_portal_user_role(id,create_time,update_time,user_id,user_role,status) VALUES(X_PORTAL_USER_ROLE_SEQ.nextval,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),4,'ROLE_SYS_ADMIN',1);
INSERT INTO x_user(id,create_time,update_time,user_name,descr,status) values (X_USER_SEQ.nextval,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),'rangertagsync','rangertagsync',0);

INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, 'CORE_DB_SCHEMA',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '001',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '002',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '003',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '006',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '009',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '010',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '012',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '013',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '014',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '016',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '018',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '019',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '020',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '021',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '022',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '023',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '024',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '025',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '026',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '027',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '028',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '029',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '030',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '031',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '032',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, '033',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval, 'DB_PATCHES',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,1,3,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,1,1,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,1,4,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,1,2,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,1,6,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,2,3,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,2,1,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,2,4,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,2,2,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,2,6,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,3,5,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,3,3,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,3,1,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,4,3,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,4,1,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,4,4,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,4,2,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,4,6,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,3,2,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.nextval,3,4,sys_extract_utc(systimestamp),sys_extract_utc(systimestamp),1,1,1);
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10001',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10002',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10003',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10004',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10005',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10006',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10007',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10008',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10009',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10010',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10011',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10012',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10013',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10014',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'J10015',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
INSERT INTO x_db_version_h (id,version,inst_at,inst_by,updated_at,updated_by,active) VALUES (X_DB_VERSION_H_SEQ.nextval,'JAVA_PATCHES',sys_extract_utc(systimestamp),'Ranger 1.0.0',sys_extract_utc(systimestamp),'localhost','Y');
commit;
