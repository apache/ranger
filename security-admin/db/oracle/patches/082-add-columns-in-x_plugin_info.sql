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
-- sync_source_info CLOB NOT NULL,

DECLARE
    v_count NUMBER := 0;
    v_table_exists NUMBER := 0;
BEGIN
    SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'X_PLUGIN_INFO';
    IF v_table_exists > 0 THEN

        SELECT COUNT(*) INTO v_count FROM user_tab_cols
        WHERE table_name = 'X_PLUGIN_INFO' AND column_name = 'POLICY_DOWNLOAD_TIME';
        IF v_count = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE x_plugin_info ADD policy_download_time NUMBER(20) DEFAULT NULL';
        END IF;

        SELECT COUNT(*) INTO v_count FROM user_tab_cols
        WHERE table_name = 'X_PLUGIN_INFO' AND column_name = 'POLICY_ACTIVATION_TIME';
        IF v_count = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE x_plugin_info ADD policy_activation_time NUMBER(20) DEFAULT NULL';
        END IF;

        SELECT COUNT(*) INTO v_count FROM user_tab_cols
        WHERE table_name = 'X_PLUGIN_INFO' AND column_name = 'TAG_DOWNLOAD_TIME';
        IF v_count = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE x_plugin_info ADD tag_download_time NUMBER(20) DEFAULT NULL';
        END IF;

        SELECT COUNT(*) INTO v_count FROM user_tab_cols
        WHERE table_name = 'X_PLUGIN_INFO' AND column_name = 'TAG_ACTIVATION_TIME';
        IF v_count = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE x_plugin_info ADD tag_activation_time NUMBER(20) DEFAULT NULL';
        END IF;

        SELECT COUNT(*) INTO v_count FROM user_tab_cols
        WHERE table_name = 'X_PLUGIN_INFO' AND column_name = 'GDS_DOWNLOAD_TIME';
        IF v_count = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE x_plugin_info ADD gds_download_time NUMBER(20) DEFAULT NULL';
        END IF;

        SELECT COUNT(*) INTO v_count FROM user_tab_cols
        WHERE table_name = 'X_PLUGIN_INFO' AND column_name = 'GDS_ACTIVATION_TIME';
        IF v_count = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE x_plugin_info ADD gds_activation_time NUMBER(20) DEFAULT NULL';
        END IF;

        SELECT COUNT(*) INTO v_count FROM user_tab_cols
        WHERE table_name = 'X_PLUGIN_INFO' AND column_name = 'ROLE_DOWNLOAD_TIME';
        IF v_count = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE x_plugin_info ADD role_download_time NUMBER(20) DEFAULT NULL';
        END IF;

        SELECT COUNT(*) INTO v_count FROM user_tab_cols
        WHERE table_name = 'X_PLUGIN_INFO' AND column_name = 'ROLE_ACTIVATION_TIME';
        IF v_count = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE x_plugin_info ADD role_activation_time NUMBER(20) DEFAULT NULL';
        END IF;

        SELECT COUNT(*) INTO v_count FROM user_tab_cols
        WHERE table_name = 'X_PLUGIN_INFO' AND column_name = 'USERSTORE_DOWNLOAD_TIME';
        IF v_count = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE x_plugin_info ADD userstore_download_time NUMBER(20) DEFAULT NULL';
        END IF;

        SELECT COUNT(*) INTO v_count FROM user_tab_cols
        WHERE table_name = 'X_PLUGIN_INFO' AND column_name = 'USERSTORE_ACTIVATION_TIME';
        IF v_count = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE x_plugin_info ADD userstore_activation_time NUMBER(20) DEFAULT NULL';
        END IF;

        SELECT COUNT(*) INTO v_count FROM user_tab_cols
        WHERE table_name = 'X_PLUGIN_INFO' AND column_name = 'CLUSTER_NAME';
        IF v_count = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE x_plugin_info ADD cluster_name VARCHAR2(255) DEFAULT NULL';
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_plugin_info_IDX_policy_download_time') AND TABLE_NAME= upper('x_plugin_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_plugin_info_IDX_policy_download_time ON x_plugin_info(policy_download_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_plugin_info_IDX_policy_activation_time') AND TABLE_NAME= upper('x_plugin_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_plugin_info_IDX_policy_activation_time ON x_plugin_info(policy_activation_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_plugin_info_IDX_tag_download_time') AND TABLE_NAME= upper('x_plugin_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_plugin_info_IDX_tag_download_time ON x_plugin_info(tag_download_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_plugin_info_IDX_tag_activation_time') AND TABLE_NAME= upper('x_plugin_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_plugin_info_IDX_tag_activation_time ON x_plugin_info(tag_activation_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_plugin_info_IDX_gds_download_time') AND TABLE_NAME= upper('x_plugin_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_plugin_info_IDX_gds_download_time ON x_plugin_info(gds_download_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_plugin_info_IDX_gds_activation_time') AND TABLE_NAME= upper('x_plugin_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_plugin_info_IDX_gds_activation_time ON x_plugin_info(gds_activation_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_plugin_info_IDX_role_download_time') AND TABLE_NAME= upper('x_plugin_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_plugin_info_IDX_role_download_time ON x_plugin_info(role_download_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_plugin_info_IDX_role_activation_time') AND TABLE_NAME= upper('x_plugin_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_plugin_info_IDX_role_activation_time ON x_plugin_info(role_activation_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_plugin_info_IDX_userstore_download_time') AND TABLE_NAME= upper('x_plugin_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_plugin_info_IDX_userstore_download_time ON x_plugin_info(userstore_download_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_plugin_info_IDX_userstore_activation_time') AND TABLE_NAME= upper('x_plugin_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_plugin_info_IDX_userstore_activation_time ON x_plugin_info(userstore_activation_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_plugin_info_IDX_cluster_name') AND TABLE_NAME= upper('x_plugin_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_plugin_info_IDX_cluster_name ON x_plugin_info(cluster_name)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_service_version_info_IDX_policy_update_time') AND TABLE_NAME= upper('x_service_version_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_service_version_info_IDX_policy_update_time ON x_service_version_info(policy_update_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_service_version_info_IDX_tag_update_time') AND TABLE_NAME= upper('x_service_version_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_service_version_info_IDX_tag_update_time ON x_service_version_info(tag_update_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_service_version_info_IDX_role_update_time') AND TABLE_NAME= upper('x_service_version_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_service_version_info_IDX_role_update_time ON x_service_version_info(role_update_time)';
            commit;
        END IF;

        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_service_version_info_IDX_gds_update_time') AND TABLE_NAME= upper('x_service_version_info');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_service_version_info_IDX_gds_update_time ON x_service_version_info(gds_update_time)';
            commit;
        END IF;
    END IF;

COMMIT;

call spdropview('vx_plugin_info');
COMMIT;

CREATE VIEW vx_plugin_info AS
    SELECT
        xpi.id, xpi.create_time, xpi.update_time, xpi.service_name,
        xsd.name AS service_type, xpi.app_type, xpi.host_name, xpi.ip_address, xpi.info, x_ts.is_enabled AS is_tag_service_enable,
        xpi.policy_download_time, xpi.policy_activation_time, xpi.tag_download_time, xpi.tag_activation_time,
        xpi.gds_download_time, xpi.gds_activation_time, xpi.role_download_time, xpi.role_activation_time,
        xpi.userstore_download_time, xpi.userstore_activation_time, xpi.cluster_name,
        xsvi.policy_update_time AS last_policy_update_time, xsvi.policy_version AS latest_policy_version,
        xsvi.tag_update_time AS last_tag_update_time, xsvi.tag_version AS latest_tag_version,
        xsvi.gds_update_time AS last_gds_update_time, xsvi.gds_version AS latest_gds_version,
        xsvi.role_update_time AS last_role_update_time, xsvi.role_version AS latest_role_version
    FROM
        x_plugin_info xpi
        LEFT OUTER JOIN x_service xs ON xs.name = xpi.service_name
        LEFT OUTER JOIN x_service_version_info xsvi ON xsvi.service_id = xs.id
        LEFT OUTER JOIN x_service_def xsd ON xsd.id = xs.type
        LEFT OUTER JOIN x_service x_ts ON x_ts.id = xs.tag_service;
commit;

END;/
