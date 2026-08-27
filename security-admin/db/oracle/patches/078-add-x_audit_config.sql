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

DECLARE
    v_table_count NUMBER := 0;
    v_seq_count NUMBER := 0;
    v_portal_user_count NUMBER := 0;
    v_user_count NUMBER := 0;
    v_role_count NUMBER := 0;
    v_xuser_count NUMBER := 0;
    v_cfg_count NUMBER := 0;
    v_audit_user_id NUMBER := 0;
    sql_stmt VARCHAR2(4000);
BEGIN
    SELECT count(*) INTO v_table_count FROM user_tables WHERE table_name = 'X_AUDIT_CONFIG';
    IF (v_table_count = 0) THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE x_audit_config (
                id NUMBER(20) NOT NULL,
                create_time DATE DEFAULT NULL NULL,
                update_time DATE DEFAULT NULL NULL,
                cfg_name varchar(255) NOT NULL,
                cfg_value CLOB DEFAULT NULL NULL,
                version NUMBER(20) DEFAULT NULL NULL,
                PRIMARY KEY (id),
                CONSTRAINT x_audit_config_UK_cfg_name UNIQUE (cfg_name)
            )';
        SELECT count(*) INTO v_seq_count FROM user_sequences WHERE sequence_name = 'X_AUDIT_CONFIG_SEQ';
        IF (v_seq_count = 0) THEN
            EXECUTE IMMEDIATE 'CREATE SEQUENCE X_AUDIT_CONFIG_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
        END IF;
    END IF;

    SELECT count(*) INTO v_portal_user_count FROM user_tables WHERE table_name = 'X_PORTAL_USER';
    IF (v_portal_user_count > 0) THEN
        SELECT count(*) INTO v_user_count FROM x_portal_user WHERE login_id = 'rangerauditserver';
        IF (v_user_count = 0) THEN
            INSERT INTO x_portal_user (id, create_time, update_time, first_name, last_name, pub_scr_name, login_id, password, email, status, user_src)
            VALUES (X_PORTAL_USER_SEQ.nextval, sys_extract_utc(systimestamp), sys_extract_utc(systimestamp), 'rangerauditserver', NULL, 'rangerauditserver', 'rangerauditserver', ' ', 'rangerauditserver', 0, 0);
        END IF;

        UPDATE x_portal_user SET status = 0, password = ' ' WHERE login_id = 'rangerauditserver';

        v_audit_user_id := getXportalUIdByLoginId('rangerauditserver');

        IF (v_audit_user_id IS NOT NULL AND v_audit_user_id > 0) THEN
            SELECT count(*) INTO v_role_count FROM x_portal_user_role WHERE user_id = v_audit_user_id AND user_role = 'ROLE_ADMIN_AUDITOR';
            IF (v_role_count = 0) THEN
                INSERT INTO x_portal_user_role (id, create_time, update_time, user_id, user_role, status)
                VALUES (X_PORTAL_USER_ROLE_SEQ.nextval, sys_extract_utc(systimestamp), sys_extract_utc(systimestamp), v_audit_user_id, 'ROLE_ADMIN_AUDITOR', 1);
            END IF;

            SELECT count(*) INTO v_xuser_count FROM x_user WHERE user_name = 'rangerauditserver';
            IF (v_xuser_count = 0) THEN
                INSERT INTO x_user (id, create_time, update_time, user_name, status, descr)
                VALUES (X_USER_SEQ.nextval, sys_extract_utc(systimestamp), sys_extract_utc(systimestamp), 'rangerauditserver', 0, 'Ranger audit server machine user');
            END IF;
        END IF;
    END IF;

    EXECUTE IMMEDIATE 'SELECT count(*) FROM x_audit_config WHERE cfg_name = :1' INTO v_cfg_count USING 'ingestor.url';
    IF (v_cfg_count = 0) THEN
        sql_stmt := 'INSERT INTO x_audit_config (id, create_time, update_time, cfg_name, cfg_value, version) VALUES (X_AUDIT_CONFIG_SEQ.nextval, sys_extract_utc(systimestamp), sys_extract_utc(systimestamp), :1, :2, 1)';
        EXECUTE IMMEDIATE sql_stmt USING 'ingestor.url', 'https://ranger-audit-ingestor:8765';
    END IF;

    EXECUTE IMMEDIATE 'SELECT count(*) FROM x_audit_config WHERE cfg_name = :1' INTO v_cfg_count USING 'service.hive.allowed.users';
    IF (v_cfg_count = 0) THEN
        sql_stmt := 'INSERT INTO x_audit_config (id, create_time, update_time, cfg_name, cfg_value, version) VALUES (X_AUDIT_CONFIG_SEQ.nextval, sys_extract_utc(systimestamp), sys_extract_utc(systimestamp), :1, :2, 1)';
        EXECUTE IMMEDIATE sql_stmt USING 'service.hive.allowed.users', 'hive';
    END IF;

    EXECUTE IMMEDIATE 'SELECT count(*) FROM x_audit_config WHERE cfg_name = :1' INTO v_cfg_count USING 'audit.partition.plan';
    IF (v_cfg_count = 0) THEN
        sql_stmt := 'INSERT INTO x_audit_config (id, create_time, update_time, cfg_name, cfg_value, version) VALUES (X_AUDIT_CONFIG_SEQ.nextval, sys_extract_utc(systimestamp), sys_extract_utc(systimestamp), :1, :2, 1)';
        EXECUTE IMMEDIATE sql_stmt USING 'audit.partition.plan', '{"topic":"ranger_audits","plugins":{},"buffer":{"partitions":[1,2,3,4,5,6,7,8,9]}}';
    END IF;

    COMMIT;
END;/
