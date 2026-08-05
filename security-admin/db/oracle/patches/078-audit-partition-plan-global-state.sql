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

CREATE OR REPLACE FUNCTION getXportalUIdByLoginId(input_val IN VARCHAR2)
RETURN NUMBER IS
BEGIN
DECLARE
    myid Number := 0;
BEGIN
    SELECT x_portal_user.id INTO myid FROM x_portal_user WHERE x_portal_user.login_id = input_val;
    RETURN myid;
END;
END;
/

DECLARE
    t_count number := 0;
    v_admin_id number;
    v_audit_user_id number;
    v_plan_count number := 0;
    v_user_count number := 0;
    v_role_count number := 0;
    v_xuser_count number := 0;
    v_plan_json CLOB := '{"version":1,"topic":"ranger_audits","topicPartitionCount":9,"plugins":{},"buffer":{"partitions":[1,2,3,4,5,6,7,8,9]}}';
    sql_stmt VARCHAR2(4000);
BEGIN
    SELECT count(*) INTO t_count FROM user_tables WHERE table_name = 'X_RANGER_GLOBAL_STATE';
    IF (t_count > 0) THEN
        BEGIN
            EXECUTE IMMEDIATE 'ALTER TABLE x_ranger_global_state MODIFY (app_data CLOB)';
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;

        v_admin_id := getXportalUIdByLoginId('admin');

        SELECT count(*) INTO v_user_count FROM x_portal_user WHERE login_id = 'rangerauditserver';
        IF (v_user_count = 0) THEN
            sql_stmt := 'INSERT INTO x_portal_user (id, create_time, update_time, first_name, last_name, pub_scr_name, login_id, password, email, status, user_src) VALUES (X_PORTAL_USER_SEQ.nextval, sys_extract_utc(systimestamp), sys_extract_utc(systimestamp), :1, NULL, :2, :3, :4, :5, 1, 0)';
            EXECUTE IMMEDIATE sql_stmt USING 'rangerauditserver', 'rangerauditserver', 'rangerauditserver', '9c8f4e2b1a0d6e3f7b5c4a8291d0e6f3', 'rangerauditserver';
            COMMIT;
        END IF;

        v_audit_user_id := getXportalUIdByLoginId('rangerauditserver');

        IF (v_audit_user_id IS NOT NULL AND v_audit_user_id > 0) THEN
            SELECT count(*) INTO v_role_count FROM x_portal_user_role WHERE user_id = v_audit_user_id AND user_role = 'ROLE_ADMIN_AUDITOR';
            IF (v_role_count = 0) THEN
                sql_stmt := 'INSERT INTO x_portal_user_role (id, create_time, update_time, user_id, user_role, status) VALUES (X_PORTAL_USER_ROLE_SEQ.nextval, sys_extract_utc(systimestamp), sys_extract_utc(systimestamp), :1, :2, 1)';
                EXECUTE IMMEDIATE sql_stmt USING v_audit_user_id, 'ROLE_ADMIN_AUDITOR';
                COMMIT;
            END IF;

            SELECT count(*) INTO v_xuser_count FROM x_user WHERE user_name = 'rangerauditserver';
            IF (v_xuser_count = 0) THEN
                sql_stmt := 'INSERT INTO x_user (id, create_time, update_time, user_name, status, descr) VALUES (X_USER_SEQ.nextval, sys_extract_utc(systimestamp), sys_extract_utc(systimestamp), :1, 0, :2)';
                EXECUTE IMMEDIATE sql_stmt USING 'rangerauditserver', 'Ranger audit server machine user';
                COMMIT;
            END IF;
        END IF;

        SELECT count(*) INTO v_plan_count FROM x_ranger_global_state WHERE state_name = 'RangerAuditPartitionPlan';
        IF (v_plan_count = 0) THEN
            sql_stmt := 'INSERT INTO x_ranger_global_state (id, create_time, update_time, added_by_id, upd_by_id, version, state_name, app_data) VALUES (X_RANGER_GLOBAL_STATE_SEQ.nextval, sys_extract_utc(systimestamp), sys_extract_utc(systimestamp), :1, :2, 1, :3, :4)';
            EXECUTE IMMEDIATE sql_stmt USING v_admin_id, v_admin_id, 'RangerAuditPartitionPlan', v_plan_json;
            COMMIT;
        END IF;
    END IF;
END;
/
