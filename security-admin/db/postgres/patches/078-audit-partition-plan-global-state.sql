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

select 'delimiter start';
CREATE OR REPLACE FUNCTION patch_audit_partition_plan_global_state()
RETURNS void AS $$
DECLARE
    v_column_is_varchar integer := 0;
    v_admin_id bigint;
    v_audit_user_id bigint;
    v_plan_json text := '{"version":1,"topic":"ranger_audits","topicPartitionCount":9,"plugins":{},"buffer":{"partitions":[1,2,3,4,5,6,7,8,9]}}';
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'x_ranger_global_state') THEN
        SELECT count(*) INTO v_column_is_varchar
        FROM pg_attribute
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'x_ranger_global_state')
          AND attname = 'app_data'
          AND atttypid = (SELECT oid FROM pg_type WHERE typname = 'varchar');

        IF v_column_is_varchar > 0 THEN
            ALTER TABLE x_ranger_global_state ALTER COLUMN app_data TYPE TEXT;
        END IF;

        SELECT getXportalUIdByLoginId('admin') INTO v_admin_id;

        IF NOT EXISTS (SELECT 1 FROM x_portal_user WHERE login_id = 'rangerauditserver') THEN
            INSERT INTO x_portal_user(create_time, update_time, first_name, last_name, pub_scr_name, login_id, password, email, status)
            VALUES (current_timestamp, current_timestamp, 'rangerauditserver', '', 'rangerauditserver', 'rangerauditserver', '9c8f4e2b1a0d6e3f7b5c4a8291d0e6f3', 'rangerauditserver', 1);
        END IF;

        SELECT getXportalUIdByLoginId('rangerauditserver') INTO v_audit_user_id;

        IF v_audit_user_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM x_portal_user_role WHERE user_id = v_audit_user_id AND user_role = 'ROLE_ADMIN_AUDITOR'
        ) THEN
            INSERT INTO x_portal_user_role(create_time, update_time, user_id, user_role, status)
            VALUES (current_timestamp, current_timestamp, v_audit_user_id, 'ROLE_ADMIN_AUDITOR', 1);
        END IF;

        IF v_audit_user_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM x_user WHERE user_name = 'rangerauditserver') THEN
            INSERT INTO x_user(create_time, update_time, user_name, status, descr)
            VALUES (current_timestamp, current_timestamp, 'rangerauditserver', 0, 'Ranger audit server machine user');
        END IF;

        IF NOT EXISTS (SELECT 1 FROM x_ranger_global_state WHERE state_name = 'RangerAuditPartitionPlan') THEN
            INSERT INTO x_ranger_global_state (create_time, update_time, added_by_id, upd_by_id, version, state_name, app_data)
            VALUES (current_timestamp, current_timestamp, v_admin_id, v_admin_id, 1, 'RangerAuditPartitionPlan', v_plan_json);
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;
select 'delimiter end';

select patch_audit_partition_plan_global_state();
select 'delimiter end';
