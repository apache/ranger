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

DELIMITER $$
DROP PROCEDURE IF EXISTS getXportalUIdByLoginId$$
CREATE PROCEDURE `getXportalUIdByLoginId`(IN input_val VARCHAR(100), OUT myid BIGINT)
BEGIN
SET myid = 0;
SELECT x_portal_user.id INTO myid FROM x_portal_user WHERE x_portal_user.login_id = input_val;
END $$

DELIMITER ;

DROP PROCEDURE IF EXISTS patch_audit_partition_plan_global_state;

DELIMITER ;;
CREATE PROCEDURE patch_audit_partition_plan_global_state()
BEGIN
    DECLARE adminID BIGINT;
    DECLARE auditServerID BIGINT;
    DECLARE planJson TEXT DEFAULT '{"version":1,"topic":"ranger_audits","topicPartitionCount":9,"plugins":{},"buffer":{"partitions":[1,2,3,4,5,6,7,8,9]}}';

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'x_ranger_global_state' AND column_name = 'state_name'
    ) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = 'x_ranger_global_state'
              AND column_name = 'app_data' AND data_type IN ('varchar', 'text')
        ) THEN
            ALTER TABLE x_ranger_global_state MODIFY app_data LONGTEXT DEFAULT NULL;
        END IF;

        IF NOT EXISTS (SELECT 1 FROM x_portal_user WHERE login_id = 'rangerauditserver') THEN
            INSERT INTO x_portal_user(create_time, update_time, added_by_id, upd_by_id, first_name, last_name, pub_scr_name, login_id, password, email, status, user_src, notes)
            VALUES (UTC_TIMESTAMP(), UTC_TIMESTAMP(), NULL, NULL, 'rangerauditserver', '', 'rangerauditserver', 'rangerauditserver', '', 'rangerauditserver', 0, 0, NULL);
        END IF;

        UPDATE x_portal_user SET status = 0, password = '' WHERE login_id = 'rangerauditserver';

        CALL getXportalUIdByLoginId('admin', adminID);
        CALL getXportalUIdByLoginId('rangerauditserver', auditServerID);

        IF auditServerID IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM x_portal_user_role WHERE user_id = auditServerID AND user_role = 'ROLE_ADMIN_AUDITOR'
        ) THEN
            INSERT INTO x_portal_user_role(create_time, update_time, added_by_id, upd_by_id, user_id, user_role, status)
            VALUES (UTC_TIMESTAMP(), UTC_TIMESTAMP(), NULL, NULL, auditServerID, 'ROLE_ADMIN_AUDITOR', 1);
        END IF;

        IF auditServerID IS NOT NULL AND NOT EXISTS (SELECT 1 FROM x_user WHERE user_name = 'rangerauditserver') THEN
            INSERT INTO x_user(create_time, update_time, added_by_id, upd_by_id, user_name, descr, status)
            VALUES (UTC_TIMESTAMP(), UTC_TIMESTAMP(), NULL, NULL, 'rangerauditserver', 'Ranger audit server machine user', 0);
        END IF;

        IF NOT EXISTS (SELECT 1 FROM x_ranger_global_state WHERE state_name = 'RangerAuditPartitionPlan') THEN
            INSERT INTO x_ranger_global_state (create_time, update_time, added_by_id, upd_by_id, version, state_name, app_data)
            VALUES (UTC_TIMESTAMP(), UTC_TIMESTAMP(), adminID, adminID, 1, 'RangerAuditPartitionPlan', planJson);
        END IF;
    END IF;
END;;

DELIMITER ;
CALL patch_audit_partition_plan_global_state();
DROP PROCEDURE IF EXISTS patch_audit_partition_plan_global_state;
