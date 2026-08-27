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

DROP PROCEDURE IF EXISTS patch_audit_config_global_state;

DELIMITER ;;
CREATE PROCEDURE patch_audit_config_global_state()
BEGIN
    DECLARE auditServerID BIGINT;

    CREATE TABLE IF NOT EXISTS `x_audit_config`(
    `id` bigint(20) NOT NULL AUTO_INCREMENT,
    `create_time` datetime NULL DEFAULT NULL,
    `update_time` datetime NULL DEFAULT NULL,
    `cfg_name` varchar(255) NOT NULL,
    `cfg_value` LONGTEXT NULL DEFAULT NULL,
    `version` bigint(20) NULL DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `x_audit_config_UK_cfg_name`(`cfg_name`)
    )ROW_FORMAT=DYNAMIC;

    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = 'x_portal_user'
    ) THEN
        IF NOT EXISTS (SELECT 1 FROM x_portal_user WHERE login_id = 'rangerauditserver') THEN
            INSERT INTO x_portal_user(create_time, update_time, added_by_id, upd_by_id, first_name, last_name, pub_scr_name, login_id, password, email, status, user_src, notes)
            VALUES (UTC_TIMESTAMP(), UTC_TIMESTAMP(), NULL, NULL, 'rangerauditserver', '', 'rangerauditserver', 'rangerauditserver', '', 'rangerauditserver', 0, 0, NULL);
        END IF;

        UPDATE x_portal_user SET status = 0, password = '' WHERE login_id = 'rangerauditserver';

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
    END IF;

    IF NOT EXISTS (SELECT 1 FROM x_audit_config WHERE cfg_name = 'ingestor.url') THEN
        INSERT INTO x_audit_config (create_time, update_time, cfg_name, cfg_value, version)
        VALUES (UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'ingestor.url', 'https://ranger-audit-ingestor:8765', 1);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM x_audit_config WHERE cfg_name = 'service.hive.allowed.users') THEN
        INSERT INTO x_audit_config (create_time, update_time, cfg_name, cfg_value, version)
        VALUES (UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'service.hive.allowed.users', 'hive', 1);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM x_audit_config WHERE cfg_name = 'audit.partition.plan') THEN
        INSERT INTO x_audit_config (create_time, update_time, cfg_name, cfg_value, version)
        VALUES (UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'audit.partition.plan', '{"topic":"ranger_audits","plugins":{},"buffer":{"partitions":[1,2,3,4,5,6,7,8,9]}}', 1);
    END IF;
END;;

DELIMITER ;
CALL patch_audit_config_global_state();
DROP PROCEDURE IF EXISTS patch_audit_config_global_state;
