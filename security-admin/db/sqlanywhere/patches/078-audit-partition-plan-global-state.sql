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

CREATE OR REPLACE FUNCTION dbo.getXportalUIdByLoginId (input_val CHAR(60))
RETURNS INTEGER
BEGIN
  DECLARE myid INTEGER;
  SELECT x_portal_user.id INTO myid FROM x_portal_user WHERE x_portal_user.login_id = input_val;
  RETURN (myid);
END;
GO

BEGIN
    IF NOT EXISTS(SELECT * FROM SYS.SYSTABLE WHERE table_name = 'x_audit_config') THEN
        CREATE TABLE dbo.x_audit_config(
            id bigint IDENTITY NOT NULL,
            create_time datetime DEFAULT NULL NULL,
            update_time datetime DEFAULT NULL NULL,
            cfg_name varchar(255) NOT NULL,
            cfg_value LONG VARCHAR DEFAULT NULL NULL,
            version bigint DEFAULT NULL NULL,
            CONSTRAINT x_audit_config_PK_id PRIMARY KEY CLUSTERED(id),
            CONSTRAINT x_audit_config_UK_cfg_name UNIQUE NONCLUSTERED(cfg_name)
        );
    END IF;

    IF EXISTS(SELECT * FROM SYS.SYSTABLE WHERE table_name = 'x_portal_user') THEN
        IF NOT EXISTS(SELECT * FROM x_portal_user WHERE login_id = 'rangerauditserver') THEN
            INSERT INTO x_portal_user(create_time, update_time, added_by_id, upd_by_id, first_name, last_name, pub_scr_name, login_id, password, email, status, user_src, notes)
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, NULL, 'rangerauditserver', '', 'rangerauditserver', 'rangerauditserver', '', 'rangerauditserver', 0, 0, NULL);
        END IF;

        UPDATE x_portal_user SET status = 0, password = '' WHERE login_id = 'rangerauditserver';

        IF NOT EXISTS(SELECT * FROM x_portal_user_role WHERE user_id = getXportalUIdByLoginId('rangerauditserver') AND user_role = 'ROLE_ADMIN_AUDITOR') THEN
            INSERT INTO x_portal_user_role(create_time, update_time, added_by_id, upd_by_id, user_id, user_role, status)
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, NULL, getXportalUIdByLoginId('rangerauditserver'), 'ROLE_ADMIN_AUDITOR', 1);
        END IF;

        IF NOT EXISTS(SELECT * FROM x_user WHERE user_name = 'rangerauditserver') THEN
            INSERT INTO x_user(create_time, update_time, added_by_id, upd_by_id, user_name, descr, status)
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, NULL, 'rangerauditserver', 'Ranger audit server machine user', 0);
        END IF;
    END IF;

    IF NOT EXISTS(SELECT * FROM x_audit_config WHERE cfg_name = 'ingestor.url') THEN
        INSERT INTO x_audit_config(create_time, update_time, cfg_name, cfg_value, version)
        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'ingestor.url', 'https://ranger-audit-ingestor:8765', 1);
    END IF;
    IF NOT EXISTS(SELECT * FROM x_audit_config WHERE cfg_name = 'service.hive.allowed.users') THEN
        INSERT INTO x_audit_config(create_time, update_time, cfg_name, cfg_value, version)
        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'service.hive.allowed.users', 'hive', 1);
    END IF;
    IF NOT EXISTS(SELECT * FROM x_audit_config WHERE cfg_name = 'topic') THEN
        INSERT INTO x_audit_config(create_time, update_time, cfg_name, cfg_value, version)
        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'topic', 'ranger_audits', 1);
    END IF;
    IF NOT EXISTS(SELECT * FROM x_audit_config WHERE cfg_name = 'RangerAuditPartitionPlan') THEN
        INSERT INTO x_audit_config(create_time, update_time, cfg_name, cfg_value, version)
        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'RangerAuditPartitionPlan', '{"plugins":{},"buffer":{"partitions":[1,2,3,4,5,6,7,8,9]}}', 1);
    END IF;
END
GO
EXIT
