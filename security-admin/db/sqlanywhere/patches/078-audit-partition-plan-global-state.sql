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
    DECLARE planJson LONG VARCHAR DEFAULT '{"version":1,"topic":"ranger_audits","topicPartitionCount":9,"plugins":{},"buffer":{"partitions":[1,2,3,4,5,6,7,8,9]}}';

    IF EXISTS(SELECT * FROM SYS.SYSCOLUMNS WHERE tname = 'x_ranger_global_state' AND cname = 'state_name') THEN
        IF EXISTS(SELECT * FROM SYS.SYSCOLUMNS WHERE tname = 'x_ranger_global_state' AND cname = 'app_data' AND coltype = 'varchar') THEN
            ALTER TABLE dbo.x_ranger_global_state MODIFY app_data LONG VARCHAR DEFAULT NULL;
        END IF;

        IF NOT EXISTS(SELECT * FROM x_portal_user WHERE login_id = 'rangerauditserver') THEN
            INSERT INTO x_portal_user(create_time, update_time, added_by_id, upd_by_id, first_name, last_name, pub_scr_name, login_id, password, email, status, user_src, notes)
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, NULL, 'rangerauditserver', '', 'rangerauditserver', 'rangerauditserver', '9c8f4e2b1a0d6e3f7b5c4a8291d0e6f3', 'rangerauditserver', 1, 0, NULL);
        END IF;

        IF NOT EXISTS(SELECT * FROM x_portal_user_role WHERE user_id = getXportalUIdByLoginId('rangerauditserver') AND user_role = 'ROLE_ADMIN_AUDITOR') THEN
            INSERT INTO x_portal_user_role(create_time, update_time, added_by_id, upd_by_id, user_id, user_role, status)
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, NULL, getXportalUIdByLoginId('rangerauditserver'), 'ROLE_ADMIN_AUDITOR', 1);
        END IF;

        IF NOT EXISTS(SELECT * FROM x_user WHERE user_name = 'rangerauditserver') THEN
            INSERT INTO x_user(create_time, update_time, added_by_id, upd_by_id, user_name, descr, status)
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, NULL, 'rangerauditserver', 'Ranger audit server machine user', 0);
        END IF;

        IF NOT EXISTS(SELECT * FROM x_ranger_global_state WHERE state_name = 'RangerAuditPartitionPlan') THEN
            INSERT INTO x_ranger_global_state(create_time, update_time, added_by_id, upd_by_id, version, state_name, app_data)
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, getXportalUIdByLoginId('admin'), getXportalUIdByLoginId('admin'), 1, 'RangerAuditPartitionPlan', planJson);
        END IF;
    END IF;
END
GO
EXIT
