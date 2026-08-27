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
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF EXISTS (SELECT *
           FROM   sys.objects
           WHERE  object_id = OBJECT_ID(N'dbo.getXportalUIdByLoginId')
                  AND type IN ( N'FN', N'IF', N'TF', N'FS', N'FT' ))
  DROP FUNCTION dbo.getXportalUIdByLoginId
GO
CREATE FUNCTION dbo.getXportalUIdByLoginId(@inputValue varchar(200))
RETURNS int
AS
BEGIN
        DECLARE @myid int;
        SELECT @myid = id FROM x_portal_user WHERE x_portal_user.login_id = @inputValue;
        RETURN @myid;
END
GO

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'x_ranger_global_state' AND COLUMN_NAME = 'state_name')
BEGIN
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'x_audit_config')
    BEGIN
        CREATE TABLE [dbo].[x_audit_config](
            [id] [bigint] IDENTITY(1,1) NOT NULL,
            [create_time] [datetime2] DEFAULT NULL NULL,
            [update_time] [datetime2] DEFAULT NULL NULL,
            [cfg_name] [varchar](255) NOT NULL,
            [cfg_value] NVARCHAR(MAX) DEFAULT NULL NULL,
            [version] [bigint] DEFAULT NULL NULL,
            PRIMARY KEY CLUSTERED ([id] ASC),
            CONSTRAINT [x_audit_config_UK_cfg_name] UNIQUE NONCLUSTERED ([cfg_name] ASC)
        ) ON [PRIMARY];
    END;

    IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'x_ranger_global_state' AND COLUMN_NAME = 'app_data' AND DATA_TYPE = 'varchar')
    BEGIN
        ALTER TABLE [dbo].[x_ranger_global_state] ALTER COLUMN [app_data] NVARCHAR(MAX) NULL;
    END;

    IF NOT EXISTS(SELECT * FROM x_portal_user WHERE login_id = 'rangerauditserver')
    BEGIN
        INSERT INTO x_portal_user (create_time, update_time, added_by_id, upd_by_id, first_name, last_name, pub_scr_name, login_id, password, email, status, user_src, notes)
        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, NULL, 'rangerauditserver', '', 'rangerauditserver', 'rangerauditserver', '', 'rangerauditserver', 0, 0, NULL);
    END;

    UPDATE x_portal_user SET status = 0, password = '' WHERE login_id = 'rangerauditserver';

    IF NOT EXISTS(SELECT * FROM x_portal_user_role WHERE user_id = dbo.getXportalUIdByLoginId('rangerauditserver') AND user_role = 'ROLE_ADMIN_AUDITOR')
    BEGIN
        INSERT INTO x_portal_user_role (create_time, update_time, added_by_id, upd_by_id, user_id, user_role, status)
        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, NULL, dbo.getXportalUIdByLoginId('rangerauditserver'), 'ROLE_ADMIN_AUDITOR', 1);
    END;

    IF NOT EXISTS(SELECT * FROM x_user WHERE user_name = 'rangerauditserver')
    BEGIN
        INSERT INTO x_user (create_time, update_time, added_by_id, upd_by_id, user_name, descr, status)
        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, NULL, 'rangerauditserver', 'Ranger audit server machine user', 0);
    END;

    IF NOT EXISTS(SELECT * FROM x_ranger_global_state WHERE state_name = 'RangerAuditPartitionPlan')
    BEGIN
        INSERT INTO x_ranger_global_state (create_time, update_time, added_by_id, upd_by_id, version, state_name, app_data)
        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, dbo.getXportalUIdByLoginId('admin'), dbo.getXportalUIdByLoginId('admin'), 1, 'RangerAuditPartitionPlan',
            N'{"version":1,"topic":"ranger_audits","topicPartitionCount":9,"plugins":{},"buffer":{"partitions":[1,2,3,4,5,6,7,8,9]}}');
    END;

    IF NOT EXISTS(SELECT * FROM x_audit_config WHERE cfg_name = 'ingestor.url')
    BEGIN
        INSERT INTO x_audit_config (create_time, update_time, cfg_name, cfg_value, version)
        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'ingestor.url', N'https://ranger-audit-ingestor:8765', 1);
    END;
    IF NOT EXISTS(SELECT * FROM x_audit_config WHERE cfg_name = 'service.hive.allowed.users')
    BEGIN
        INSERT INTO x_audit_config (create_time, update_time, cfg_name, cfg_value, version)
        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'service.hive.allowed.users', N'hive', 1);
    END;
    IF NOT EXISTS(SELECT * FROM x_audit_config WHERE cfg_name = 'topic-partitions')
    BEGIN
        INSERT INTO x_audit_config (create_time, update_time, cfg_name, cfg_value, version)
        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'topic-partitions', N'30', 1);
    END;
END;
GO
EXIT
