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
CREATE TABLE IF NOT EXISTS x_audit_config (
id BIGINT,
create_time TIMESTAMP DEFAULT NULL NULL,
update_time TIMESTAMP DEFAULT NULL NULL,
cfg_name varchar(255) NOT NULL,
cfg_value TEXT DEFAULT NULL NULL,
version BIGINT DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_audit_config_UK_cfg_name UNIQUE (cfg_name)
);
CREATE SEQUENCE IF NOT EXISTS x_audit_config_seq;
ALTER SEQUENCE x_audit_config_seq OWNED BY x_audit_config.id;
ALTER TABLE x_audit_config ALTER COLUMN id SET DEFAULT nextval('x_audit_config_seq'::regclass);

CREATE OR REPLACE FUNCTION patch_audit_config_global_state()
RETURNS void AS $$
DECLARE
    v_audit_user_id bigint;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'x_portal_user') THEN
        IF NOT EXISTS (SELECT 1 FROM x_portal_user WHERE login_id = 'rangerauditserver') THEN
            INSERT INTO x_portal_user(create_time, update_time, first_name, last_name, pub_scr_name, login_id, password, email, status)
            VALUES (current_timestamp, current_timestamp, 'rangerauditserver', '', 'rangerauditserver', 'rangerauditserver', '', 'rangerauditserver', 0);
        END IF;

        UPDATE x_portal_user SET status = 0, password = '' WHERE login_id = 'rangerauditserver';

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
    END IF;

    IF NOT EXISTS (SELECT 1 FROM x_audit_config WHERE cfg_name = 'ingestor.url') THEN
        INSERT INTO x_audit_config (create_time, update_time, cfg_name, cfg_value, version)
        VALUES (current_timestamp, current_timestamp, 'ingestor.url', 'https://ranger-audit-ingestor:8765', 1);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM x_audit_config WHERE cfg_name = 'service.hive.allowed.users') THEN
        INSERT INTO x_audit_config (create_time, update_time, cfg_name, cfg_value, version)
        VALUES (current_timestamp, current_timestamp, 'service.hive.allowed.users', 'hive', 1);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM x_audit_config WHERE cfg_name = 'topic') THEN
        INSERT INTO x_audit_config (create_time, update_time, cfg_name, cfg_value, version)
        VALUES (current_timestamp, current_timestamp, 'topic', 'ranger_audits', 1);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM x_audit_config WHERE cfg_name = 'RangerAuditPartitionPlan') THEN
        INSERT INTO x_audit_config (create_time, update_time, cfg_name, cfg_value, version)
        VALUES (current_timestamp, current_timestamp, 'RangerAuditPartitionPlan', '{"plugins":{},"buffer":{"partitions":[1,2,3,4,5,6,7,8,9]}}', 1);
    END IF;
END;
$$ LANGUAGE plpgsql;
select 'delimiter end';

select patch_audit_config_global_state();
select 'delimiter end';
