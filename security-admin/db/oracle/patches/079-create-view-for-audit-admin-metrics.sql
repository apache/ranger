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
    v_index_exists number:=0;
    v_table_exists number := 0;
BEGIN
    SELECT COUNT(*) INTO v_table_exists FROM USER_TABLES WHERE TABLE_NAME = upper('x_trx_log_v2');
    IF (v_table_exists > 0) THEN
        SELECT COUNT(*) INTO v_index_exists FROM USER_INDEXES WHERE INDEX_NAME = upper('x_trx_log_v2_IDX_metrics') AND TABLE_NAME= upper('x_trx_log_v2');
        IF (v_index_exists = 0) THEN
            execute IMMEDIATE 'CREATE INDEX x_trx_log_v2_IDX_metrics ON x_trx_log_v2 (create_time, class_type, action)';
            commit;
        END IF;
    END IF;
END;/

CREATE OR REPLACE PROCEDURE spdropview(ObjName IN varchar2)
IS
v_counter integer;
BEGIN
    select count(*) into v_counter from User_Views where VIEW_NAME = upper(ObjName);
     if (v_counter > 0) then
     execute immediate 'DROP VIEW ' || ObjName;
     end if;
END;/
/

call spdropview('vx_audit_admin_metrics_by_days');
commit;

CREATE VIEW vx_audit_admin_metrics_by_days AS
SELECT
    class_type,
    action,
    COUNT(id) AS audit_count,
    EXTRACT(DAY from CAST(create_time AS TIMESTAMP)) AS days,
    TRUNC(create_time) AS auditDate
FROM x_trx_log_v2
GROUP BY
    action,
    class_type,
    EXTRACT(DAY from CAST(create_time AS TIMESTAMP)),
    TRUNC(create_time)
ORDER BY auditDate, days;
commit;
