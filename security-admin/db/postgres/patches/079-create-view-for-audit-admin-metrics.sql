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
CREATE OR REPLACE FUNCTION create_index_for_x_trx_log_v2()
RETURNS void AS $$
DECLARE
    v_attnum1 integer := 0;
    v_attnum2 integer := 0;
    v_attnum3 integer := 0;
    v_table_exists integer := 0;
BEGIN
    SELECT COUNT(*) INTO v_table_exists FROM pg_class WHERE relname = 'x_trx_log_v2';
    IF v_table_exists > 0 THEN
        select attnum into v_attnum1 from pg_attribute where attrelid in(select oid from pg_class where relname='x_trx_log_v2') and attname in('create_time');
        select attnum into v_attnum2 from pg_attribute where attrelid in(select oid from pg_class where relname='x_trx_log_v2') and attname in('class_type');
        select attnum into v_attnum3 from pg_attribute where attrelid in(select oid from pg_class where relname='x_trx_log_v2') and attname in('action');
        IF v_attnum1 > 0 and v_attnum2 > 0 and v_attnum3 > 0 THEN
            IF not exists (select * from pg_index where indrelid in(select oid from pg_class where relname='x_trx_log_v2') and indkey[0]=v_attnum1 and indkey[1]=v_attnum2 and indkey[2]=v_attnum3) THEN
                CREATE INDEX x_trx_log_v2_IDX_metrics ON x_trx_log_v2 (create_time, class_type, action);
            END IF;
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;
select 'delimiter end';

select create_index_for_x_trx_log_v2();
select 'delimiter end';

DROP VIEW IF EXISTS vx_audit_admin_metrics_by_days;
CREATE VIEW vx_audit_admin_metrics_by_days AS
SELECT
    class_type,
    action,
    COUNT(id) AS audit_count,
    EXTRACT(DAY FROM create_time) AS days,
    CAST(create_time AS DATE) AS auditDate
FROM x_trx_log_v2
GROUP BY action, class_type, days, auditDate
ORDER BY auditDate, days;
select 'delimiter end';
