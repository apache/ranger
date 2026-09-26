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

BEGIN
DECLARE tableID INT = 0;
DECLARE columnID1 INT = 0;
DECLARE columnID2 INT = 0;
DECLARE columnID3 INT = 0;
    IF EXISTS(select * from SYS.SYSCOLUMNS where tname = 'x_trx_log_v2' and cname in('create_time', 'class_type', 'action')) THEN
        select table_id into tableID from SYS.SYSTAB where table_name = 'x_trx_log_v2';
        select column_id into columnID1 from SYS.SYSTABCOL where table_id=tableID and column_name = 'create_time';
        select column_id into columnID2 from SYS.SYSTABCOL where table_id=tableID and column_name = 'class_type';
        select column_id into columnID3 from SYS.SYSTABCOL where table_id=tableID and column_name = 'action';
        IF NOT EXISTS(select * from SYS.SYSIDXCOL where table_id=tableID and column_id in (columnID1, columnID2, columnID3)) THEN
            CREATE NONCLUSTERED INDEX x_trx_log_v2_IDX_metrics ON dbo.x_trx_log_v2(create_time ASC, class_type ASC, action ASC);
        END IF;
    END IF;
END
GO

DROP VIEW IF EXISTS dbo.vx_audit_admin_metrics_by_days
GO
CREATE VIEW vx_audit_admin_metrics_by_days AS
SELECT
    class_type,
    action,
    COUNT(id) AS audit_count,
    DAY(create_time) as days,
    cast(create_time as date) as auditDate
FROM x_trx_log_v2
GROUP BY action, class_type, days, auditDate
ORDER BY auditDate, days;
GO
exit
