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

DROP VIEW IF EXISTS vx_policy_export_audit_metrics_by_days;
CREATE VIEW vx_policy_export_audit_metrics_by_days AS
SELECT
    repository_name,
    agent_id,
    client_ip,
    http_ret_code,
    COUNT(id) AS audit_count,
    EXTRACT(DAY FROM create_time) AS days,
    CAST(create_time AS DATE) AS auditDate
FROM x_policy_export_audit
GROUP BY
    repository_name, agent_id, client_ip, http_ret_code, days, auditDate
ORDER BY auditDate, days;
commit;
