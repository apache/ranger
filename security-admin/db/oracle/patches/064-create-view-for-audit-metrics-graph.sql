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

call spdropview('vx_audit_metrics_by_hours');
call spdropview('vx_audit_metrics_by_days');
commit;

CREATE VIEW vx_audit_metrics_by_hours AS
		select
			service_type,
			service_name,
			app_id,
			cluster_name,
			client_ip,
			EXTRACT(HOUR from CAST(create_time AS TIMESTAMP)) as hours,
			sum(number_of_audits) as numberOfAudits
			from x_audit_metrics
			where CREATE_TIME >= TRUNC(CURRENT_DATE)
			group by service_type, service_name, app_id, cluster_name, client_ip, EXTRACT(HOUR from CAST(create_time AS TIMESTAMP))
			ORDER BY hours;

CREATE VIEW	vx_audit_metrics_by_days AS
	select
			service_type,
			service_name,
			app_id,
			cluster_name,
			client_ip,
			EXTRACT(DAY from CAST(create_time AS TIMESTAMP)) AS days,
			sum(number_of_audits) as numberOfAudits,
			trunc(create_time) as auditDate
			from x_audit_metrics
			group by  service_type, service_name, app_id, cluster_name, client_ip, EXTRACT(DAY from CAST(create_time AS TIMESTAMP)), trunc(create_time)
			ORDER BY auditDate, days;
commit;
