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


DROP VIEW IF EXISTS vx_audit_metrics_by_hours;
DROP VIEW IF EXISTS vx_audit_metrics_by_days;

CREATE VIEW vx_audit_metrics_by_hours AS select
	service_type,
	service_name,
	app_id,
	cluster_name,
	client_ip,
	EXTRACT(HOUR from create_time) as hours,
	sum(number_of_audits) as numberOfAudits
	from x_audit_metrics
	where (cast(CREATE_TIME as date) = CURRENT_DATE)
	group by service_type, service_name, app_id, cluster_name, client_ip, hours
	ORDER BY hours;

CREATE OR REPLACE VIEW	vx_audit_metrics_by_days AS
	select
			service_type,
			service_name,
			app_id,
			cluster_name,
			client_ip,
			EXTRACT(DAY from create_time) as days,
			sum(number_of_audits) as numberOfAudits,
			cast(create_time as date) as auditDate
			from x_audit_metrics
			group by  service_type, service_name, app_id, cluster_name, client_ip, days, auditDate
			ORDER BY auditDate, days;
