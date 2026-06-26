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


DROP TABLE IF EXISTS x_audit_metrics CASCADE;
DROP SEQUENCE IF EXISTS x_audit_metrics_seq;

CREATE SEQUENCE x_audit_metrics_seq;
CREATE TABLE x_audit_metrics(
id BIGINT DEFAULT nextval('x_audit_metrics_seq'::regclass),
service_type BIGINT DEFAULT NULL NULL,
service_name varchar(255) DEFAULT NULL NULL,
app_id varchar(255) DEFAULT NULL NULL,
cluster_name varchar(255) DEFAULT NULL NULL,
client_ip varchar(255) DEFAULT NULL NULL,
metrics_text varchar(4000) DEFAULT NULL NULL,
throughput_unit varchar(255) DEFAULT NULL NULL,
number_of_audits BIGINT DEFAULT '0' NULL,
version BIGINT DEFAULT '0' NOT NULL,
create_time TIMESTAMP DEFAULT NULL NULL,
update_time TIMESTAMP DEFAULT NULL NULL,
added_by_id BIGINT DEFAULT NULL NULL,
upd_by_id BIGINT DEFAULT NULL NULL,
 PRIMARY KEY (id),
 CONSTRAINT x_audit_metrics_FK_service_type FOREIGN KEY (service_type) REFERENCES x_service_def (id),
 CONSTRAINT x_audit_metrics_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
 CONSTRAINT x_audit_metrics_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);
commit;