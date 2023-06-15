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

DROP TABLE IF EXISTS x_security_zone_ref_role CASCADE;

DROP SEQUENCE IF EXISTS x_sec_zone_ref_role_seq;

CREATE SEQUENCE x_sec_zone_ref_role_seq;
CREATE TABLE x_security_zone_ref_role (
id BIGINT DEFAULT nextval('x_sec_zone_ref_role_seq'::regclass),
create_time TIMESTAMP DEFAULT NULL NULL,
update_time TIMESTAMP DEFAULT NULL NULL,
added_by_id BIGINT DEFAULT NULL NULL,
upd_by_id BIGINT DEFAULT NULL NULL,
zone_id BIGINT DEFAULT NULL NULL,
role_id BIGINT DEFAULT NULL NULL,
role_name varchar(255) NULL DEFAULT NULL::character varying,
primary key (id),
CONSTRAINT x_sz_ref_role_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_sz_ref_role_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_sz_ref_role_FK_zone_id FOREIGN KEY (zone_id) REFERENCES x_security_zone (id),
CONSTRAINT x_sz_ref_role_FK_role_id FOREIGN KEY (role_id) REFERENCES x_role (id)
);
commit;

