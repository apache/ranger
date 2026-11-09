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

CREATE SEQUENCE IF NOT EXISTS x_policy_ref_constraint_seq;
CREATE TABLE IF NOT EXISTS x_policy_ref_constraint(
id BIGINT DEFAULT nextval('x_policy_ref_constraint_seq'::regclass),
policy_id BIGINT NOT NULL,
service_def_id BIGINT NOT NULL,
constraint_name varchar(255) NOT NULL,
primary key(id),
CONSTRAINT x_p_ref_cnst_UK_polId_cName UNIQUE(policy_id, constraint_name),
CONSTRAINT x_p_ref_cnst_FK_policy_id FOREIGN KEY(policy_id) REFERENCES x_policy(id),
CONSTRAINT x_p_ref_cnst_FK_sdef_id FOREIGN KEY(service_def_id) REFERENCES x_service_def(id)
);
commit;
