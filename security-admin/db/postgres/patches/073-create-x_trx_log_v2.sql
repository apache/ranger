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

DROP TABLE IF EXISTS x_trx_log_v2 CASCADE;

DROP SEQUENCE IF EXISTS x_trx_log_v2_seq;

CREATE SEQUENCE x_trx_log_v2_seq;
CREATE TABLE x_trx_log_v2(
id BIGINT DEFAULT nextval('x_trx_log_v2_seq'::regclass),
create_time TIMESTAMP DEFAULT NULL NULL,
added_by_id BIGINT DEFAULT NULL NULL,
class_type INT DEFAULT '0' NOT NULL,
object_id BIGINT DEFAULT NULL NULL,
parent_object_id BIGINT DEFAULT NULL NULL,
parent_object_class_type INT DEFAULT '0' NOT NULL,
parent_object_name VARCHAR(1024) DEFAULT NULL NULL,
object_name VARCHAR(1024) DEFAULT NULL NULL,
change_info TEXT NULL DEFAULT NULL,
trx_id VARCHAR(1024) DEFAULT NULL NULL,
action VARCHAR(255) DEFAULT NULL NULL,
sess_id VARCHAR(512) DEFAULT NULL NULL,
req_id VARCHAR(30) DEFAULT NULL NULL,
sess_type VARCHAR(30) DEFAULT NULL NULL,
PRIMARY KEY(id)
);

CREATE INDEX x_trx_log_v2_FK_added_by_id ON x_trx_log_v2(added_by_id);
CREATE INDEX x_trx_log_v2_cr_time ON x_trx_log_v2(create_time);
CREATE INDEX x_trx_log_v2_trx_id ON x_trx_log_v2(trx_id);