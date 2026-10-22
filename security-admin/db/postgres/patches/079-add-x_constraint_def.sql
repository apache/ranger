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

CREATE SEQUENCE IF NOT EXISTS x_constraint_def_seq;
CREATE TABLE IF NOT EXISTS x_constraint_def(
id BIGINT DEFAULT nextval('x_constraint_def_seq'::regclass),
guid VARCHAR(1024) DEFAULT NULL NULL,
create_time TIMESTAMP DEFAULT NULL NULL,
update_time TIMESTAMP DEFAULT NULL NULL,
added_by_id BIGINT DEFAULT NULL NULL,
upd_by_id BIGINT DEFAULT NULL NULL,
def_id BIGINT NOT NULL,
item_id BIGINT NOT NULL,
name VARCHAR(1024) DEFAULT NULL NULL,
label VARCHAR(1024) DEFAULT NULL NULL,
description VARCHAR(1024) DEFAULT NULL NULL,
rb_key_label VARCHAR(1024) DEFAULT NULL NULL,
rb_key_description VARCHAR(1024) DEFAULT NULL NULL,
resources TEXT DEFAULT NULL NULL,
access_types TEXT DEFAULT NULL NULL,
parameters TEXT DEFAULT NULL NULL,
options TEXT DEFAULT NULL NULL,
sort_order INT DEFAULT '0' NULL,
primary key(id),
CONSTRAINT x_constraint_def_FK_defid FOREIGN KEY(def_id) REFERENCES x_service_def(id),
CONSTRAINT x_constraint_def_FK_added_by FOREIGN KEY(added_by_id) REFERENCES x_portal_user(id),
CONSTRAINT x_constraint_def_FK_upd_by FOREIGN KEY(upd_by_id) REFERENCES x_portal_user(id)
);
CREATE INDEX IF NOT EXISTS x_constraint_def_IDX_def_id ON x_constraint_def(def_id);
