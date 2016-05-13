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

/
CREATE SEQUENCE X_SERVICE_VERSION_INFO_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE TABLE x_service_version_info(
id NUMBER(20) NOT NULL,
service_id NUMBER(20) NOT NULL,
policy_version NUMBER(20) DEFAULT 0 NOT NULL,
policy_update_time DATE DEFAULT NULL NULL,
tag_version NUMBER(20) DEFAULT 0 NOT NULL,
tag_update_time DATE DEFAULT NULL NULL,
primary key (id),
CONSTRAINT x_svc_ver_info_FK_service_id FOREIGN KEY (service_id) REFERENCES x_service(id)
);
CREATE INDEX x_svc_ver_info_IDX_service_id ON x_service_version_info(service_id);
commit;