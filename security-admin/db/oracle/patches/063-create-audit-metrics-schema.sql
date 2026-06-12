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


CREATE OR REPLACE PROCEDURE spdropsequence(ObjName IN varchar2)
IS
v_counter integer;
BEGIN
    select count(*) into v_counter from user_sequences where sequence_name = upper(ObjName);
      if (v_counter > 0) then
        execute immediate 'DROP SEQUENCE ' || ObjName;
      end if;
END;/
/

CREATE OR REPLACE PROCEDURE spdroptable(ObjName IN varchar2)
IS
v_counter integer;
BEGIN
    select count(*) into v_counter from user_tables where table_name = upper(ObjName);
     if (v_counter > 0) then
     execute immediate 'drop table ' || ObjName || ' cascade constraints';
     end if;
END;/
/


call spdropsequence('X_AUDIT_METRICS_SEQ');

CREATE SEQUENCE X_AUDIT_METRICS_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

call spdroptable('x_audit_metrics');
commit;

CREATE TABLE x_audit_metrics(
id NUMBER(20) NOT NULL,
service_type NUMBER(20) DEFAULT NULL NULL,
service_name varchar(255) DEFAULT NULL NULL,
app_id varchar(255) DEFAULT NULL NULL,
cluster_name varchar(255) DEFAULT NULL NULL,
client_ip varchar(255) DEFAULT NULL NULL,
metrics_text varchar(4000) DEFAULT NULL NULL,
throughput_unit varchar(255) DEFAULT NULL NULL,
number_of_audits NUMBER(20) DEFAULT NULL NULL,
version NUMBER(20) DEFAULT NULL NULL,
create_time DATE DEFAULT NULL NULL,
update_time DATE DEFAULT NULL NULL,
added_by_id NUMBER(20) DEFAULT NULL NULL,
upd_by_id NUMBER(20) DEFAULT NULL NULL,
PRIMARY KEY (id),
CONSTRAINT x_audit_metrics_FK_service_type FOREIGN KEY (service_type) REFERENCES x_service_def (id),
CONSTRAINT x_audit_metrics_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
CONSTRAINT x_audit_metrics_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);
commit;
