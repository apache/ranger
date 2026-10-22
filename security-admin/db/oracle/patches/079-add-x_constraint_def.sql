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

DECLARE
    v_table_count NUMBER := 0;
    v_seq_count NUMBER := 0;
    v_idx_count NUMBER := 0;
BEGIN
    SELECT count(*) INTO v_table_count FROM user_tables WHERE table_name = 'X_CONSTRAINT_DEF';
    IF (v_table_count = 0) THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE x_constraint_def (
                id NUMBER(20) NOT NULL,
                guid VARCHAR(1024) DEFAULT NULL NULL,
                create_time DATE DEFAULT NULL NULL,
                update_time DATE DEFAULT NULL NULL,
                added_by_id NUMBER(20) DEFAULT NULL NULL,
                upd_by_id NUMBER(20) DEFAULT NULL NULL,
                def_id NUMBER(20) NOT NULL,
                item_id NUMBER(20) NOT NULL,
                name VARCHAR(1024) DEFAULT NULL NULL,
                label VARCHAR(1024) DEFAULT NULL NULL,
                description VARCHAR(1024) DEFAULT NULL NULL,
                rb_key_label VARCHAR(1024) DEFAULT NULL NULL,
                rb_key_description VARCHAR(1024) DEFAULT NULL NULL,
                resources CLOB DEFAULT NULL NULL,
                access_types CLOB DEFAULT NULL NULL,
                parameters CLOB DEFAULT NULL NULL,
                options CLOB DEFAULT NULL NULL,
                sort_order NUMBER(10) DEFAULT ''0'' NULL,
                primary key (id),
                CONSTRAINT x_constraint_def_FK_defid FOREIGN KEY (def_id) REFERENCES x_service_def (id),
                CONSTRAINT x_constraint_def_FK_added_by FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
                CONSTRAINT x_constraint_def_FK_upd_by FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
            )';
    END IF;

    SELECT count(*) INTO v_seq_count FROM user_sequences WHERE sequence_name = 'X_CONSTRAINT_DEF_SEQ';
    IF (v_seq_count = 0) THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE X_CONSTRAINT_DEF_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
    END IF;

    SELECT count(*) INTO v_idx_count FROM user_indexes WHERE index_name = 'X_CONSTRAINT_DEF_IDX_DEF_ID';
    IF (v_idx_count = 0) THEN
        EXECUTE IMMEDIATE 'CREATE INDEX x_constraint_def_IDX_def_id ON x_constraint_def(def_id)';
    END IF;
END;
/

commit;
