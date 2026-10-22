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

BEGIN
    IF NOT EXISTS(SELECT * FROM SYS.SYSTABLE WHERE table_name = 'x_constraint_def') THEN
        CREATE TABLE dbo.x_constraint_def (
            id bigint IDENTITY NOT NULL,
            guid varchar(1024) DEFAULT NULL NULL,
            create_time datetime DEFAULT NULL NULL,
            update_time datetime DEFAULT NULL NULL,
            added_by_id bigint DEFAULT NULL NULL,
            upd_by_id bigint DEFAULT NULL NULL,
            def_id bigint NOT NULL,
            item_id bigint NOT NULL,
            name varchar(1024) DEFAULT NULL NULL,
            label varchar(1024) DEFAULT NULL NULL,
            description varchar(1024) DEFAULT NULL NULL,
            rb_key_label varchar(1024) DEFAULT NULL NULL,
            rb_key_description varchar(1024) DEFAULT NULL NULL,
            resources text DEFAULT NULL NULL,
            access_types text DEFAULT NULL NULL,
            parameters text DEFAULT NULL NULL,
            options text DEFAULT NULL NULL,
            sort_order int DEFAULT 0 NULL,
            CONSTRAINT x_constraint_def_PK_id PRIMARY KEY CLUSTERED(id)
        );

        ALTER TABLE dbo.x_constraint_def ADD CONSTRAINT x_constraint_def_FK_defid FOREIGN KEY(def_id) REFERENCES dbo.x_service_def (id);
        ALTER TABLE dbo.x_constraint_def ADD CONSTRAINT x_constraint_def_FK_added_by FOREIGN KEY(added_by_id) REFERENCES dbo.x_portal_user (id);
        ALTER TABLE dbo.x_constraint_def ADD CONSTRAINT x_constraint_def_FK_upd_by FOREIGN KEY(upd_by_id) REFERENCES dbo.x_portal_user (id);
    END IF;
END
GO

CREATE INDEX IF NOT EXISTS x_constraint_def_IDX_def_id ON dbo.x_constraint_def(def_id ASC)
GO
