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


CREATE OR REPLACE PROCEDURE dbo.removeForeignKeysAndTable (IN table_name varchar(100))
AS
BEGIN
        DECLARE @stmt VARCHAR(300)
        DECLARE @tblname VARCHAR(300)
        DECLARE @drpstmt VARCHAR(1000)
        DECLARE cur CURSOR FOR select 'alter table dbo.' + table_name + ' drop constraint ' + role from SYS.SYSFOREIGNKEYS where foreign_creator ='dbo' and foreign_tname = table_name
        OPEN cur WITH HOLD
                fetch cur into @stmt
                WHILE (@@sqlstatus = 0)
                BEGIN
                        execute(@stmt)
                        fetch cur into @stmt
                END
        close cur
        DEALLOCATE CURSOR cur
        SET @tblname ='dbo.' + table_name;
        SET @drpstmt = 'DROP TABLE IF EXISTS ' + @tblname;
        execute(@drpstmt)
END
GO

call dbo.removeForeignKeysAndTable('x_audit_metrics')
GO

CREATE TABLE dbo.x_audit_metrics(
id bigint IDENTITY NOT NULL,
service_type bigint DEFAULT NULL NULL,
service_name varchar(255) DEFAULT NULL NULL,
app_id varchar(255) DEFAULT NULL NULL,
cluster_name varchar(255) DEFAULT NULL NULL,
client_ip varchar(255) DEFAULT NULL NULL,
metrics_text varchar(4000) DEFAULT NULL NULL,
throughput_unit varchar(255) DEFAULT NULL NULL,
number_of_audits bigint DEFAULT 0 NOT NULL,
version bigint  DEFAULT 0 NOT NULL,
create_time datetime DEFAULT NULL NULL,
update_time datetime DEFAULT NULL NULL,
added_by_id bigint DEFAULT NULL NULL,
upd_by_id bigint  DEFAULT NULL NULL,
CONSTRAINT x_x_audit_metrics_PK_id PRIMARY KEY CLUSTERED(id),
)
GO
ALTER TABLE dbo.x_audit_metrics ADD CONSTRAINT x_audit_metrics_FK_service_type FOREIGN KEY(service_type) REFERENCES dbo.x_service_def (id)
GO
ALTER TABLE dbo.x_audit_metrics ADD CONSTRAINT x_audit_metrics_FK_added_by_id FOREIGN KEY(added_by_id) REFERENCES dbo.x_portal_user (id)
GO
ALTER TABLE dbo.x_audit_metrics ADD CONSTRAINT x_audit_metrics_FK_upd_by_id FOREIGN KEY(upd_by_id) REFERENCES dbo.x_portal_user (id)
GO
exit