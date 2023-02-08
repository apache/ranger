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

-- Commented because dropping this column would break high-availability setups
-- We can uncomment this after the next major version upgrade
/*
CREATE OR REPLACE PROCEDURE dbo.dropTableColumn (@table_name varchar(100), @column_name varchar(100))
AS
BEGIN
    DECLARE @stmt VARCHAR(300)
    IF EXISTS(select * from SYS.SYSCOLUMNS where tname = @table_name and cname = @column_name)
    BEGIN
        SET @stmt = 'ALTER TABLE dbo.' + @table_name + ' DROP ' + @column_name;
        execute(@stmt)
    END
END
GO

CREATE OR REPLACE PROCEDURE dbo.dropTableConstraint (@table_name varchar(100), @constraint_name varchar(100))
AS
BEGIN
    DECLARE @stmt VARCHAR(300)
    IF EXISTS(select * from SYS.SYSCONSTRAINT where constraint_name = @constraint_name)
    BEGIN
        SET @stmt = 'ALTER TABLE dbo.' + @table_name + ' DROP CONSTRAINT ' + @constraint_name;
        execute(@stmt)
    END
END
GO

call dbo.dropTableColumn('x_policy_ref_resource', 'guid')
GO
call dbo.dropTableColumn('x_policy_ref_resource', 'create_time')
GO
call dbo.dropTableColumn('x_policy_ref_resource', 'update_time')
GO
call dbo.dropTableConstraint('x_policy_ref_resource', 'x_policy_ref_resource_FK_added_by')
GO
call dbo.dropTableColumn('x_policy_ref_resource', 'added_by_id')
GO
call dbo.dropTableConstraint('x_policy_ref_resource', 'x_policy_ref_resource_FK_upd_by')
GO
call dbo.dropTableColumn('x_policy_ref_resource', 'upd_by_id')
GO

call dbo.dropTableColumn('x_policy_ref_role', 'create_time')
GO
call dbo.dropTableColumn('x_policy_ref_role', 'update_time')
GO
call dbo.dropTableConstraint('x_policy_ref_role', 'x_pol_ref_role_FK_added_by_id')
GO
call dbo.dropTableColumn('x_policy_ref_role', 'added_by_id')
GO
call dbo.dropTableConstraint('x_policy_ref_role', 'x_pol_ref_role_FK_upd_by_id')
GO
call dbo.dropTableColumn('x_policy_ref_role', 'upd_by_id')
GO

call dbo.dropTableColumn('x_policy_ref_group', 'guid')
GO
call dbo.dropTableColumn('x_policy_ref_group', 'create_time')
GO
call dbo.dropTableColumn('x_policy_ref_group', 'update_time')
GO
call dbo.dropTableConstraint('x_policy_ref_group', 'x_policy_ref_group_FK_added_by')
GO
call dbo.dropTableColumn('x_policy_ref_group', 'added_by_id')
GO
call dbo.dropTableConstraint('x_policy_ref_group', 'x_policy_ref_group_FK_upd_by')
GO
call dbo.dropTableColumn('x_policy_ref_group', 'upd_by_id')
GO

call dbo.dropTableColumn('x_policy_ref_user', 'guid')
GO
call dbo.dropTableColumn('x_policy_ref_user', 'create_time')
GO
call dbo.dropTableColumn('x_policy_ref_user', 'update_time')
GO
call dbo.dropTableConstraint('x_policy_ref_user', 'x_policy_ref_user_FK_added_by')
GO
call dbo.dropTableColumn('x_policy_ref_user', 'added_by_id')
GO
call dbo.dropTableConstraint('x_policy_ref_user', 'x_policy_ref_user_FK_upd_by')
GO
call dbo.dropTableColumn('x_policy_ref_user', 'upd_by_id')
GO

call dbo.dropTableColumn('x_policy_ref_access_type', 'guid')
GO
call dbo.dropTableColumn('x_policy_ref_access_type', 'create_time')
GO
call dbo.dropTableColumn('x_policy_ref_access_type', 'update_time')
GO
call dbo.dropTableConstraint('x_policy_ref_access_type', 'x_policy_ref_access_type_FK_added_by')
GO
call dbo.dropTableColumn('x_policy_ref_access_type', 'added_by_id')
GO
call dbo.dropTableConstraint('x_policy_ref_access_type', 'x_policy_ref_access_type_FK_upd_by')
GO
call dbo.dropTableColumn('x_policy_ref_access_type', 'upd_by_id')
GO

call dbo.dropTableColumn('x_policy_ref_condition', 'guid')
GO
call dbo.dropTableColumn('x_policy_ref_condition', 'create_time')
GO
call dbo.dropTableColumn('x_policy_ref_condition', 'update_time')
GO
call dbo.dropTableConstraint('x_policy_ref_condition', 'x_policy_ref_condition_FK_added_by')
GO
call dbo.dropTableColumn('x_policy_ref_condition', 'added_by_id')
GO
call dbo.dropTableConstraint('x_policy_ref_condition', 'x_policy_ref_condition_FK_upd_by')
GO
call dbo.dropTableColumn('x_policy_ref_condition', 'upd_by_id')
GO

call dbo.dropTableColumn('x_policy_ref_datamask_type', 'guid')
GO
call dbo.dropTableColumn('x_policy_ref_datamask_type', 'create_time')
GO
call dbo.dropTableColumn('x_policy_ref_datamask_type', 'update_time')
GO
call dbo.dropTableConstraint('x_policy_ref_datamask_type', 'x_policy_ref_datamask_type_FK_added_by')
GO
call dbo.dropTableColumn('x_policy_ref_datamask_type', 'added_by_id')
GO
call dbo.dropTableConstraint('x_policy_ref_datamask_type', 'x_policy_ref_datamask_type_FK_upd_by')
GO
call dbo.dropTableColumn('x_policy_ref_datamask_type', 'upd_by_id')
GO

call dbo.dropTableColumn('x_security_zone_ref_service', 'create_time')
GO
call dbo.dropTableColumn('x_security_zone_ref_service', 'update_time')
GO
call dbo.dropTableConstraint('x_security_zone_ref_service', 'x_sz_ref_service_FK_added_by_id')
GO
call dbo.dropTableColumn('x_security_zone_ref_service', 'added_by_id')
GO
call dbo.dropTableConstraint('x_security_zone_ref_service', 'x_sz_ref_service_FK_upd_by_id')
GO
call dbo.dropTableColumn('x_security_zone_ref_service', 'upd_by_id')
GO

call dbo.dropTableColumn('x_security_zone_ref_tag_srvc', 'create_time')
GO
call dbo.dropTableColumn('x_security_zone_ref_tag_srvc', 'update_time')
GO
call dbo.dropTableConstraint('x_security_zone_ref_tag_srvc', 'x_sz_ref_tag_service_FK_added_by_id')
GO
call dbo.dropTableColumn('x_security_zone_ref_tag_srvc', 'added_by_id')
GO
call dbo.dropTableConstraint('x_security_zone_ref_tag_srvc', 'x_sz_ref_tag_service_FK_upd_by_id')
GO
call dbo.dropTableColumn('x_security_zone_ref_tag_srvc', 'upd_by_id')
GO

call dbo.dropTableColumn('x_security_zone_ref_user', 'create_time')
GO
call dbo.dropTableColumn('x_security_zone_ref_user', 'update_time')
GO
call dbo.dropTableConstraint('x_security_zone_ref_user', 'x_sz_ref_user_FK_added_by_id')
GO
call dbo.dropTableColumn('x_security_zone_ref_user', 'added_by_id')
GO
call dbo.dropTableConstraint('x_security_zone_ref_user', 'x_sz_ref_user_FK_upd_by_id')
GO
call dbo.dropTableColumn('x_security_zone_ref_user', 'upd_by_id')
GO

call dbo.dropTableColumn('x_security_zone_ref_group', 'create_time')
GO
call dbo.dropTableColumn('x_security_zone_ref_group', 'update_time')
GO
call dbo.dropTableConstraint('x_security_zone_ref_group', 'x_sz_ref_grp_FK_added_by_id')
GO
call dbo.dropTableColumn('x_security_zone_ref_group', 'added_by_id')
GO
call dbo.dropTableConstraint('x_security_zone_ref_group', 'x_sz_ref_grp_FK_upd_by_id')
GO
call dbo.dropTableColumn('x_security_zone_ref_group', 'upd_by_id')
GO

call dbo.dropTableColumn('x_security_zone_ref_resource', 'create_time')
GO
call dbo.dropTableColumn('x_security_zone_ref_resource', 'update_time')
GO
call dbo.dropTableConstraint('x_security_zone_ref_resource', 'x_sz_ref_resource_FK_added_by_id')
GO
call dbo.dropTableColumn('x_security_zone_ref_resource', 'added_by_id')
GO
call dbo.dropTableConstraint('x_security_zone_ref_resource', 'x_sz_ref_resource_FK_upd_by_id')
GO
call dbo.dropTableColumn('x_security_zone_ref_resource', 'upd_by_id')
GO

call dbo.dropTableColumn('x_role_ref_user', 'create_time')
GO
call dbo.dropTableColumn('x_role_ref_user', 'update_time')
GO
call dbo.dropTableConstraint('x_role_ref_user', 'x_role_ref_user_FK_added_by_id')
GO
call dbo.dropTableColumn('x_role_ref_user', 'added_by_id')
GO
call dbo.dropTableConstraint('x_role_ref_user', 'x_role_ref_user_FK_upd_by_id')
GO
call dbo.dropTableColumn('x_role_ref_user', 'upd_by_id')
GO

call dbo.dropTableColumn('x_role_ref_group', 'create_time')
GO
call dbo.dropTableColumn('x_role_ref_group', 'update_time')
GO
call dbo.dropTableConstraint('x_role_ref_group', 'x_role_ref_grp_FK_added_by_id')
GO
call dbo.dropTableColumn('x_role_ref_group', 'added_by_id')
GO
call dbo.dropTableConstraint('x_role_ref_group', 'x_role_ref_grp_FK_upd_by_id')
GO
call dbo.dropTableColumn('x_role_ref_group', 'upd_by_id')
GO

call dbo.dropTableColumn('x_role_ref_role', 'create_time')
GO
call dbo.dropTableColumn('x_role_ref_role', 'update_time')
GO
call dbo.dropTableConstraint('x_role_ref_role', 'x_role_ref_role_FK_added_by_id')
GO
call dbo.dropTableColumn('x_role_ref_role', 'added_by_id')
GO
call dbo.dropTableConstraint('x_role_ref_role', 'x_role_ref_role_FK_upd_by_id')
GO
call dbo.dropTableColumn('x_role_ref_role', 'upd_by_id')
GO
*/
