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

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
IF EXISTS (
        SELECT type_desc, type
        FROM sys.procedures WITH(NOLOCK)
        WHERE NAME = 'dropTableColumn'
            AND type = 'P'
      )
BEGIN
	 PRINT 'Proc exist with name dbo.dropTableColumn'
     DROP PROCEDURE dbo.dropTableColumn
	 PRINT 'Proc dropped dbo.dropTableColumn'
END
GO
CREATE PROCEDURE dbo.dropTableColumn
	-- Add the parameters for the stored procedure here
	@tablename nvarchar(100),
	@columnname nvarchar(100)
AS
BEGIN
  IF EXISTS(select * from INFORMATION_SCHEMA.columns where table_name = @tablename and column_name = @columnname)
  BEGIN
    DECLARE @stmt VARCHAR(300);
	  SET @stmt = 'ALTER TABLE [dbo].[' + @tablename + '] DROP COLUMN [' + @columnname + ']'
    EXEC (@stmt);
  END
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
IF EXISTS (
        SELECT type_desc, type
        FROM sys.procedures WITH(NOLOCK)
        WHERE NAME = 'dropTableConstraint'
            AND type = 'P'
      )
BEGIN
	 PRINT 'Proc exist with name dbo.dropTableConstraint'
     DROP PROCEDURE dbo.dropTableConstraint
	 PRINT 'Proc dropped dbo.dropTableConstraint'
END
GO
CREATE PROCEDURE dbo.dropTableConstraint
	-- Add the parameters for the stored procedure here
	@tablename nvarchar(100),
	@constraintname nvarchar(100)
AS
BEGIN
  IF (OBJECT_ID(@constraintname) IS NOT NULL)
  BEGIN
    DECLARE @stmt VARCHAR(300);
	  SET @stmt = 'ALTER TABLE [dbo].[' + @tablename + '] DROP CONSTRAINT ' + @constraintname
    EXEC (@stmt);
  END
END
GO

EXEC dbo.dropTableColumn 'x_policy_ref_resource', 'create_time'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_resource', 'update_time'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_resource', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_resource', 'x_policy_ref_resource_FK_added_by'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_resource', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_resource', 'x_policy_ref_resource_FK_upd_by'
GO

EXEC dbo.dropTableColumn 'x_policy_ref_role', 'create_time'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_role', 'update_time'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_role', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_role', 'x_policy_ref_role_FK_added_by_id'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_role', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_role', 'x_policy_ref_role_FK_upd_by_id'
GO

EXEC dbo.dropTableColumn 'x_policy_ref_group', 'create_time'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_group', 'update_time'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_group', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_group', 'x_policy_ref_group_FK_added_by'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_group', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_group', 'x_policy_ref_group_FK_upd_by'
GO

EXEC dbo.dropTableColumn 'x_policy_ref_user', 'create_time'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_user', 'update_time'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_user', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_user', 'x_policy_ref_user_FK_added_by'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_user', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_user', 'x_policy_ref_user_FK_upd_by'
GO

EXEC dbo.dropTableColumn 'x_policy_ref_access_type', 'create_time'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_access_type', 'update_time'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_access_type', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_access_type', 'x_policy_ref_access_type_FK_added_by'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_access_type', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_access_type', 'x_policy_ref_access_type_FK_upd_by'
GO

EXEC dbo.dropTableColumn 'x_policy_ref_condition', 'create_time'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_condition', 'update_time'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_condition', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_condition', 'x_policy_ref_condition_FK_added_by'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_condition', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_condition', 'x_policy_ref_condition_FK_upd_by'
GO

EXEC dbo.dropTableColumn 'x_policy_ref_datamask_type', 'create_time'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_datamask_type', 'update_time'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_datamask_type', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_datamask_type', 'x_policy_ref_datamask_type_FK_added_by'
GO
EXEC dbo.dropTableConstraint 'x_policy_ref_datamask_type', 'added_by_id'
GO
EXEC dbo.dropTableColumn 'x_policy_ref_datamask_type', 'x_policy_ref_datamask_type_FK_upd_by'
GO

IF EXISTS (
        SELECT type_desc, type
        FROM sys.procedures WITH(NOLOCK)
        WHERE NAME = 'dropTableColumn'
            AND type = 'P'
      )
BEGIN
	 PRINT 'Proc exist with name dbo.dropTableColumn'
     DROP PROCEDURE dbo.dropTableColumn
	 PRINT 'Proc dropped dbo.dropTableColumn'
END
GO

IF EXISTS (
        SELECT type_desc, type
        FROM sys.procedures WITH(NOLOCK)
        WHERE NAME = 'dropTableConstraint'
            AND type = 'P'
      )
BEGIN
	 PRINT 'Proc exist with name dbo.dropTableConstraint'
     DROP PROCEDURE dbo.dropTableConstraint
	 PRINT 'Proc dropped dbo.dropTableConstraint'
END
GO
